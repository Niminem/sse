import std/[unittest, net, os, strutils, uri]
import sse/[types, client]

type
  ServerInfo = object
    socket: Socket
    port: Port

proc startServer(host = "127.0.0.1"): ServerInfo =
  var server = newSocket()
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(0), host)
  server.listen(1)
  let (_, port) = server.getLocalAddr()
  result = ServerInfo(socket: server, port: port)

proc recvRequest(conn: Socket, timeout = 5000): string =
  ## Read until we see the end of the HTTP request headers.
  result = ""
  while not result.endsWith("\r\n\r\n"):
    let c = conn.recv(1, timeout = timeout)
    if c.len == 0: break
    result.add c

suite "client - connection lifecycle":
  test "happy path: connect, receive one event, server closes":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: hello\n\n"
      )
      sleep(10)
      conn.close()
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var gotOpen = false
    var events: seq[SseEvent] = @[]
    var errors: seq[string] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onOpen = proc() = gotOpen = true
    es.onMessage = proc(ev: SseEvent) = events.add(ev)
    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check gotOpen == true
    check events.len == 1
    check events[0].data == "hello"
    check es.readyState == rsClosed

  test "onOpen fires with readyState rsOpen":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: done\n\n"
      )
      sleep(10)
      conn.close()
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var stateInOnOpen: ReadyState

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onOpen = proc() = stateInOnOpen = es.readyState

    es.connect()
    joinThread(serverThread)

    check stateInOnOpen == rsOpen

  test "close() during streaming stops client":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: first\n\n" &
        "data: second\n\n"
      )
      sleep(10)
      conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 0,
        reconnectionTime = 100
      ))

    es.onMessage = proc(ev: SseEvent) =
      events.add(ev)
      es.close()

    es.connect()
    joinThread(serverThread)

    check events.len >= 1
    check events[0].data == "first"
    check es.readyState == rsClosed

  test "CancelToken stops client mid-stream":
    let info = startServer()
    let cancel = newCancelToken()

    var serverThread: Thread[tuple[s: Socket, ct: CancelToken]]
    proc serverProc(args: tuple[s: Socket, ct: CancelToken]) {.thread.} =
      var conn: Socket
      new(conn)
      args.s.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: first\n\n"
      )
      sleep(100)
      args.ct.cancel()
      sleep(10)
      conn.close()
      args.s.close()

    createThread(serverThread, serverProc, (info.socket, cancel))

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        cancelToken = cancel,
        maxReconnectAttempts = 0,
        reconnectionTime = 100
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 1
    check events[0].data == "first"

suite "client - event delivery":
  test "multiple events delivered in order":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: one\n\n" &
        "data: two\n\n" &
        "data: three\n\n"
      )
      sleep(10)
      conn.close()
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 3
    check events[0].data == "one"
    check events[1].data == "two"
    check events[2].data == "three"

  test "event with id, event type, and multi-line data":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "id: 42\n" &
        "event: update\n" &
        "data: line1\n" &
        "data: line2\n" &
        "data: line3\n\n"
      )
      sleep(10)
      conn.close()
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 1
    check events[0].id == "42"
    check events[0].eventType == "update"
    check events[0].data == "line1\nline2\nline3"

  test "retry field updates reconnectionTime":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "retry: 5000\n\n" &
        "data: after-retry\n\n"
      )
      sleep(10)
      conn.close()
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 1
    check events[0].data == "after-retry"
    check es.reconnectionTime == 5000

suite "client - reconnection":
  test "auto-reconnect succeeds, events from both connections":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      # First connection: send one event then drop
      var conn1: Socket
      new(conn1)
      server.accept(conn1)
      discard conn1.recvRequest()
      conn1.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: from-first\n\n"
      )
      sleep(10)
      conn1.close()
      # Second connection: send another event then drop
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      discard conn2.recvRequest()
      conn2.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: from-second\n\n"
      )
      sleep(10)
      conn2.close()
      # Failure connections to exhaust retries
      for i in 0 ..< 2:
        var c: Socket
        new(c)
        server.accept(c)
        c.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 2,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 2
    check events[0].data == "from-first"
    check events[1].data == "from-second"

  test "Last-Event-ID sent on reconnect":
    let info = startServer()
    var secondRequest: string

    var serverThread: Thread[tuple[s: Socket, req: ptr string]]
    proc serverProc(args: tuple[s: Socket, req: ptr string]) {.thread.} =
      # First connection: send event with id, then drop
      var conn1: Socket
      new(conn1)
      args.s.accept(conn1)
      discard conn1.recvRequest()
      conn1.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "id: abc\n" &
        "data: first\n\n"
      )
      sleep(10)
      conn1.close()
      # Second connection: capture the request to check Last-Event-ID
      var conn2: Socket
      new(conn2)
      args.s.accept(conn2)
      args.req[] = conn2.recvRequest()
      conn2.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: second\n\n"
      )
      sleep(10)
      conn2.close()
      # Failure connections to exhaust retries
      for i in 0 ..< 2:
        var c: Socket
        new(c)
        args.s.accept(c)
        c.close()
      args.s.close()

    createThread(serverThread, serverProc, (info.socket, addr secondRequest))

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 2,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 2
    check "Last-Event-ID: abc" in secondRequest

  test "maxReconnectAttempts enforced":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      # Accept and immediately close 3 connections (initial + 2 retries)
      for i in 0 ..< 3:
        var conn: Socket
        new(conn)
        server.accept(conn)
        conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var errors: seq[string] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 2,
        reconnectionTime = 10
      ))

    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check es.readyState == rsClosed
    check errors.len > 0
    check "max reconnect attempts" in errors[errors.len - 1]

  test "reconnectAttempts resets on successful connection":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      # First connection: serve then drop
      var conn1: Socket
      new(conn1)
      server.accept(conn1)
      discard conn1.recvRequest()
      conn1.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: one\n\n"
      )
      sleep(10)
      conn1.close()
      # Second connection: succeeds (counter should reset), serve then drop
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      discard conn2.recvRequest()
      conn2.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: two\n\n"
      )
      sleep(10)
      conn2.close()
      # Third connection: succeeds again (proves counter reset, not accumulated)
      var conn3: Socket
      new(conn3)
      server.accept(conn3)
      discard conn3.recvRequest()
      conn3.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: three\n\n"
      )
      sleep(10)
      conn3.close()
      # Fourth connection: exhaust retries
      var conn4: Socket
      new(conn4)
      server.accept(conn4)
      conn4.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    # With maxReconnectAttempts=1, if counter didn't reset we'd stop after
    # conn1 drop + one retry. Getting 3 events proves the counter resets.
    check events.len == 3
    check events[0].data == "one"
    check events[1].data == "two"
    check events[2].data == "three"

suite "client - HTTP response handling":
  test "non-200 status is fatal":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 404 Not Found\r\n" &
        "Content-Type: text/plain\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var errors: seq[string] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check es.readyState == rsClosed
    check errors.len == 1
    check "status code" in errors[0]

  test "wrong Content-Type is fatal":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/plain\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var errors: seq[string] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check es.readyState == rsClosed
    check errors.len == 1
    check "Content-Type" in errors[0]

  test "missing Content-Type is fatal":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var errors: seq[string] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check es.readyState == rsClosed
    check errors.len == 1
    check "Content-Type" in errors[0]

suite "client - redirects":
  test "301 redirect followed to second server":
    let target = startServer()
    let redirector = startServer()

    var targetThread: Thread[Socket]
    proc targetProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: from-target\n\n"
      )
      sleep(10)
      conn.close()
      server.close()

    var redirThread: Thread[tuple[s: Socket, targetPort: int]]
    proc redirProc(args: tuple[s: Socket, targetPort: int]) {.thread.} =
      var conn: Socket
      new(conn)
      args.s.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 301 Moved Permanently\r\n" &
        "Location: http://127.0.0.1:" & $args.targetPort & "/events\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      args.s.close()

    createThread(targetThread, targetProc, target.socket)
    createThread(redirThread, redirProc, (redirector.socket, int(target.port)))

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(redirector.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 0,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) =
      events.add(ev)
      es.close()

    es.connect()
    joinThread(redirThread)
    joinThread(targetThread)

    check events.len == 1
    check events[0].data == "from-target"

  test "301 updates es.url permanently":
    let target = startServer()
    let redirector = startServer()

    var targetThread: Thread[Socket]
    proc targetProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: ok\n\n"
      )
      sleep(10)
      conn.close()
      server.close()

    var redirThread: Thread[tuple[s: Socket, targetPort: int]]
    proc redirProc(args: tuple[s: Socket, targetPort: int]) {.thread.} =
      var conn: Socket
      new(conn)
      args.s.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 301 Moved Permanently\r\n" &
        "Location: http://127.0.0.1:" & $args.targetPort & "/new-path\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      args.s.close()

    createThread(targetThread, targetProc, target.socket)
    createThread(redirThread, redirProc, (redirector.socket, int(target.port)))

    let es = newEventSource("http://127.0.0.1:" & $int(redirector.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 0,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = es.close()

    es.connect()
    joinThread(redirThread)
    joinThread(targetThread)

    check es.url.hostname == "127.0.0.1"
    check es.url.port == $int(target.port)
    check es.url.path == "/new-path"

  test "307 redirect followed but es.url unchanged":
    let target = startServer()
    let redirector = startServer()

    var targetThread: Thread[Socket]
    proc targetProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n" &
        "data: from-temp\n\n"
      )
      sleep(10)
      conn.close()
      server.close()

    var redirThread: Thread[tuple[s: Socket, targetPort: int]]
    proc redirProc(args: tuple[s: Socket, targetPort: int]) {.thread.} =
      var conn: Socket
      new(conn)
      args.s.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 307 Temporary Redirect\r\n" &
        "Location: http://127.0.0.1:" & $args.targetPort & "/temp\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      args.s.close()

    createThread(targetThread, targetProc, target.socket)
    createThread(redirThread, redirProc, (redirector.socket, int(target.port)))

    var events: seq[SseEvent] = @[]
    let originalPort = int(redirector.port)

    let es = newEventSource("http://127.0.0.1:" & $originalPort,
      config = initEventSourceConfig(
        maxReconnectAttempts = 0,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) =
      events.add(ev)
      es.close()

    es.connect()
    joinThread(redirThread)
    joinThread(targetThread)

    check events.len == 1
    check events[0].data == "from-temp"
    check es.url.port == $originalPort

  test "redirect without Location header is fatal":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 301 Moved Permanently\r\n" &
        "\r\n"
      )
      sleep(10)
      conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var errors: seq[string] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check es.readyState == rsClosed
    check errors.len == 1
    check "Location" in errors[0]

suite "client - chunked transfer encoding":
  test "chunked body decoded correctly":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "Transfer-Encoding: chunked\r\n" &
        "\r\n"
      )
      let chunk1 = "data: hello\n\n"
      conn.send(chunk1.len.toHex(2) & "\r\n" & chunk1 & "\r\n")
      let chunk2 = "data: world\n\n"
      conn.send(chunk2.len.toHex(2) & "\r\n" & chunk2 & "\r\n")
      conn.send("0\r\n\r\n")
      sleep(100)
      conn.close()
      # Reconnect attempt: close immediately
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 2
    check events[0].data == "hello"
    check events[1].data == "world"

  test "zero-length chunk ends stream gracefully":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "Transfer-Encoding: chunked\r\n" &
        "\r\n"
      )
      let chunk = "data: only-one\n\n"
      conn.send(chunk.len.toHex(2) & "\r\n" & chunk & "\r\n")
      conn.send("0\r\n\r\n")
      sleep(200)
      conn.close()
      # Reconnect attempt after chunked end: close immediately
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        reconnectionTime = 10
      ))

    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check events.len == 1
    check events[0].data == "only-one"

suite "client - edge cases":
  test "server closes immediately is retriable not fatal":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      # All connections: accept and immediately close (no response)
      for i in 0 ..< 3:
        var conn: Socket
        new(conn)
        server.accept(conn)
        conn.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var errors: seq[string] = @[]
    var gotOpen = false

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 2,
        reconnectionTime = 10
      ))

    es.onOpen = proc() = gotOpen = true
    es.onError = proc(msg: string) = errors.add(msg)

    es.connect()
    joinThread(serverThread)

    check gotOpen == false
    check es.readyState == rsClosed
    check "max reconnect attempts" in errors[errors.len - 1]

  test "inactivityTimeout disconnects when server sends nothing":
    let info = startServer()

    var serverThread: Thread[Socket]
    proc serverProc(server: Socket) {.thread.} =
      var conn: Socket
      new(conn)
      server.accept(conn)
      discard conn.recvRequest()
      conn.send(
        "HTTP/1.1 200 OK\r\n" &
        "Content-Type: text/event-stream\r\n" &
        "\r\n"
      )
      # Send nothing — client should disconnect after inactivityTimeout
      sleep(3000)
      conn.close()
      # Second connection (reconnect attempt): close immediately to exhaust retries
      var conn2: Socket
      new(conn2)
      server.accept(conn2)
      conn2.close()
      server.close()

    createThread(serverThread, serverProc, info.socket)

    var gotOpen = false
    var events: seq[SseEvent] = @[]

    let es = newEventSource("http://127.0.0.1:" & $int(info.port),
      config = initEventSourceConfig(
        maxReconnectAttempts = 1,
        inactivityTimeout = 1500,
        reconnectionTime = 10
      ))

    es.onOpen = proc() = gotOpen = true
    es.onMessage = proc(ev: SseEvent) = events.add(ev)

    es.connect()
    joinThread(serverThread)

    check gotOpen == true
    check events.len == 0
