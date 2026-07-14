import std/[unittest, asyncdispatch, net, os, strutils, atomics]
import sse/[types, http, client_async]

# ===========================================================================
# 1. Construction & URL Validation (spec §6.1 steps 1–3)
# ===========================================================================

suite "Construction & URL Validation":

  test "valid URL creates client in Connecting state with correct URL fields":
    let c1 = newAsyncSseClient("http://example.com/events")
    check c1.readyState == Connecting
    check c1.url.scheme == "http"
    check c1.url.host == "example.com"
    check c1.url.port == 80
    check c1.url.path == "/events"
    check c1.url.useTls == false

    let c2 = newAsyncSseClient("https://example.com:8443/stream?t=1")
    check c2.readyState == Connecting
    check c2.url.scheme == "https"
    check c2.url.port == 8443
    check c2.url.useTls == true
    check c2.url.path == "/stream?t=1"

  test "url serialization normalizes default port away":
    check $newAsyncSseClient("http://example.com:80/events").url ==
      "http://example.com/events"
    check $newAsyncSseClient("http://example.com:9090/events").url ==
      "http://example.com:9090/events"
    check $newAsyncSseClient("https://example.com:443/x").url ==
      "https://example.com/x"

  test "path defaults to / when absent":
    check newAsyncSseClient("http://example.com").url.path == "/"

  test "invalid URLs raise ValueError at construction":
    expect(ValueError):
      discard newAsyncSseClient("")
    expect(ValueError):
      discard newAsyncSseClient("example.com/events")
    expect(ValueError):
      discard newAsyncSseClient("ftp://example.com/events")
    expect(ValueError):
      discard newAsyncSseClient("ws://example.com/events")
    expect(ValueError):
      discard newAsyncSseClient("http:///events")
    expect(ValueError):
      discard newAsyncSseClient("http://example.com:0/events")
    expect(ValueError):
      discard newAsyncSseClient("http://example.com:65536/events")
    expect(ValueError):
      discard newAsyncSseClient("http://example.com:abc/events")

  test "default config values":
    let client = newAsyncSseClient("http://example.com/events")
    check client.config.autoReconnect == true
    check client.config.maxRedirects == 10
    check client.config.maxReconnectDelay == 60_000
    check client.config.stallTimeout == 0
    check client.config.recvSize == 4096

  test "custom config is honored":
    let cfg = AsyncSseClientConfig(
      autoReconnect: false,
      maxRedirects: 5,
      maxReconnectDelay: 30_000,
      stallTimeout: 10_000,
      recvSize: 8192,
    )
    let client = newAsyncSseClient("http://example.com/events", config = cfg)
    check client.config == cfg

  test "initial state: callbacks nil, lastEventId empty, reconnectionTime 3000":
    let client = newAsyncSseClient("http://example.com/events")
    check client.onEvent == nil
    check client.onComment == nil
    check client.onOpen == nil
    check client.onError == nil
    check client.lastEventId == ""
    check client.reconnectionTime == 3000

  test "cancel token stored when provided":
    let token = newCancelToken()
    let client = newAsyncSseClient("http://example.com/events",
                                   cancelToken = token)
    token.cancel()
    check client.readyState == Connecting

# ===========================================================================
# Test Infrastructure — Threaded Loopback Server
# ===========================================================================

const NoReconnect = AsyncSseClientConfig(
  autoReconnect: false,
  maxRedirects: 10,
  maxReconnectDelay: 60_000,
  stallTimeout: 0,
  recvSize: 4096,
)

const FastReconnect = AsyncSseClientConfig(
  autoReconnect: true,
  maxRedirects: 10,
  maxReconnectDelay: 100,
  stallTimeout: 0,
  recvSize: 4096,
)

proc sseResponse(body: string; contentType = "text/event-stream"): string =
  "HTTP/1.1 200 OK\r\n" &
  "Content-Type: " & contentType & "\r\n" &
  "\r\n" &
  body

proc findFreePort(): int =
  let sock = newSocket()
  sock.setSockOpt(OptReuseAddr, true)
  sock.bindAddr(Port(0), address = "127.0.0.1")
  result = int(sock.getLocalAddr()[1])
  sock.close()

# Server-readiness signalling. Every server proc increments the counter
# once its socket is listening; awaitListening blocks until the servers
# just created are ready. Replaces fixed 100-200 ms startup sleeps
# (~3.5 s of padding per run) and cannot race a slow thread start.
# A probe-connect helper is not an option here: the servers accept exactly
# one client, so a probe would be consumed as the client under test.
var serversListening: Atomic[int]
var serversExpected = 0  # main-thread bookkeeping; tests run sequentially

proc awaitListening(n = 1) =
  serversExpected += n
  while serversListening.load() < serversExpected:
    sleep(1)

type ServerConfig = tuple[port: int, response: string]

proc serveOnce(cfg: ServerConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  discard serversListening.fetchAdd(1)
  try:
    var client: Socket
    server.accept(client)
    discard client.recv(4096)
    client.send(cfg.response)
    client.close()
  except CatchableError:
    discard
  server.close()

proc clientUrl(port: int; path = "/events"): string =
  "http://127.0.0.1:" & $port & path

template withServer(response: string; body: untyped) =
  let port {.inject.} = findFreePort()
  var thr: Thread[ServerConfig]
  createThread(thr, serveOnce, (port, response))
  awaitListening()
  proc run() {.async.} =
    body
  waitFor run()
  joinThread(thr)

# ===========================================================================
# 2. Successful Connection (Happy Path)
# ===========================================================================

suite "Successful Connection":

  test "onOpen fires and readyState transitions through Open to Closed":
    withServer(sseResponse("data: hi\n\n")):
      var opened = false
      var stateAtOpen = Closed

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onOpen = proc() =
        opened = true
        stateAtOpen = client.readyState

      await client.connect()

      check opened
      check stateAtOpen == Open
      check client.readyState == Closed

  test "simple event delivered with correct eventType, data, lastEventId, origin":
    withServer(sseResponse("data: hello world\n\n")):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 1
      check events[0].eventType == "message"
      check events[0].data == "hello world"
      check events[0].lastEventId == ""
      check events[0].origin == "http://127.0.0.1:" & $port

  test "multiple events in one stream":
    withServer(sseResponse(
        "data: first\n\ndata: second\n\ndata: third\n\n")):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 3
      check events[0].data == "first"
      check events[1].data == "second"
      check events[2].data == "third"

  test "multi-line data fields":
    withServer(sseResponse(
        "data: line one\ndata: line two\ndata: line three\n\n")):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 1
      check events[0].data == "line one\nline two\nline three"

  test "named event types reset per event":
    withServer(sseResponse(
        "event: add\ndata: 100\n\n" &
        "event: remove\ndata: 200\n\n" &
        "data: plain\n\n")):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 3
      check events[0].eventType == "add"
      check events[0].data == "100"
      check events[1].eventType == "remove"
      check events[1].data == "200"
      check events[2].eventType == "message"
      check events[2].data == "plain"

  test "comments delivered via onComment without creating events":
    withServer(sseResponse(
        ": first comment\n: second comment\ndata: hello\n\n")):
      var comments: seq[string] = @[]
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onComment = proc(c: string) = comments.add(c)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check comments == @[" first comment", " second comment"]
      check events.len == 1
      check events[0].data == "hello"

  test "event IDs persist across events":
    withServer(sseResponse(
        "id: 1\ndata: first\n\n" &
        "data: second\n\n" &
        "id: 3\ndata: third\n\n")):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 3
      check events[0].lastEventId == "1"
      check events[1].lastEventId == "1"
      check events[2].lastEventId == "3"
      check client.lastEventId == "3"

  test "onError fires with 'stream ended' when autoReconnect is false":
    withServer(sseResponse("data: bye\n\n")):
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check errors.len == 1
      check errors[0] == "stream ended"

# ===========================================================================
# 3. Connection Failure — Fatal (spec §6.4)
# ===========================================================================

suite "Connection Failure - Fatal":

  test "non-200 status (404) causes fatal error, no reconnect":
    withServer("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"):
      var errors: seq[string] = @[]
      var opened = false

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onOpen = proc() = opened = true
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check not opened
      check client.readyState == Closed
      check errors.len == 1
      check "HTTP 404" in errors[0]

  test "204 No Content causes fatal error":
    withServer("HTTP/1.1 204 No Content\r\n\r\n"):
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check errors.len == 1
      check "HTTP 204" in errors[0]

  test "500 Internal Server Error causes fatal error":
    withServer("HTTP/1.1 500 Internal Server Error\r\n\r\n"):
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check "HTTP 500" in errors[0]

  test "wrong Content-Type causes fatal error":
    withServer("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\ndata: hi\n\n"):
      var errors: seq[string] = @[]
      var opened = false

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onOpen = proc() = opened = true
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check not opened
      check client.readyState == Closed
      check errors.len == 1

  test "Content-Type with params is accepted (not fatal)":
    withServer(sseResponse("data: ok\n\n", contentType = "text/event-stream; charset=utf-8")):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 1
      check events[0].data == "ok"

  test "Content-Type matching is case-insensitive":
    withServer("HTTP/1.1 200 OK\r\nContent-Type: Text/Event-Stream\r\n\r\ndata: ok\n\n"):
      var events: seq[SseEvent] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 1
      check events[0].data == "ok"

  test "fatal error with autoReconnect true still does not reconnect":
    withServer("HTTP/1.1 404 Not Found\r\n\r\n"):
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = DefaultConfig)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check errors.len == 1

# ===========================================================================
# Additional Infrastructure — Multi-Connection Server
# ===========================================================================

type MultiServerConfig = object
  port: int
  resp1, resp2, resp3: string
  count: int

proc serveMulti(cfg: MultiServerConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  discard serversListening.fetchAdd(1)
  let responses = [cfg.resp1, cfg.resp2, cfg.resp3]
  try:
    for i in 0 ..< cfg.count:
      var client: Socket
      server.accept(client)
      discard client.recv(4096)
      if responses[i].len > 0:
        client.send(responses[i])
      client.close()
  except CatchableError:
    discard
  server.close()

proc multiCfg(port: int; responses: openArray[string]): MultiServerConfig =
  result.port = port
  result.count = min(responses.len, 3)
  if result.count > 0: result.resp1 = responses[0]
  if result.count > 1: result.resp2 = responses[1]
  if result.count > 2: result.resp3 = responses[2]

template withMultiServer(responses: openArray[string]; body: untyped) =
  let port {.inject.} = findFreePort()
  var thr: Thread[MultiServerConfig]
  createThread(thr, serveMulti, multiCfg(port, responses))
  awaitListening()
  proc run() {.async.} =
    body
  waitFor run()
  joinThread(thr)

# ===========================================================================
# 4. Connection Failure — Retryable
# ===========================================================================

const FastNoReconnect = AsyncSseClientConfig(
  autoReconnect: false,
  maxRedirects: 10,
  maxReconnectDelay: 60_000,
  stallTimeout: 0,
  recvSize: 4096,
)

suite "Connection Failure - Retryable":

  test "connection refused (no listener) with autoReconnect false":
    let port = findFreePort()
    proc run() {.async.} =
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = FastNoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check errors.len == 1
      check "connection failed" in errors[0]

    waitFor run()

  test "server accepts then closes immediately (0-byte recv)":
    withMultiServer(@[""]):
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = FastNoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check errors.len == 1
      check "connection failed" in errors[0]

  test "server sends partial headers then closes":
    withMultiServer(@["HTTP/1.1 200 OK\r\nContent-"]):
      var errors: seq[string] = @[]

      let client = newAsyncSseClient(clientUrl(port), config = FastNoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check errors.len == 1
      check "connection failed" in errors[0]

  test "autoReconnect recovers after clean end-of-body":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: first\n\n"), sseResponse("data: second\n\n")]))
    awaitListening()

    proc runRetryTest() {.async.} =
      var errors: seq[string] = @[]
      var events: seq[SseEvent] = @[]
      var openCount = 0

      let client = newAsyncSseClient(clientUrl(port), config = FastReconnect)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2:
          client.close()
      client.onError = proc(msg: string) = errors.add(msg)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check openCount >= 2
      check events.len >= 1
      check events[0].data == "first"
      check errors.len >= 1
      check "reconnecting" in errors[0]

    waitFor runRetryTest()
    joinThread(thr)

# ===========================================================================
# 5. Reconnection Behavior
# ===========================================================================

# Server that captures requests for verification.
type CaptureServerConfig = object
  port: int
  resp1, resp2: string
  reqBuf: ptr array[2, string]

proc serveCaptureMulti(cfg: CaptureServerConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  discard serversListening.fetchAdd(1)
  let responses = [cfg.resp1, cfg.resp2]
  try:
    for i in 0 ..< 2:
      var client: Socket
      server.accept(client)
      let req = client.recv(4096)
      cfg.reqBuf[i] = req
      if responses[i].len > 0:
        client.send(responses[i])
      client.close()
  except CatchableError:
    discard
  server.close()

suite "Reconnection Behavior":

  test "lastEventId persists across reconnections":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("id: 42\ndata: first\n\n"),
       sseResponse("data: second\n\n")]))
    awaitListening()

    proc run() {.async.} =
      var events: seq[SseEvent] = @[]
      var openCount = 0
      let client = newAsyncSseClient(clientUrl(port), config = FastReconnect)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()

      check events.len >= 1
      check events[0].lastEventId == "42"
      check client.lastEventId == "42"

    waitFor run()
    joinThread(thr)

  test "Last-Event-ID header sent on reconnection":
    var reqBuf: array[2, string]
    let port = findFreePort()
    var thr: Thread[CaptureServerConfig]
    let cfg = CaptureServerConfig(
      port: port,
      resp1: sseResponse("id: abc\ndata: first\n\n"),
      resp2: sseResponse("data: second\n\n"),
      reqBuf: addr reqBuf)
    createThread(thr, serveCaptureMulti, cfg)
    awaitListening()

    proc run() {.async.} =
      var openCount = 0
      let client = newAsyncSseClient(clientUrl(port), config = FastReconnect)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()
      await client.connect()

    waitFor run()
    joinThread(thr)

    check "Last-Event-ID" notin reqBuf[0]
    check "Last-Event-ID: abc" in reqBuf[1]

  test "server retry field updates reconnectionTime":
    withServer(sseResponse("retry: 5000\ndata: hi\n\n")):
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = discard
      await client.connect()
      check client.reconnectionTime == 5000

  test "reconnection fires error event with reconnecting message":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: first\n\n"),
       sseResponse("data: second\n\n")]))
    awaitListening()

    proc run() {.async.} =
      var errors: seq[string] = @[]
      var openCount = 0
      let client = newAsyncSseClient(clientUrl(port), config = FastReconnect)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()
      client.onError = proc(msg: string) = errors.add(msg)
      await client.connect()

      check errors.len >= 1
      check "connection lost" in errors[0] or "reconnecting" in errors[0]

    waitFor run()
    joinThread(thr)

  test "readyState transitions: Open → Connecting on reconnect":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: a\n\n"), sseResponse("data: b\n\n")]))
    awaitListening()

    proc run() {.async.} =
      var states: seq[ReadyState] = @[]
      var openCount = 0
      let client = newAsyncSseClient(clientUrl(port), config = FastReconnect)
      client.onOpen = proc() =
        states.add(client.readyState)
        inc openCount
        if openCount >= 2: client.close()
      client.onError = proc(msg: string) =
        states.add(client.readyState)
      await client.connect()

      check Open in states
      check Connecting in states

    waitFor run()
    joinThread(thr)

# ===========================================================================
# 6. Exponential Backoff
# ===========================================================================

import std/times

suite "Exponential Backoff":

  test "server retry field affects actual reconnection delay":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("retry: 100\ndata: first\n\n"),
       sseResponse("data: second\n\n")]))
    awaitListening()

    proc run() {.async.} =
      var openCount = 0
      var t0, t1: float
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onOpen = proc() =
        inc openCount
        if openCount == 1: t0 = epochTime()
        elif openCount == 2:
          t1 = epochTime()
          client.close()
      await client.connect()

      let elapsed = (t1 - t0) * 1000
      check elapsed < 2000
      check client.reconnectionTime == 100

    waitFor run()
    joinThread(thr)

  test "maxReconnectDelay caps backoff":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("retry: 50000\ndata: first\n\n"),
       sseResponse("data: second\n\n")]))
    awaitListening()

    proc run() {.async.} =
      var openCount = 0
      var t0, t1: float
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 200, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onOpen = proc() =
        inc openCount
        if openCount == 1: t0 = epochTime()
        elif openCount == 2:
          t1 = epochTime()
          client.close()
      await client.connect()

      let elapsed = (t1 - t0) * 1000
      check elapsed < 2000
      check client.reconnectionTime == 50000

    waitFor run()
    joinThread(thr)

  test "consecutiveFailures resets after successful connection":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("retry: 100\ndata: a\n\n"),
       sseResponse("data: b\n\n"),
       sseResponse("data: c\n\n")]))
    awaitListening()

    proc run() {.async.} =
      var openCount = 0
      var reconnectTimes: seq[float] = @[]
      var lastClose: float
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onOpen = proc() =
        inc openCount
        if openCount > 1:
          reconnectTimes.add((epochTime() - lastClose) * 1000)
        if openCount >= 3: client.close()
      client.onError = proc(msg: string) =
        if "reconnecting" in msg:
          lastClose = epochTime()
      await client.connect()

      check openCount >= 3
      for t in reconnectTimes:
        check t < 2000

    waitFor run()
    joinThread(thr)

# ===========================================================================
# 7. Redirects
# ===========================================================================

proc redirectResponse(status: int; location: string): string =
  "HTTP/1.1 " & $status & " Redirect\r\n" &
  "Location: " & location & "\r\n" &
  "Content-Length: 0\r\n" &
  "\r\n"

suite "Redirects":

  test "301 redirect followed and updates client.url permanently":
    let port = findFreePort()
    let resps = [redirectResponse(301, "http://127.0.0.1:" & $port & "/new-path"),
                 sseResponse("data: redirected\n\n")]
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port, resps))
    awaitListening()
    proc run301() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port, "/old"), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "redirected"
      check client.url.path == "/new-path"
    waitFor run301()
    joinThread(thr)

  test "307 redirect followed but does NOT update client.url":
    let port = findFreePort()
    let resps = [redirectResponse(307, "http://127.0.0.1:" & $port & "/temp"),
                 sseResponse("data: temp-ok\n\n")]
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port, resps))
    awaitListening()
    proc run307() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port, "/original"), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "temp-ok"
      check client.url.path == "/original"
    waitFor run307()
    joinThread(thr)

  test "308 redirect updates client.url permanently":
    let port = findFreePort()
    let resps = [redirectResponse(308, "http://127.0.0.1:" & $port & "/perm"),
                 sseResponse("data: perm-ok\n\n")]
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port, resps))
    awaitListening()
    proc run308() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port, "/old"), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check client.url.path == "/perm"
    waitFor run308()
    joinThread(thr)

  test "multiple redirects in a chain":
    let port = findFreePort()
    let resps = [redirectResponse(302, "http://127.0.0.1:" & $port & "/hop1"),
                 redirectResponse(302, "http://127.0.0.1:" & $port & "/hop2"),
                 sseResponse("data: final\n\n")]
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port, resps))
    awaitListening()
    proc runChain() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "final"
    waitFor runChain()
    joinThread(thr)

  test "exceeding maxRedirects causes fatal error":
    let port = findFreePort()
    let resps = [redirectResponse(302, "http://127.0.0.1:" & $port & "/a"),
                 redirectResponse(302, "http://127.0.0.1:" & $port & "/b"),
                 redirectResponse(302, "http://127.0.0.1:" & $port & "/c")]
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port, resps))
    awaitListening()
    proc runMaxRedir() {.async.} =
      var errors: seq[string] = @[]
      let cfg = AsyncSseClientConfig(
        autoReconnect: false, maxRedirects: 2,
        maxReconnectDelay: 60_000, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onError = proc(msg: string) = errors.add(msg)
      await client.connect()
      check client.readyState == Closed
      check errors.len == 1
      check "too many redirects" in errors[0]
    waitFor runMaxRedir()
    joinThread(thr)

  test "redirect with no Location header causes fatal error":
    withServer("HTTP/1.1 301 Moved\r\nContent-Length: 0\r\n\r\n"):
      var errors: seq[string] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)
      await client.connect()
      check client.readyState == Closed
      check "no Location" in errors[0]

  test "redirect with invalid Location causes fatal error":
    withServer(redirectResponse(301, "://invalid")):
      var errors: seq[string] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onError = proc(msg: string) = errors.add(msg)
      await client.connect()
      check client.readyState == Closed
      check errors.len == 1

  test "origin reflects final URL after redirect":
    let port = findFreePort()
    let resps = [redirectResponse(307, "http://127.0.0.1:" & $port & "/final"),
                 sseResponse("data: hello\n\n")]
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port, resps))
    awaitListening()
    proc runOrigin() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port, "/start"), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].origin == "http://127.0.0.1:" & $port
    waitFor runOrigin()
    joinThread(thr)

# ===========================================================================
# 8. close()
# ===========================================================================

type SlowServerConfig = object
  port: int
  headers: string
  body: string
  delayMs: int

proc serveSlowBody(cfg: SlowServerConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  discard serversListening.fetchAdd(1)
  try:
    var client: Socket
    server.accept(client)
    discard client.recv(4096)
    client.send(cfg.headers)
    # Stay silent for up to delayMs, but return as soon as the client
    # disconnects — a blind sleep(delayMs) would keep joinThread (and the
    # whole suite) blocked long after the client under test closed or
    # cancelled. The client never sends after its request, so recv only
    # ever times out (still connected) or returns "" (peer closed).
    var waited = 0
    while waited < cfg.delayMs:
      try:
        if client.recv(1, timeout = 100).len == 0:
          break
      except TimeoutError:
        waited += 100
    if cfg.body.len > 0:
      client.send(cfg.body)
    client.close()
  except CatchableError:
    discard
  server.close()

suite "close()":

  test "close sets readyState to Closed and connect returns":
    withServer(sseResponse("data: hi\n\n")):
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      await client.connect()
      check client.readyState == Closed
      client.close()
      check client.readyState == Closed

  test "close during body read terminates connect promptly":
    let port = findFreePort()
    var thr: Thread[SlowServerConfig]
    let cfg = SlowServerConfig(
      port: port,
      headers: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
               "data: first\n\n",
      body: "data: late\n\n",
      delayMs: 10000)
    createThread(thr, serveSlowBody, cfg)
    awaitListening()

    proc runClose() {.async.} =
      var events: seq[SseEvent] = @[]
      var errors: seq[string] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) =
        events.add(e)
        client.close()
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check errors.len == 0
      check events.len >= 1
      check events[0].data == "first"

    waitFor runClose()
    joinThread(thr)

  test "no onError fired on close":
    let port = findFreePort()
    var thr: Thread[SlowServerConfig]
    let cfg = SlowServerConfig(
      port: port,
      headers: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
               "data: hello\n\n",
      body: "",
      delayMs: 10000)
    createThread(thr, serveSlowBody, cfg)
    awaitListening()

    proc runNoError() {.async.} =
      var errors: seq[string] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = DefaultConfig)
      client.onError = proc(msg: string) = errors.add(msg)
      client.onEvent = proc(e: SseEvent) =
        client.close()

      await client.connect()
      check errors.len == 0

    waitFor runNoError()
    joinThread(thr)

  test "close during reconnection sleep exits quickly":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: hi\n\n")]))
    awaitListening()

    proc runCloseReconn() {.async.} =
      var errorFired = false
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onError = proc(msg: string) = errorFired = true

      let connectFut = client.connect()
      await sleepAsync(500)
      check errorFired
      check client.readyState == Connecting

      let t0 = epochTime()
      client.close()
      await connectFut
      let elapsed = (epochTime() - t0) * 1000

      check client.readyState == Closed
      check elapsed < 1000

    waitFor runCloseReconn()
    joinThread(thr)

  test "close when already Closed is a no-op":
    proc runNoop() {.async.} =
      let port = findFreePort()
      var thr: Thread[ServerConfig]
      createThread(thr, serveOnce, (port, sseResponse("data: x\n\n")))
      awaitListening()
      let c = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      await c.connect()
      check c.readyState == Closed
      c.close()
      check c.readyState == Closed
      joinThread(thr)

    waitFor runNoop()

# ===========================================================================
# 9. CancelToken
# ===========================================================================

suite "CancelToken":

  test "cancel token terminates connect like close":
    let port = findFreePort()
    var thr: Thread[SlowServerConfig]
    let cfg = SlowServerConfig(
      port: port,
      headers: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
               "data: ping\n\n",
      body: "",
      delayMs: 10000)
    createThread(thr, serveSlowBody, cfg)
    awaitListening()

    proc runCancel() {.async.} =
      var events: seq[SseEvent] = @[]
      var errors: seq[string] = @[]
      let token = newCancelToken()
      let client = newAsyncSseClient(clientUrl(port),
                                     config = NoReconnect, cancelToken = token)
      client.onEvent = proc(e: SseEvent) =
        events.add(e)
        token.cancel()
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check client.readyState == Closed
      check events.len >= 1
      check errors.len == 0

    waitFor runCancel()
    joinThread(thr)

  test "cancel token shared across clients cancels all":
    let port1 = findFreePort()
    let port2 = findFreePort()
    var thr1: Thread[SlowServerConfig]
    var thr2: Thread[SlowServerConfig]
    let cfg1 = SlowServerConfig(port: port1,
      headers: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
               "data: a\n\n",
      body: "", delayMs: 10000)
    let cfg2 = SlowServerConfig(port: port2,
      headers: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
               "data: b\n\n",
      body: "", delayMs: 10000)
    createThread(thr1, serveSlowBody, cfg1)
    createThread(thr2, serveSlowBody, cfg2)
    awaitListening(2)

    proc runShared() {.async.} =
      let token = newCancelToken()
      let c1 = newAsyncSseClient(clientUrl(port1),
                                  config = NoReconnect, cancelToken = token)
      let c2 = newAsyncSseClient(clientUrl(port2),
                                  config = NoReconnect, cancelToken = token)
      c1.onEvent = proc(e: SseEvent) = token.cancel()
      c2.onEvent = proc(e: SseEvent) = token.cancel()

      let f1 = c1.connect()
      let f2 = c2.connect()
      await f1
      await f2

      check c1.readyState == Closed
      check c2.readyState == Closed

    waitFor runShared()
    joinThread(thr1)
    joinThread(thr2)

  test "cancel during reconnection sleep":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: hi\n\n")]))
    awaitListening()

    proc runCancelReconn() {.async.} =
      let token = newCancelToken()
      var openCount = 0
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port),
                                     config = cfg, cancelToken = token)
      client.onOpen = proc() = inc openCount
      client.onError = proc(msg: string) =
        token.cancel()

      await client.connect()

      check client.readyState == Closed
      check openCount == 1

    waitFor runCancelReconn()
    joinThread(thr)

# ===========================================================================
# 10. Stall Timeout
# ===========================================================================

suite "Stall Timeout":

  test "stall timeout fires when server goes silent":
    let port = findFreePort()
    var thr: Thread[SlowServerConfig]
    let cfg = SlowServerConfig(
      port: port,
      headers: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
               "data: hello\n\n",
      body: "",
      delayMs: 2000)
    createThread(thr, serveSlowBody, cfg)
    awaitListening()

    proc runStall() {.async.} =
      var events: seq[SseEvent] = @[]
      var errors: seq[string] = @[]
      let stallCfg = AsyncSseClientConfig(
        autoReconnect: false, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 800, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = stallCfg)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      client.onError = proc(msg: string) = errors.add(msg)

      await client.connect()

      check events.len >= 1
      check events[0].data == "hello"
      check client.readyState == Closed

    waitFor runStall()
    joinThread(thr)

  test "stall timeout zero means no timeout (default)":
    withServer(sseResponse("data: ok\n\n")):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1

  test "stall timeout with autoReconnect triggers reconnection":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: first\n\n"), sseResponse("data: second\n\n")]))
    awaitListening()

    proc runStallReconn() {.async.} =
      var openCount = 0
      let stallCfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 100, stallTimeout: 300, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = stallCfg)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()
      await client.connect()
      check openCount >= 2

    waitFor runStallReconn()
    joinThread(thr)

# ===========================================================================
# 11. Transfer Modes
# ===========================================================================

suite "Transfer Modes":

  test "chunked transfer encoding decoded correctly":
    let chunkedBody = "5\r\ndata:\r\n5\r\n hi\n\n\r\n0\r\n\r\n"
    let resp = "HTTP/1.1 200 OK\r\n" &
               "Content-Type: text/event-stream\r\n" &
               "Transfer-Encoding: chunked\r\n" &
               "\r\n" & chunkedBody
    withServer(resp):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "hi"

  test "Content-Length response stops reading at length":
    let body = "data: hello\n\n"
    let resp = "HTTP/1.1 200 OK\r\n" &
               "Content-Type: text/event-stream\r\n" &
               "Content-Length: " & $body.len & "\r\n" &
               "\r\n" & body
    withServer(resp):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "hello"

  test "identity transfer (no headers) reads until close":
    withServer(sseResponse("data: one\n\ndata: two\n\n")):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 2

# ===========================================================================
# 12. Parser Integration Edge Cases
# ===========================================================================

suite "Parser Integration":

  test "BOM at stream start is stripped":
    let bom = "\xEF\xBB\xBF"
    withServer(sseResponse(bom & "data: after bom\n\n")):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "after bom"

  test "id with no value resets lastEventId to empty":
    withServer(sseResponse("id: 5\ndata: a\n\nid\ndata: b\n\n")):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events[0].lastEventId == "5"
      check events[1].lastEventId == ""
      check client.lastEventId == ""

  test "incomplete event at end-of-stream is discarded":
    withServer(sseResponse("data: complete\n\ndata: incomplete")):
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()
      check events.len == 1
      check events[0].data == "complete"

  test "parser.reset called on reconnection clears buffers":
    let port = findFreePort()
    var thr: Thread[MultiServerConfig]
    createThread(thr, serveMulti, multiCfg(port,
      [sseResponse("data: partial"),
       sseResponse("data: fresh\n\n")]))
    awaitListening()

    proc runReset() {.async.} =
      var events: seq[SseEvent] = @[]
      var errorCount = 0
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 100, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      # Close from onEvent, not onOpen: closing in onOpen would (correctly,
      # per spec §5.1 step 6) suppress delivery of the very event this test
      # asserts on. If parser.reset failed to clear the stale "data: partial"
      # line buffer, the delivered data would not equal "fresh".
      client.onEvent = proc(e: SseEvent) =
        events.add(e)
        client.close()
      client.onError = proc(msg: string) =
        # Bail out if the event never arrives (server has only 2 responses;
        # further reconnect attempts are refused and would loop forever).
        inc errorCount
        if errorCount >= 5: client.close()
      await client.connect()

      check events.len >= 1
      check events[0].data == "fresh"

    waitFor runReset()
    joinThread(thr)

# ===========================================================================
# 13. Custom Requests (method / extra headers / body)
# ===========================================================================

# Server that captures a single request for wire-level verification.
type SingleCaptureConfig = object
  port: int
  resp: string
  reqBuf: ptr string

proc serveCaptureOnce(cfg: SingleCaptureConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  discard serversListening.fetchAdd(1)
  try:
    var client: Socket
    server.accept(client)
    cfg.reqBuf[] = client.recv(4096)
    client.send(cfg.resp)
    client.close()
  except CatchableError:
    discard
  server.close()

suite "Custom Requests":

  test "default config sends spec-compliant GET":
    let client = newAsyncSseClient("http://example.com/events")
    check client.config.httpMethod == HttpGet
    check client.config.extraHeaders.len == 0
    check client.config.body == ""

  test "reserved extra header rejected at construction":
    for name in ["Host", "accept", "Cache-Control", "Last-Event-ID",
                 "content-length"]:
      var cfg = NoReconnect
      cfg.extraHeaders = @[(name, "v")]
      expect(ValueError):
        discard newAsyncSseClient("http://example.com/events", config = cfg)

  test "CR/LF in extra header rejected at construction":
    var cfg = NoReconnect
    cfg.extraHeaders = @[("X-Bad", "v\r\nInjected: yes")]
    expect(ValueError):
      discard newAsyncSseClient("http://example.com/events", config = cfg)

  test "body with GET rejected at construction":
    var cfg = NoReconnect
    cfg.body = "payload"
    expect(ValueError):
      discard newAsyncSseClient("http://example.com/events", config = cfg)

  test "POST with headers and body goes over the wire; stream parsed":
    var reqBuf = ""
    let port = findFreePort()
    var thr: Thread[SingleCaptureConfig]
    createThread(thr, serveCaptureOnce, SingleCaptureConfig(
      port: port,
      resp: sseResponse("data: streamed\n\n"),
      reqBuf: addr reqBuf))
    awaitListening()

    proc runPost() {.async.} =
      let payload = """{"model":"m","stream":true}"""
      var cfg = NoReconnect
      cfg.httpMethod = HttpPost
      cfg.extraHeaders = @[("content-type", "application/json"),
                           ("x-api-key", "secret")]
      cfg.body = payload

      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onEvent = proc(e: SseEvent) = events.add(e)
      await client.connect()

      check events.len == 1
      check events[0].data == "streamed"

    waitFor runPost()
    joinThread(thr)

    let payload = """{"model":"m","stream":true}"""
    check reqBuf.startsWith("POST /events HTTP/1.1\r\n")
    check "Host: 127.0.0.1:" & $port & "\r\n" in reqBuf
    check "Accept: text/event-stream\r\n" in reqBuf
    check "content-type: application/json\r\n" in reqBuf
    check "x-api-key: secret\r\n" in reqBuf
    check "Content-Length: " & $payload.len & "\r\n" in reqBuf
    check reqBuf.endsWith("\r\n\r\n" & payload)

  test "extra headers re-sent on reconnection":
    var reqBuf: array[2, string]
    let port = findFreePort()
    var thr: Thread[CaptureServerConfig]
    let scfg = CaptureServerConfig(
      port: port,
      resp1: sseResponse("data: first\n\n"),
      resp2: sseResponse("data: second\n\n"),
      reqBuf: addr reqBuf)
    createThread(thr, serveCaptureMulti, scfg)
    awaitListening()

    proc runReconn() {.async.} =
      var cfg = FastReconnect
      cfg.extraHeaders = @[("Authorization", "Bearer tok")]
      var openCount = 0
      let client = newAsyncSseClient(clientUrl(port), config = cfg)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()
      await client.connect()

    waitFor runReconn()
    joinThread(thr)

    check "Authorization: Bearer tok" in reqBuf[0]
    check "Authorization: Bearer tok" in reqBuf[1]

# ===========================================================================
# 14. Accessors
# ===========================================================================

suite "Accessors":

  test "lastEventId reflects parser state":
    withServer(sseResponse("id: abc\ndata: hello\n\n")):
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = discard
      check client.lastEventId == ""
      await client.connect()
      check client.lastEventId == "abc"

  test "reconnectionTime reflects parser state including server retry":
    withServer(sseResponse("retry: 7500\ndata: hi\n\n")):
      let client = newAsyncSseClient(clientUrl(port), config = NoReconnect)
      client.onEvent = proc(e: SseEvent) = discard
      check client.reconnectionTime == 3000
      await client.connect()
      check client.reconnectionTime == 7500
