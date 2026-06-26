import std/[unittest, strutils, httpcore, os]
import sse/[types, server]

suite "server - sseHeaders":
  test "returns correct Content-Type":
    let h = sseHeaders()
    check h["Content-Type"] == "text/event-stream; charset=utf-8"

  test "returns Cache-Control no-cache":
    let h = sseHeaders()
    check h["Cache-Control"] == "no-cache"

  test "returns Connection keep-alive":
    let h = sseHeaders()
    check h["Connection"] == "keep-alive"

  test "returns X-Accel-Buffering no":
    let h = sseHeaders()
    check h["X-Accel-Buffering"] == "no"

  test "SseContentType constant":
    check SseContentType == "text/event-stream"

suite "server - SseConnection basics":
  test "initSseConnection with default config":
    let conn = initSseConnection()
    check conn.buf == ""
    check conn.config.heartbeatInterval == 15_000

  test "initSseConnection with custom config":
    let conn = initSseConnection(initSseServerConfig(heartbeatInterval = 5000))
    check conn.config.heartbeatInterval == 5000

  test "flush returns buffer and clears it":
    var conn = initSseConnection()
    conn.sendComment("hello")
    let data = conn.flush()
    check data == ": hello\n\n"
    check conn.buf == ""

  test "flush on empty buffer returns empty string":
    var conn = initSseConnection()
    check conn.flush() == ""

suite "server - sendEvent":
  test "serializes event into buffer":
    var conn = initSseConnection()
    conn.sendEvent(SseEvent(data: "hello", eventType: "", id: "", retry: -1))
    check conn.buf == "data: hello\n\n"

  test "serializes event with all fields":
    var conn = initSseConnection()
    conn.sendEvent(SseEvent(data: "msg", eventType: "ping", id: "7", retry: 2000))
    check conn.buf == "event: ping\nid: 7\nretry: 2000\ndata: msg\n\n"

  test "multiple events append to buffer":
    var conn = initSseConnection()
    conn.sendEvent(SseEvent(data: "first", eventType: "", id: "", retry: -1))
    conn.sendEvent(SseEvent(data: "second", eventType: "", id: "", retry: -1))
    check conn.buf == "data: first\n\ndata: second\n\n"

  test "multi-line data handled correctly":
    var conn = initSseConnection()
    conn.sendEvent(SseEvent(data: "a\nb\nc", eventType: "", id: "", retry: -1))
    check conn.buf == "data: a\ndata: b\ndata: c\n\n"

suite "server - sendComment":
  test "sends empty comment":
    var conn = initSseConnection()
    conn.sendComment()
    check conn.buf == ":\n\n"

  test "sends comment with text":
    var conn = initSseConnection()
    conn.sendComment("keep-alive")
    check conn.buf == ": keep-alive\n\n"

  test "sends multi-line comment":
    var conn = initSseConnection()
    conn.sendComment("line1\nline2")
    check conn.buf == ": line1\n: line2\n\n"

suite "server - sendRetry":
  test "sends retry field":
    var conn = initSseConnection()
    conn.sendRetry(5000)
    check conn.buf == "retry: 5000\n\n"

  test "sends retry zero":
    var conn = initSseConnection()
    conn.sendRetry(0)
    check conn.buf == "retry: 0\n\n"

suite "server - heartbeat":
  test "no heartbeat immediately after init":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 100))
    check not conn.needsHeartbeat()
    check not conn.maybeSendHeartbeat()
    check conn.buf == ""

  test "heartbeat sent after interval elapses":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    check conn.needsHeartbeat()
    check conn.maybeSendHeartbeat()
    check conn.buf == ":\n\n"

  test "maybeSendHeartbeat resets timer":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    discard conn.maybeSendHeartbeat()
    check not conn.needsHeartbeat()

  test "sendEvent resets heartbeat timer":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    check conn.needsHeartbeat()
    conn.sendEvent(SseEvent(data: "x", eventType: "", id: "", retry: -1))
    check not conn.needsHeartbeat()

  test "sendComment resets heartbeat timer":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    check conn.needsHeartbeat()
    conn.sendComment("ping")
    check not conn.needsHeartbeat()

  test "sendRetry resets heartbeat timer":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    check conn.needsHeartbeat()
    conn.sendRetry(1000)
    check not conn.needsHeartbeat()

  test "auto-heartbeat prepended before event when interval elapsed":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    conn.sendEvent(SseEvent(data: "payload", eventType: "", id: "", retry: -1))
    check conn.buf.startsWith(":\n\n")
    check conn.buf.endsWith("data: payload\n\n")

  test "no auto-heartbeat when interval has not elapsed":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 60_000))
    conn.sendEvent(SseEvent(data: "fast", eventType: "", id: "", retry: -1))
    check conn.buf == "data: fast\n\n"

  test "heartbeat uses lastWriteTime not init time":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 1))
    sleep(5)
    conn.sendComment("reset")
    let afterComment = conn.flush()
    check afterComment == ": reset\n\n"
    check not conn.needsHeartbeat()
    sleep(5)
    check conn.needsHeartbeat()

  test "heartbeat disabled when interval is 0":
    var conn = initSseConnection(initSseServerConfig(heartbeatInterval = 0))
    sleep(5)
    check not conn.needsHeartbeat()
    check not conn.maybeSendHeartbeat()
    conn.sendEvent(SseEvent(data: "x", eventType: "", id: "", retry: -1))
    check conn.buf == "data: x\n\n"
