import std/[unittest, strutils]
import sse/server
import sse/parser
import sse/types

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc parseWire(wire: string): seq[SseEvent] =
  var events: seq[SseEvent] = @[]
  var p = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = events.add(e))
  p.feed(wire)
  p.complete()
  events

proc parseComments(wire: string): seq[string] =
  var comments: seq[string] = @[]
  var p = initSseParser(nil)
  p.onComment =
    proc (c: string) {.closure, gcsafe.} = comments.add(c)
  p.feed(wire)
  p.complete()
  comments

proc parseRetry(wire: string): int =
  var p = initSseParser(nil)
  p.feed(wire)
  p.complete()
  p.reconnectionTime

# ---------------------------------------------------------------------------
# 1. initSseConnection
# ---------------------------------------------------------------------------

suite "initSseConnection":

  test "Default state":
    let conn = initSseConnection()
    check conn.nextId == 1
    check conn.lastId == ""
    check conn.lastActivityMs == 0
    check conn.keepAliveIntervalMs == 15_000

  test "Custom historyLimit and keepAliveIntervalMs":
    let conn = initSseConnection(historyLimit = 50, keepAliveIntervalMs = 5000)
    check conn.keepAliveIntervalMs == 5000

  test "Custom nowMs seeds lastActivityMs":
    let conn = initSseConnection(nowMs = 42000)
    check conn.lastActivityMs == 42000

# ---------------------------------------------------------------------------
# 2. formatEvent — Auto-incrementing IDs
# ---------------------------------------------------------------------------

suite "formatEvent — Auto-increment IDs":

  test "First event gets id 1":
    var conn = initSseConnection()
    let wire = conn.formatEvent("hello")
    check "id: 1\n" in wire
    check conn.lastId == "1"
    check conn.nextId == 2

  test "Sequential events auto-increment":
    var conn = initSseConnection()
    discard conn.formatEvent("a")
    discard conn.formatEvent("b")
    let wire = conn.formatEvent("c")
    check "id: 3\n" in wire
    check conn.lastId == "3"
    check conn.nextId == 4

  test "Explicit id overrides auto-increment":
    var conn = initSseConnection()
    let wire = conn.formatEvent("x", id = "custom-99")
    check "id: custom-99\n" in wire
    check conn.lastId == "custom-99"
    check conn.nextId == 1  # not advanced

  test "Auto-increment resumes after explicit id":
    var conn = initSseConnection()
    discard conn.formatEvent("a")          # id=1, nextId=2
    discard conn.formatEvent("b", id = "X") # id=X, nextId stays 2
    let wire = conn.formatEvent("c")       # id=2, nextId=3
    check "id: 2\n" in wire
    check conn.nextId == 3

# ---------------------------------------------------------------------------
# 3. formatEvent — Wire output
# ---------------------------------------------------------------------------

suite "formatEvent — Wire output":

  test "Simple message (default type elided)":
    var conn = initSseConnection()
    let wire = conn.formatEvent("hello")
    check "data: hello\n" in wire
    check "event:" notin wire
    check wire.endsWith("\n\n")

  test "Custom event type":
    var conn = initSseConnection()
    let wire = conn.formatEvent("payload", eventType = "update")
    check "event: update\n" in wire
    check "data: payload\n" in wire

  test "Multi-line data":
    var conn = initSseConnection()
    let wire = conn.formatEvent("line1\nline2")
    check "data: line1\ndata: line2\n" in wire

  test "Empty data":
    var conn = initSseConnection()
    let wire = conn.formatEvent("")
    check "data: \n" in wire

  test "Data with leading space preserved":
    var conn = initSseConnection()
    let wire = conn.formatEvent(" hello")
    check "data:  hello\n" in wire

  test "eventType 'message' is elided":
    var conn = initSseConnection()
    let wire = conn.formatEvent("x", eventType = "message")
    check "event:" notin wire

# ---------------------------------------------------------------------------
# 4. formatComment
# ---------------------------------------------------------------------------

suite "formatComment":

  test "Simple comment":
    check formatComment("hello") == ": hello\n"

  test "Empty comment":
    check formatComment("") == ": \n"

  test "Multi-line comment":
    check formatComment("a\nb") == ": a\n: b\n"

  test "No trailing blank line":
    check not formatComment("test").endsWith("\n\n")

# ---------------------------------------------------------------------------
# 5. formatRetry
# ---------------------------------------------------------------------------

suite "formatRetry":

  test "Basic retry":
    check formatRetry(3000) == "retry: 3000\n"

  test "Zero":
    check formatRetry(0) == "retry: 0\n"

  test "Negative clamped to 0":
    check formatRetry(-5) == "retry: 0\n"

  test "No trailing blank line":
    check not formatRetry(1000).endsWith("\n\n")

# ---------------------------------------------------------------------------
# 6. formatId
# ---------------------------------------------------------------------------

suite "formatId":

  test "Basic id":
    var conn = initSseConnection()
    let wire = conn.formatId("99")
    check wire == "id: 99\n"

  test "Updates conn.lastId":
    var conn = initSseConnection()
    discard conn.formatId("hello")
    check conn.lastId == "hello"

  test "Empty id (resets)":
    var conn = initSseConnection()
    conn.lastId = "previous"
    let wire = conn.formatId("")
    check wire == "id: \n"
    check conn.lastId == ""

  test "NULL/CR/LF stripped from wire output":
    var conn = initSseConnection()
    let wire = conn.formatId("a\x00b\nc\rd")
    check wire == "id: abcd\n"

  test "conn.lastId stores raw value (not sanitized)":
    var conn = initSseConnection()
    discard conn.formatId("a\x00b")
    check conn.lastId == "a\x00b"

# ---------------------------------------------------------------------------
# 7. Replay buffer
# ---------------------------------------------------------------------------

suite "Replay buffer — basic":

  test "historyLimit 0 → no events stored":
    var conn = initSseConnection(historyLimit = 0)
    discard conn.formatEvent("a")
    discard conn.formatEvent("b")
    check conn.hasEvent("1") == false
    check conn.replay("") == ""

  test "Events stored when historyLimit > 0":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("hello")
    check conn.hasEvent("1") == true

  test "hasEvent returns false for missing id":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("x")
    check conn.hasEvent("999") == false

  test "hasEvent with explicit id":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("x", id = "abc")
    check conn.hasEvent("abc") == true
    check conn.hasEvent("1") == false

suite "Replay buffer — replay":

  test "Replay after a known id":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("first")    # id=1
    discard conn.formatEvent("second")   # id=2
    discard conn.formatEvent("third")    # id=3
    let r = conn.replay("1")
    let events = parseWire(r)
    check events.len == 2
    check events[0].data == "second"
    check events[1].data == "third"

  test "Replay with unknown id returns all":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("a")
    discard conn.formatEvent("b")
    let r = conn.replay("unknown")
    let events = parseWire(r)
    check events.len == 2
    check events[0].data == "a"
    check events[1].data == "b"

  test "Replay with empty lastEventId returns all":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("x")
    discard conn.formatEvent("y")
    let r = conn.replay("")
    let events = parseWire(r)
    check events.len == 2

  test "Replay after last id returns empty":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("only")  # id=1
    let r = conn.replay("1")
    check r == ""

  test "Replay preserves wire format exactly":
    var conn = initSseConnection(historyLimit = 10)
    let wire = conn.formatEvent("data here", eventType = "ping", id = "ev-1")
    let r = conn.replay("")
    check r == wire

suite "Replay buffer — eviction":

  test "Oldest event evicted when limit reached":
    var conn = initSseConnection(historyLimit = 3)
    discard conn.formatEvent("a")  # id=1
    discard conn.formatEvent("b")  # id=2
    discard conn.formatEvent("c")  # id=3
    discard conn.formatEvent("d")  # id=4 → evicts id=1
    check conn.hasEvent("1") == false
    check conn.hasEvent("2") == true
    check conn.hasEvent("3") == true
    check conn.hasEvent("4") == true

  test "Eviction maintains FIFO order":
    var conn = initSseConnection(historyLimit = 2)
    discard conn.formatEvent("a")  # id=1
    discard conn.formatEvent("b")  # id=2
    discard conn.formatEvent("c")  # id=3 → evicts id=1
    discard conn.formatEvent("d")  # id=4 → evicts id=2
    let r = conn.replay("")
    let events = parseWire(r)
    check events.len == 2
    check events[0].data == "c"
    check events[1].data == "d"

  test "historyLimit 1 only keeps last event":
    var conn = initSseConnection(historyLimit = 1)
    discard conn.formatEvent("first")
    discard conn.formatEvent("second")
    discard conn.formatEvent("third")
    let r = conn.replay("")
    let events = parseWire(r)
    check events.len == 1
    check events[0].data == "third"

# ---------------------------------------------------------------------------
# 8. httpHeaders
# ---------------------------------------------------------------------------

suite "httpHeaders — Basic":

  test "Contains status line":
    let h = httpHeaders()
    check h.startsWith("HTTP/1.1 200 OK\r\n")

  test "Contains Content-Type header":
    let h = httpHeaders()
    check "Content-Type: text/event-stream\r\n" in h

  test "Contains Cache-Control header":
    let h = httpHeaders()
    check "Cache-Control: no-cache\r\n" in h

  test "Contains Connection header":
    let h = httpHeaders()
    check "Connection: keep-alive\r\n" in h

  test "Terminated by CRLF CRLF":
    let h = httpHeaders()
    check h.endsWith("\r\n\r\n")

  test "Custom Content-Type":
    let h = httpHeaders(contentType = "text/event-stream; charset=utf-8")
    check "Content-Type: text/event-stream; charset=utf-8\r\n" in h

  test "Uses CRLF line endings throughout":
    let h = httpHeaders()
    let withoutCrlf = h.replace("\r\n", "")
    check '\r' notin withoutCrlf
    check '\n' notin withoutCrlf

suite "httpHeaders — Extra Headers":

  test "Single extra header":
    let h = httpHeaders(extraHeaders = [("Access-Control-Allow-Origin", "*")])
    check "Access-Control-Allow-Origin: *\r\n" in h

  test "Multiple extra headers":
    let h = httpHeaders(extraHeaders = [
      ("Access-Control-Allow-Origin", "*"),
      ("X-Custom", "value"),
    ])
    check "Access-Control-Allow-Origin: *\r\n" in h
    check "X-Custom: value\r\n" in h

  test "Extra headers appear before final blank line":
    let h = httpHeaders(extraHeaders = [("X-Test", "123")])
    let idx = h.find("X-Test: 123\r\n")
    let endIdx = h.find("\r\n\r\n")
    check idx < endIdx

  test "Empty extra headers array → same as default":
    check httpHeaders() == httpHeaders(extraHeaders = [])

# ---------------------------------------------------------------------------
# 9. parseLastEventId
# ---------------------------------------------------------------------------

suite "parseLastEventId":

  test "Found in typical request":
    let headers = "GET /events HTTP/1.1\r\nHost: example.com\r\nLast-Event-ID: 42\r\n\r\n"
    check parseLastEventId(headers) == "42"

  test "Case-insensitive match":
    let headers = "GET / HTTP/1.1\r\nlast-event-id: abc\r\n\r\n"
    check parseLastEventId(headers) == "abc"

  test "Mixed case":
    let headers = "GET / HTTP/1.1\r\nLAST-EVENT-ID: XYZ\r\n\r\n"
    check parseLastEventId(headers) == "XYZ"

  test "Missing header returns empty string":
    let headers = "GET / HTTP/1.1\r\nHost: x\r\n\r\n"
    check parseLastEventId(headers) == ""

  test "Trims leading whitespace":
    let headers = "Last-Event-ID:   spaces\r\n\r\n"
    check parseLastEventId(headers) == "spaces"

  test "Trims trailing whitespace":
    let headers = "Last-Event-ID: trailing   \r\n\r\n"
    check parseLastEventId(headers) == "trailing"

  test "Trims tabs":
    let headers = "Last-Event-ID:\t\ttabbed\t\r\n\r\n"
    check parseLastEventId(headers) == "tabbed"

  test "Empty value":
    let headers = "Last-Event-ID:\r\n\r\n"
    check parseLastEventId(headers) == ""

  test "Value with colons":
    let headers = "Last-Event-ID: 2024-01-01T12:00:00Z\r\n\r\n"
    check parseLastEventId(headers) == "2024-01-01T12:00:00Z"

  test "Multiple headers — first wins":
    let headers = "Last-Event-ID: first\r\nLast-Event-ID: second\r\n\r\n"
    check parseLastEventId(headers) == "first"

  test "Not confused by partial match":
    let headers = "X-Last-Event-ID: nope\r\nLast-Event-ID: yes\r\n\r\n"
    check parseLastEventId(headers) == "yes"

  test "LF-only line endings":
    let headers = "GET / HTTP/1.1\nLast-Event-ID: lf-only\n\n"
    check parseLastEventId(headers) == "lf-only"

# ---------------------------------------------------------------------------
# 10. Keep-alive tracking
# ---------------------------------------------------------------------------

suite "Keep-alive tracking":

  test "needsKeepAlive false when within interval":
    let conn = initSseConnection(nowMs = 1000)
    check conn.needsKeepAlive(nowMs = 5000) == false

  test "needsKeepAlive true when at interval":
    let conn = initSseConnection(keepAliveIntervalMs = 15000, nowMs = 0)
    check conn.needsKeepAlive(nowMs = 15000) == true

  test "needsKeepAlive true when past interval":
    let conn = initSseConnection(keepAliveIntervalMs = 15000, nowMs = 0)
    check conn.needsKeepAlive(nowMs = 20000) == true

  test "markActivity resets the timer":
    var conn = initSseConnection(keepAliveIntervalMs = 10000, nowMs = 0)
    check conn.needsKeepAlive(nowMs = 10000) == true
    conn.markActivity(nowMs = 10000)
    check conn.needsKeepAlive(nowMs = 15000) == false
    check conn.needsKeepAlive(nowMs = 20000) == true

  test "Custom keepAliveIntervalMs":
    let conn = initSseConnection(keepAliveIntervalMs = 5000, nowMs = 0)
    check conn.needsKeepAlive(nowMs = 4999) == false
    check conn.needsKeepAlive(nowMs = 5000) == true

# ---------------------------------------------------------------------------
# 11. keepAliveComment
# ---------------------------------------------------------------------------

suite "keepAliveComment":

  test "Returns expected comment line":
    check keepAliveComment() == ": keepalive\n"

  test "No trailing blank line":
    check not keepAliveComment().endsWith("\n\n")

  test "Parsed as comment by parser":
    let comments = parseComments(keepAliveComment())
    check comments.len == 1
    check comments[0] == " keepalive"

# ---------------------------------------------------------------------------
# 12. Stream buffer (add / flush)
# ---------------------------------------------------------------------------

suite "Stream buffer — add / flush":

  test "Empty flush returns empty string":
    var conn = initSseConnection()
    check conn.flush() == ""

  test "Single add then flush":
    var conn = initSseConnection()
    conn.add("hello")
    check conn.flush() == "hello"

  test "Flush clears the buffer":
    var conn = initSseConnection()
    conn.add("data")
    discard conn.flush()
    check conn.flush() == ""

  test "Multiple adds accumulate":
    var conn = initSseConnection()
    conn.add("one")
    conn.add("two")
    conn.add("three")
    check conn.flush() == "onetwothree"

  test "Add formatEvent output then flush":
    var conn = initSseConnection()
    conn.add(conn.formatEvent("hello"))
    conn.add(conn.formatEvent("world"))
    let flushed = conn.flush()
    let events = parseWire(flushed)
    check events.len == 2
    check events[0].data == "hello"
    check events[1].data == "world"

  test "Add mixed content":
    var conn = initSseConnection()
    conn.add(formatRetry(5000))
    conn.add(formatComment("ping"))
    conn.add(conn.formatEvent("data"))
    let flushed = conn.flush()
    check "retry: 5000\n" in flushed
    check ": ping\n" in flushed
    check "data: data\n" in flushed

# ---------------------------------------------------------------------------
# 13. Round-trip — formatEvent through parser
# ---------------------------------------------------------------------------

suite "Round-trip — formatEvent through parser":

  test "Simple event round-trips":
    var conn = initSseConnection()
    let wire = conn.formatEvent("hello world")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].eventType == "message"
    check events[0].data == "hello world"
    check events[0].lastEventId == "1"

  test "Named event round-trips":
    var conn = initSseConnection()
    let wire = conn.formatEvent("123", eventType = "add")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].eventType == "add"
    check events[0].data == "123"

  test "Explicit id round-trips":
    var conn = initSseConnection()
    let wire = conn.formatEvent("payload", id = "ev-55")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].lastEventId == "ev-55"

  test "Multi-line data round-trips":
    var conn = initSseConnection()
    let wire = conn.formatEvent("line1\nline2\nline3")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].data == "line1\nline2\nline3"

  test "All fields round-trip":
    var conn = initSseConnection()
    let wire = conn.formatEvent("payload\nmore", eventType = "update", id = "9")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].eventType == "update"
    check events[0].data == "payload\nmore"
    check events[0].lastEventId == "9"

  test "Multiple sequential events":
    var conn = initSseConnection()
    var wire = ""
    wire.add(conn.formatEvent("first"))
    wire.add(conn.formatEvent("second"))
    wire.add(conn.formatEvent("third"))
    let events = parseWire(wire)
    check events.len == 3
    check events[0].data == "first"
    check events[0].lastEventId == "1"
    check events[1].data == "second"
    check events[1].lastEventId == "2"
    check events[2].data == "third"
    check events[2].lastEventId == "3"

  test "formatRetry parsed correctly":
    let wire = formatRetry(5000) & "data: x\n\n"
    check parseRetry(wire) == 5000

  test "formatId sets parser last event ID":
    var conn = initSseConnection()
    var wire = conn.formatId("start")
    wire.add(conn.formatEvent("hello"))
    let events = parseWire(wire)
    check events.len == 1
    check events[0].lastEventId == "1"

  test "Empty data round-trips":
    var conn = initSseConnection()
    let wire = conn.formatEvent("")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].data == ""

  test "Data with special characters":
    var conn = initSseConnection()
    let wire = conn.formatEvent("hello: world = {\"key\": \"value\"}")
    let events = parseWire(wire)
    check events.len == 1
    check events[0].data == "hello: world = {\"key\": \"value\"}"

# ---------------------------------------------------------------------------
# 14. Integration — replay through parser
# ---------------------------------------------------------------------------

suite "Integration — replay through parser":

  test "Replayed events parse correctly":
    var conn = initSseConnection(historyLimit = 10)
    discard conn.formatEvent("alpha", eventType = "msg")
    discard conn.formatEvent("beta", eventType = "msg")
    discard conn.formatEvent("gamma", eventType = "msg")
    let replayed = conn.replay("1")
    let events = parseWire(replayed)
    check events.len == 2
    check events[0].data == "beta"
    check events[0].lastEventId == "2"
    check events[1].data == "gamma"
    check events[1].lastEventId == "3"

  test "Full reconnection flow":
    var conn = initSseConnection(historyLimit = 100)
    discard conn.formatEvent("event-1")
    discard conn.formatEvent("event-2")
    discard conn.formatEvent("event-3")

    let reqHeaders = "GET /events HTTP/1.1\r\nLast-Event-ID: 1\r\n\r\n"
    let clientLastId = parseLastEventId(reqHeaders)
    check clientLastId == "1"

    let missedWire = conn.replay(clientLastId)
    let missed = parseWire(missedWire)
    check missed.len == 2
    check missed[0].data == "event-2"
    check missed[1].data == "event-3"
