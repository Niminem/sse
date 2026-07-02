import std/[unittest, strutils]
import sse/writer
import sse/parser
import sse/types

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc roundTrip(input: SseEvent): SseEvent =
  ## Serialize an event with the writer, parse it back with the parser, and
  ## return the reconstructed event. Verifies round-trip correctness.
  let wire = serializeEvent(input)
  var events: seq[SseEvent] = @[]
  var p = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = events.add(e))
  p.feed(wire)
  p.complete()
  doAssert events.len == 1, "round-trip should produce exactly 1 event, got " & $events.len
  events[0]

proc parseWire(wire: string): seq[SseEvent] =
  ## Feed raw wire bytes through the parser and collect dispatched events.
  var events: seq[SseEvent] = @[]
  var p = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = events.add(e))
  p.feed(wire)
  p.complete()
  events

proc parseComments(wire: string): seq[string] =
  ## Feed raw wire bytes through the parser and collect comment strings.
  var comments: seq[string] = @[]
  var p = initSseParser(nil)
  p.onComment =
    proc (c: string) {.closure, gcsafe.} = comments.add(c)
  p.feed(wire)
  p.complete()
  comments

proc parseRetry(wire: string): int =
  ## Feed raw wire bytes through the parser and return the resulting
  ## reconnectionTime.
  var p = initSseParser(nil)
  p.feed(wire)
  p.complete()
  p.reconnectionTime

# ---------------------------------------------------------------------------
# 1. serializeEvent — Basic Functionality
# ---------------------------------------------------------------------------

suite "serializeEvent — Basic":

  test "Simple message event":
    let ev = SseEvent(eventType: "message", data: "hello")
    let wire = serializeEvent(ev)
    check wire == "data: hello\n\n"

  test "Explicit message type is elided":
    let ev = SseEvent(eventType: "message", data: "world")
    let wire = serializeEvent(ev)
    check "event:" notin wire

  test "Custom event type is emitted":
    let ev = SseEvent(eventType: "add", data: "73857293")
    let wire = serializeEvent(ev)
    check wire == "event: add\ndata: 73857293\n\n"

  test "Empty event type is elided (same as message default)":
    let ev = SseEvent(eventType: "", data: "test")
    let wire = serializeEvent(ev)
    check "event:" notin wire
    check wire == "data: test\n\n"

  test "lastEventId is emitted as id: line":
    let ev = SseEvent(eventType: "message", data: "x", lastEventId: "42")
    let wire = serializeEvent(ev)
    check "id: 42\n" in wire

  test "Empty lastEventId → no id: line emitted":
    let ev = SseEvent(eventType: "message", data: "x", lastEventId: "")
    let wire = serializeEvent(ev)
    check "id:" notin wire

  test "Terminated by blank line":
    let ev = SseEvent(data: "x")
    let wire = serializeEvent(ev)
    check wire.endsWith("\n\n")

# ---------------------------------------------------------------------------
# 2. serializeEvent — Multi-line Data
# ---------------------------------------------------------------------------

suite "serializeEvent — Multi-line Data":

  test "Data with one LF → two data: lines":
    let ev = SseEvent(data: "line1\nline2")
    let wire = serializeEvent(ev)
    check wire == "data: line1\ndata: line2\n\n"

  test "Data with multiple LFs":
    let ev = SseEvent(data: "a\nb\nc")
    let wire = serializeEvent(ev)
    check wire == "data: a\ndata: b\ndata: c\n\n"

  test "Data ending with LF → trailing empty data: line":
    let ev = SseEvent(data: "hello\n")
    let wire = serializeEvent(ev)
    check wire == "data: hello\ndata: \n\n"

  test "Data that is only a LF":
    let ev = SseEvent(data: "\n")
    let wire = serializeEvent(ev)
    check wire == "data: \ndata: \n\n"

  test "Data with consecutive LFs":
    let ev = SseEvent(data: "a\n\nb")
    let wire = serializeEvent(ev)
    check wire == "data: a\ndata: \ndata: b\n\n"

  test "Data with CR → split into separate data: lines":
    let ev = SseEvent(data: "hello\rworld")
    let wire = serializeEvent(ev)
    check wire == "data: hello\ndata: world\n\n"

  test "Data with CRLF → split into separate data: lines":
    let ev = SseEvent(data: "hello\r\nworld")
    let wire = serializeEvent(ev)
    check wire == "data: hello\ndata: world\n\n"

  test "Data with mixed line endings":
    let ev = SseEvent(data: "a\nb\rc\r\nd")
    let wire = serializeEvent(ev)
    check wire == "data: a\ndata: b\ndata: c\ndata: d\n\n"

  test "Data ending with CR → trailing empty data: line":
    let ev = SseEvent(data: "hello\r")
    let wire = serializeEvent(ev)
    check wire == "data: hello\ndata: \n\n"

  test "Data ending with CRLF → trailing empty data: line":
    let ev = SseEvent(data: "hello\r\n")
    let wire = serializeEvent(ev)
    check wire == "data: hello\ndata: \n\n"

  test "Data with standalone CR between LFs":
    let ev = SseEvent(data: "a\n\r\nb")
    let wire = serializeEvent(ev)
    check wire == "data: a\ndata: \ndata: b\n\n"

# ---------------------------------------------------------------------------
# 3. serializeEvent — Edge Cases
# ---------------------------------------------------------------------------

suite "serializeEvent — Edge Cases":

  test "Empty data":
    let ev = SseEvent(data: "")
    let wire = serializeEvent(ev)
    check wire == "data: \n\n"

  test "Data with leading space (preserved)":
    let ev = SseEvent(data: " hello")
    let wire = serializeEvent(ev)
    check wire == "data:  hello\n\n"

  test "Data with multiple leading spaces":
    let ev = SseEvent(data: "  two spaces")
    let wire = serializeEvent(ev)
    check wire == "data:   two spaces\n\n"

  test "Data containing colons":
    let ev = SseEvent(data: "key: value")
    let wire = serializeEvent(ev)
    check wire == "data: key: value\n\n"

  test "NULL bytes in lastEventId are stripped":
    let ev = SseEvent(data: "x", lastEventId: "ab\x00cd")
    let wire = serializeEvent(ev)
    check "id: abcd\n" in wire

  test "CR/LF in lastEventId are stripped":
    let ev = SseEvent(data: "x", lastEventId: "ab\ncd\ref")
    let wire = serializeEvent(ev)
    check "id: abcdef\n" in wire

  test "CR/LF in eventType are stripped":
    let ev = SseEvent(eventType: "foo\nbar", data: "x")
    let wire = serializeEvent(ev)
    check "event: foobar\n" in wire
    check wire.count("event:") == 1

  test "CRLF in eventType are stripped":
    let ev = SseEvent(eventType: "foo\r\nbar", data: "x")
    let wire = serializeEvent(ev)
    check "event: foobar\n" in wire

  test "eventType that is entirely line endings → elided (becomes empty)":
    let ev = SseEvent(eventType: "\r\n", data: "x")
    let wire = serializeEvent(ev)
    check "event:" notin wire

  test "All fields populated":
    let ev = SseEvent(eventType: "update", data: "payload", lastEventId: "99")
    let wire = serializeEvent(ev)
    check "event: update\n" in wire
    check "id: 99\n" in wire
    check "data: payload\n" in wire
    check wire.endsWith("\n\n")

# ---------------------------------------------------------------------------
# 4. serializeEvent — Round-Trip with Parser
# ---------------------------------------------------------------------------

suite "serializeEvent — Round-Trip":

  test "Simple event round-trips":
    let input = SseEvent(eventType: "message", data: "hello")
    let output = roundTrip(input)
    check output.eventType == "message"
    check output.data == "hello"

  test "Custom type round-trips":
    let input = SseEvent(eventType: "add", data: "123")
    let output = roundTrip(input)
    check output.eventType == "add"
    check output.data == "123"

  test "Multi-line data (LF) round-trips":
    let input = SseEvent(data: "line1\nline2\nline3")
    let output = roundTrip(input)
    check output.data == "line1\nline2\nline3"

  test "Data with CR round-trips as LF (normalized)":
    let input = SseEvent(data: "hello\rworld")
    let output = roundTrip(input)
    check output.data == "hello\nworld"

  test "Data with CRLF round-trips as LF (normalized)":
    let input = SseEvent(data: "hello\r\nworld")
    let output = roundTrip(input)
    check output.data == "hello\nworld"

  test "Data with mixed line endings round-trips (all normalized to LF)":
    let input = SseEvent(data: "a\nb\rc\r\nd")
    let output = roundTrip(input)
    check output.data == "a\nb\nc\nd"

  test "lastEventId round-trips":
    let input = SseEvent(data: "x", lastEventId: "42")
    let output = roundTrip(input)
    check output.lastEventId == "42"

  test "Empty data round-trips":
    let input = SseEvent(data: "")
    let output = roundTrip(input)
    check output.data == ""

  test "Data with trailing LF round-trips":
    let input = SseEvent(data: "hello\n")
    let output = roundTrip(input)
    check output.data == "hello\n"

  test "Data with leading space round-trips":
    let input = SseEvent(data: " leading")
    let output = roundTrip(input)
    check output.data == " leading"

  test "Data with multiple leading spaces round-trips":
    let input = SseEvent(data: "  two")
    let output = roundTrip(input)
    check output.data == "  two"

  test "All fields round-trip together":
    let input = SseEvent(eventType: "ping", data: "payload\nmore",
                         lastEventId: "7")
    let output = roundTrip(input)
    check output.eventType == "ping"
    check output.data == "payload\nmore"
    check output.lastEventId == "7"

  test "Spec Example 1 — simple messages round-trip":
    for msg in ["This is the first message.",
                "This is the second message, it\nhas two lines.",
                "This is the third message."]:
      let ev = SseEvent(data: msg)
      let parsed = roundTrip(ev)
      check parsed.data == msg
      check parsed.eventType == "message"

  test "Spec Example 2 — named events round-trip":
    let cases = [
      (typ: "add", dat: "73857293"),
      (typ: "remove", dat: "2153"),
      (typ: "add", dat: "113411"),
    ]
    for c in cases:
      let ev = SseEvent(eventType: c.typ, data: c.dat)
      let parsed = roundTrip(ev)
      check parsed.eventType == c.typ
      check parsed.data == c.dat

# ---------------------------------------------------------------------------
# 5. serializeComment
# ---------------------------------------------------------------------------

suite "serializeComment":

  test "Simple comment":
    let wire = serializeComment("keepalive")
    check wire == ": keepalive\n"

  test "Empty comment":
    let wire = serializeComment("")
    check wire == ": \n"

  test "Comment with spaces":
    let wire = serializeComment(" hello world ")
    check wire == ":  hello world \n"

  test "Multi-line comment (LF) → one comment per line":
    let wire = serializeComment("line1\nline2")
    check wire == ": line1\n: line2\n"

  test "Multi-line comment (CR) → one comment per line":
    let wire = serializeComment("line1\rline2")
    check wire == ": line1\n: line2\n"

  test "Multi-line comment (CRLF) → one comment per line":
    let wire = serializeComment("line1\r\nline2")
    check wire == ": line1\n: line2\n"

  test "Comment with mixed line endings":
    let wire = serializeComment("a\nb\rc\r\nd")
    check wire == ": a\n: b\n: c\n: d\n"

  test "Comment ending with LF":
    let wire = serializeComment("hello\n")
    check wire == ": hello\n: \n"

  test "Comment ending with CR":
    let wire = serializeComment("hello\r")
    check wire == ": hello\n: \n"

  test "Comment ending with CRLF":
    let wire = serializeComment("hello\r\n")
    check wire == ": hello\n: \n"

  test "No trailing blank line (comment alone does not dispatch)":
    let wire = serializeComment("test")
    check not wire.endsWith("\n\n")

  test "Comment parsed correctly by parser":
    let wire = serializeComment("ping")
    let comments = parseComments(wire)
    check comments.len == 1
    check comments[0] == " ping"

  test "Multi-line comment parsed correctly":
    let wire = serializeComment("a\nb")
    let comments = parseComments(wire)
    check comments.len == 2
    check comments[0] == " a"
    check comments[1] == " b"

# ---------------------------------------------------------------------------
# 6. serializeRetry
# ---------------------------------------------------------------------------

suite "serializeRetry":

  test "Basic retry value":
    let wire = serializeRetry(3000)
    check wire == "retry: 3000\n"

  test "Zero retry":
    let wire = serializeRetry(0)
    check wire == "retry: 0\n"

  test "Large value":
    let wire = serializeRetry(999999)
    check wire == "retry: 999999\n"

  test "Negative value clamped to 0":
    let wire = serializeRetry(-1)
    check wire == "retry: 0\n"

  test "No trailing blank line":
    let wire = serializeRetry(5000)
    check not wire.endsWith("\n\n")

  test "Parsed correctly by parser":
    let wire = serializeRetry(5000) & "data: x\n\n"
    let rt = parseRetry(wire)
    check rt == 5000

# ---------------------------------------------------------------------------
# 7. serializeId
# ---------------------------------------------------------------------------

suite "serializeId":

  test "Basic id value":
    let wire = serializeId("42")
    check wire == "id: 42\n"

  test "Empty id (resets last event ID)":
    let wire = serializeId("")
    check wire == "id: \n"

  test "Id with special characters":
    let wire = serializeId("abc-123_xyz")
    check wire == "id: abc-123_xyz\n"

  test "NULL bytes in id are stripped":
    let wire = serializeId("a\x00b\x00c")
    check wire == "id: abc\n"

  test "Id that is only NULL bytes → effectively empty":
    let wire = serializeId("\x00\x00")
    check wire == "id: \n"

  test "CR/LF in id are stripped":
    let wire = serializeId("ab\ncd\ref")
    check wire == "id: abcdef\n"

  test "CRLF in id are stripped":
    let wire = serializeId("ab\r\ncd")
    check wire == "id: abcd\n"

  test "No trailing blank line":
    let wire = serializeId("99")
    check not wire.endsWith("\n\n")

  test "Parsed correctly by parser (resets ID)":
    let wire = "id: first\ndata: a\n\n" & serializeId("") & "data: b\n\n"
    let events = parseWire(wire)
    check events.len == 2
    check events[0].lastEventId == "first"
    check events[1].lastEventId == ""

  test "Parsed correctly by parser (sets ID)":
    let wire = serializeId("77") & "data: x\n\n"
    let events = parseWire(wire)
    check events.len == 1
    check events[0].lastEventId == "77"

# ---------------------------------------------------------------------------
# 8. Composition — Multiple serialized pieces together
# ---------------------------------------------------------------------------

suite "Composition":

  test "Comment + retry + event → parsed correctly":
    var wire = serializeComment("hello")
    wire.add(serializeRetry(2000))
    wire.add(serializeEvent(SseEvent(eventType: "update", data: "payload",
                                     lastEventId: "1")))
    let events = parseWire(wire)
    check events.len == 1
    check events[0].eventType == "update"
    check events[0].data == "payload"
    check events[0].lastEventId == "1"

  test "Multiple events concatenated":
    var wire = ""
    wire.add(serializeEvent(SseEvent(data: "first")))
    wire.add(serializeEvent(SseEvent(data: "second")))
    wire.add(serializeEvent(SseEvent(data: "third")))
    let events = parseWire(wire)
    check events.len == 3
    check events[0].data == "first"
    check events[1].data == "second"
    check events[2].data == "third"

  test "Standalone id followed by event → id carries through":
    var wire = serializeId("start")
    wire.add(serializeEvent(SseEvent(data: "hello")))
    let events = parseWire(wire)
    check events.len == 1
    check events[0].lastEventId == "start"

  test "Retry between events → parser reconnectionTime updated":
    var wire = serializeEvent(SseEvent(data: "a"))
    wire.add(serializeRetry(7000))
    wire.add(serializeEvent(SseEvent(data: "b")))
    var events: seq[SseEvent] = @[]
    var p = initSseParser(
      proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: events.add(e))
    p.feed(wire)
    p.complete()
    check events.len == 2
    check p.reconnectionTime == 7000
