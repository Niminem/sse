import std/unittest
import sse/parser
import sse/types

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc collectEvents(input: string; chunked = false): seq[SseEvent] =
  var events: seq[SseEvent] = @[]
  var p = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: events.add(e))
  if chunked:
    for ch in input:
      p.feed($ch)
  else:
    p.feed(input)
  p.complete()
  events

proc collectEventsMultiFeed(chunks: openArray[string]): seq[SseEvent] =
  var events: seq[SseEvent] = @[]
  var p = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: events.add(e))
  for c in chunks:
    p.feed(c)
  p.complete()
  events

template makeParser(eventsVar, parserVar: untyped) =
  var eventsVar: seq[SseEvent] = @[]
  var parserVar = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: eventsVar.add(e))

template makeParserWithComments(eventsVar, commentsVar, parserVar: untyped) =
  var eventsVar: seq[SseEvent] = @[]
  var commentsVar: seq[string] = @[]
  var parserVar = initSseParser(
    proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: eventsVar.add(e))
  parserVar.onComment =
    proc (c: string) {.closure, gcsafe.} = {.cast(gcsafe).}: commentsVar.add(c)

# ---------------------------------------------------------------------------
# 1. Line Ending Handling (§3.2)
# ---------------------------------------------------------------------------

suite "Line Ending Handling":

  test "LF terminates a line":
    let events = collectEvents("data: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "CR terminates a line":
    let events = collectEvents("data: hello\r\r")
    check events.len == 1
    check events[0].data == "hello"

  test "CR LF terminates a single line (not two)":
    let events = collectEvents("data: hello\r\n\r\n")
    check events.len == 1
    check events[0].data == "hello"

  test "Mixed line endings within the same stream":
    let events = collectEvents("data: a\ndata: b\rdata: c\r\n\n")
    check events.len == 1
    check events[0].data == "a\nb\nc"

  test "CR as last byte of one feed(), LF as first byte of next → one line, not two":
    makeParser(events, p)
    p.feed("data: hello\r")
    p.feed("\n\r\n")
    p.complete()
    check events.len == 1
    check events[0].data == "hello"

  test "CR as last byte of one feed(), non-LF as first byte of next → standalone CR line ending":
    makeParser(events, p)
    p.feed("data: hello\r")
    p.feed("data: world\n\n")
    p.complete()
    check events.len == 1
    check events[0].data == "hello\nworld"

  test "CR as last byte of stream → complete() flushes it as a valid line ending":
    makeParser(events, p)
    # LF terminates "data: hello"; the trailing CR is the blank line separator.
    p.feed("data: hello\n\r")
    p.complete()
    check events.len == 1
    check events[0].data == "hello"

# ---------------------------------------------------------------------------
# 2. BOM Stripping (§2.6, §3.1)
# ---------------------------------------------------------------------------

suite "BOM Stripping":

  test "Stream starts with UTF-8 BOM → strip it, parse rest normally":
    let events = collectEvents("\xEF\xBB\xBFdata: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "Stream does not start with BOM → no bytes lost":
    let events = collectEvents("data: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "BOM split across chunks (1 byte in first feed(), 2 in second)":
    let events = collectEventsMultiFeed(["\xEF", "\xBB\xBF" & "data: hello\n\n"])
    check events.len == 1
    check events[0].data == "hello"

  test "BOM split across three chunks (1 byte each)":
    let events = collectEventsMultiFeed(["\xEF", "\xBB", "\xBFdata: hello\n\n"])
    check events.len == 1
    check events[0].data == "hello"

  test "Stream shorter than 3 bytes with no BOM → bytes preserved, resolved via complete()":
    # "d\n" is 2 bytes (< 3), stays in bomBuf until complete() resolves it.
    # "d" is an unknown field (no colon), ignored. LF = blank line → dispatch with
    # empty data buffer → no event. No crash, no data lost.
    makeParser(events, p)
    p.feed("d\n")
    p.complete()
    check events.len == 0

  test "Stream shorter than 3 bytes preserves data when complete() resolves":
    # Feed only 1 byte; complete() resolves BOM, processes the byte.
    makeParser(events, p)
    p.feed("a")
    p.complete()
    check events.len == 0

  test "BOM-like prefix that isn't a full BOM → bytes preserved":
    # "\xEF\xBB" + "X" → not a BOM. The 3 bytes pass through to the line scanner.
    # We add a line ending to separate them from subsequent data.
    let events = collectEventsMultiFeed(["\xEF\xBB", "X\ndata: ok\n\n"])
    # "\xEF\xBBX" is an unknown field (no colon), ignored.
    # "data: ok" → field "data", value "ok". Blank line dispatches.
    check events.len == 1
    check events[0].data == "ok"

  test "BOM followed immediately by data in the same chunk":
    let events = collectEvents("\xEF\xBB\xBFdata: immediate\n\n")
    check events.len == 1
    check events[0].data == "immediate"

  test "U+FEFF mid-stream is NOT stripped (passes through as regular character)":
    let events = collectEvents("data: before\xEF\xBB\xBFafter\n\n")
    check events.len == 1
    check events[0].data == "before\xEF\xBB\xBFafter"

# ---------------------------------------------------------------------------
# 3. Comment Lines (§3.4 Rule 2)
# ---------------------------------------------------------------------------

suite "Comment Lines":

  test "Line starting with : is a comment; no event fired":
    let events = collectEvents(": this is a comment\n\n")
    check events.len == 0

  test "Comment content passed to onComment (everything after the :)":
    makeParserWithComments(events, comments, p)
    p.feed(": hello world\n\n")
    p.complete()
    check comments.len == 1
    check comments[0] == " hello world"

  test "Empty comment (: alone) → onComment receives empty string":
    makeParserWithComments(events, comments, p)
    p.feed(":\n\n")
    p.complete()
    check comments.len == 1
    check comments[0] == ""

  test "Comment with leading space → space is NOT stripped":
    makeParserWithComments(events, comments, p)
    p.feed(": keepalive\n\n")
    p.complete()
    check comments.len == 1
    check comments[0] == " keepalive"

  test "Comments between field lines do not interfere with event accumulation":
    let events = collectEvents("data: a\n: ignore me\ndata: b\n\n")
    check events.len == 1
    check events[0].data == "a\nb"

  test "Comment interleaved within a single event block":
    let events = collectEvents("data: a\n: comment\ndata: b\n\n")
    check events.len == 1
    check events[0].data == "a\nb"

  test "Comment-only block (comment + blank line) → no event dispatched":
    let events = collectEvents(": just a comment\n\n")
    check events.len == 0

# ---------------------------------------------------------------------------
# 4. Field Parsing (§3.4 Rules 3 & 4)
# ---------------------------------------------------------------------------

suite "Field Parsing":

  test "Field with colon and value":
    let events = collectEvents("data: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "Single leading space after colon is stripped":
    let events = collectEvents("data: hello\n\n")
    check events[0].data == "hello"

  test "Only the first space is stripped":
    let events = collectEvents("data:  hello\n\n")
    check events.len == 1
    check events[0].data == " hello"

  test "Tab after colon is NOT stripped (only U+0020 SPACE is removed)":
    let events = collectEvents("data:\thello\n\n")
    check events.len == 1
    check events[0].data == "\thello"

  test "No space after colon":
    let events = collectEvents("data:hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "Colon with nothing after → value is empty string":
    let events = collectEvents("data:\n\n")
    check events.len == 1
    check events[0].data == ""

  test "Multiple colons → split on first colon only":
    let events = collectEvents("data: foo:bar:baz\n\n")
    check events.len == 1
    check events[0].data == "foo:bar:baz"

  test "Field with no colon → field name is entire line, value is empty":
    # "data" with no colon → field "data", value "" → appends "" + LF
    let events = collectEvents("data\n\n")
    check events.len == 1
    check events[0].data == ""

  test "Unknown field names → silently ignored":
    let events = collectEvents("foo: bar\n\n")
    check events.len == 0

  test "Case sensitivity: capitalized field names are not recognized":
    let events = collectEvents("Data: hello\nEvent: add\nID: 1\nRetry: 5000\n\n")
    check events.len == 0

# ---------------------------------------------------------------------------
# 5. `data` Field Processing (§4.2)
# ---------------------------------------------------------------------------

suite "data Field Processing":

  test "Single data line":
    let events = collectEvents("data: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "Multiple data lines":
    let events = collectEvents("data: a\ndata: b\n\n")
    check events.len == 1
    check events[0].data == "a\nb"

  test "Empty data field, no colon → event IS fired with empty string":
    let events = collectEvents("data\n\n")
    check events.len == 1
    check events[0].data == ""

  test "Empty data field, colon no value → event IS fired with empty string":
    let events = collectEvents("data:\n\n")
    check events.len == 1
    check events[0].data == ""

  test "Two empty data lines → data is single newline":
    let events = collectEvents("data\ndata\n\n")
    check events.len == 1
    check events[0].data == "\n"

  test "data with only a space → space stripped, value is empty":
    let events = collectEvents("data: \n\n")
    check events.len == 1
    check events[0].data == ""

  test "data interleaved with other fields":
    let events = collectEvents("data: a\nevent: foo\ndata: b\n\n")
    check events.len == 1
    check events[0].data == "a\nb"
    check events[0].eventType == "foo"

# ---------------------------------------------------------------------------
# 6. `event` Field Processing (§4.1)
# ---------------------------------------------------------------------------

suite "event Field Processing":

  test "Sets the event type":
    let events = collectEvents("event: add\ndata: x\n\n")
    check events[0].eventType == "add"

  test "Default when absent → message":
    let events = collectEvents("data: x\n\n")
    check events[0].eventType == "message"

  test "Default when empty (event: with empty value) → message":
    let events = collectEvents("event:\ndata: x\n\n")
    check events[0].eventType == "message"

  test "Resets per event → next event without event field gets message":
    let events = collectEvents("event: custom\ndata: first\n\ndata: second\n\n")
    check events.len == 2
    check events[0].eventType == "custom"
    check events[1].eventType == "message"

  test "Last event field wins when multiple appear in one block":
    let events = collectEvents("event: first\nevent: second\ndata: x\n\n")
    check events[0].eventType == "second"

  test "event with space-only value → type is a single space, not default":
    let events = collectEvents("event:  \ndata: x\n\n")
    check events[0].eventType == " "

  test "event field without data → no event fired, but type buffer still resets":
    let events = collectEvents("event: custom\n\ndata: x\n\n")
    check events.len == 1
    check events[0].eventType == "message"

# ---------------------------------------------------------------------------
# 7. `id` Field Processing (§4.3)
# ---------------------------------------------------------------------------

suite "id Field Processing":

  test "Sets last event ID":
    let events = collectEvents("id: 42\ndata: x\n\n")
    check events[0].lastEventId == "42"

  test "Initial value is empty string":
    let events = collectEvents("data: x\n\n")
    check events[0].lastEventId == ""

  test "Persists across events":
    let events = collectEvents("id: 1\ndata: a\n\ndata: b\n\n")
    check events.len == 2
    check events[0].lastEventId == "1"
    check events[1].lastEventId == "1"

  test "Reset to empty with id: (colon, no value)":
    let events = collectEvents("id: 1\ndata: a\n\nid:\ndata: b\n\n")
    check events[0].lastEventId == "1"
    check events[1].lastEventId == ""

  test "Reset to empty with id (no colon)":
    let events = collectEvents("id: 1\ndata: a\n\nid\ndata: b\n\n")
    check events[0].lastEventId == "1"
    check events[1].lastEventId == ""

  test "NULL rejection → id value containing \\0 is ignored":
    makeParser(events, p)
    p.feed("id: good\ndata: a\n\n")
    p.feed("id: bad\x00id\ndata: b\n\n")
    p.complete()
    check events[0].lastEventId == "good"
    check events[1].lastEventId == "good"

  test "NULL mixed with other chars → entire field ignored":
    makeParser(events, p)
    p.feed("id: prev\ndata: a\n\n")
    p.feed("id: abc\x00def\ndata: b\n\n")
    p.complete()
    check events[1].lastEventId == "prev"

  test "Multiple id fields in one block → last one wins":
    let events = collectEvents("id: first\nid: second\ndata: x\n\n")
    check events[0].lastEventId == "second"

  test "id field without data → no event fired, but ID buffer is updated":
    makeParser(events, p)
    p.feed("id: 5\n\ndata: hello\n\n")
    p.complete()
    check events.len == 1
    check events[0].lastEventId == "5"

# ---------------------------------------------------------------------------
# 8. `retry` Field Processing (§4.4)
# ---------------------------------------------------------------------------

suite "retry Field Processing":

  test "Valid: all ASCII digits sets reconnectionTime":
    makeParser(events, p)
    p.feed("retry: 3000\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000

  test "Invalid: contains non-digits → ignored":
    makeParser(events, p)
    p.feed("retry: 3000ms\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000  # default unchanged

  test "Invalid: non-digit characters → ignored":
    makeParser(events, p)
    p.feed("retry: abc\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000

  test "Empty value (retry:) → ignored":
    makeParser(events, p)
    p.feed("retry:\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000

  test "retry with no colon → value is empty → ignored":
    makeParser(events, p)
    p.feed("retry\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000

  test "Leading zeros → valid, parsed as 3000":
    makeParser(events, p)
    p.feed("retry: 0003000\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000

  test "Zero → valid, sets reconnectionTime to 0":
    makeParser(events, p)
    p.feed("retry: 0\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 0

  test "Very large number → saturates at high(int)":
    makeParser(events, p)
    p.feed("retry: 99999999999999999999999999\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == high(int)

  test "Extra leading space in value → contains space → ignored":
    makeParser(events, p)
    # "retry:  3000" → first space stripped → " 3000" has a space → not all digits
    p.feed("retry:  3000\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 3000  # default unchanged

  test "Multiple retry fields in one block → last valid one wins":
    makeParser(events, p)
    p.feed("retry: 1000\nretry: 2000\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 2000

  test "retry field does not affect event dispatch":
    let events = collectEvents("retry: 5000\ndata: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

# ---------------------------------------------------------------------------
# 9. Event Dispatch (§5)
# ---------------------------------------------------------------------------

suite "Event Dispatch":

  test "Blank line triggers dispatch":
    let events = collectEvents("data: x\n\n")
    check events.len == 1

  test "No data field = no event fired":
    let events = collectEvents("event: add\nid: 1\nretry: 1000\n\n")
    check events.len == 0

  test "Trailing LF stripped from data buffer before delivery":
    # Each data field appends value + LF; the final LF is stripped.
    let events = collectEvents("data: hello\n\n")
    check events[0].data == "hello"
    check events[0].data[^1] != '\n'

  test "Multiple consecutive blank lines → no extra events":
    let events = collectEvents("data: x\n\n\n\n\n")
    check events.len == 1

  test "Stream of only blank lines → zero events fired":
    let events = collectEvents("\n\n\n\n")
    check events.len == 0

  test "lastEventId updated even when no event fires":
    makeParser(events, p)
    p.feed("id: 5\n\n")
    p.complete()
    check events.len == 0
    check p.lastEventId == "5"

  test "id set in a no-data block carries to the next dispatched event":
    let events = collectEvents("id: 5\n\ndata: hello\n\n")
    check events.len == 1
    check events[0].lastEventId == "5"

  test "Event type buffer reset after dispatch; last event ID buffer is NOT reset":
    let events = collectEvents("event: custom\nid: 7\ndata: a\n\ndata: b\n\n")
    check events[0].eventType == "custom"
    check events[0].lastEventId == "7"
    check events[1].eventType == "message"
    check events[1].lastEventId == "7"

  test "onEvent receives correct eventType, data, lastEventId":
    let events = collectEvents("event: ping\nid: 99\ndata: payload\n\n")
    check events[0].eventType == "ping"
    check events[0].data == "payload"
    check events[0].lastEventId == "99"

  test "origin is empty (parser never sets it)":
    let events = collectEvents("data: x\n\n")
    check events[0].origin == ""

  test "nil onEvent → no crash on dispatch":
    var p = initSseParser(nil)
    p.feed("data: hello\n\n")
    p.complete()

# ---------------------------------------------------------------------------
# 10. End-of-Stream / complete() (§3.5)
# ---------------------------------------------------------------------------

suite "End-of-Stream / complete()":

  test "Incomplete event (no trailing blank line) is discarded":
    let events = collectEvents("data: hello\n")
    check events.len == 0

  test "Empty stream → complete() with no data fed → no events":
    makeParser(events, p)
    p.complete()
    check events.len == 0

  test "Stream ending mid-line → partial line discarded":
    let events = collectEvents("data: hel")
    check events.len == 0

  test "Stream ending after a complete event → complete() is a no-op":
    let events = collectEvents("data: done\n\n")
    check events.len == 1
    check events[0].data == "done"

  test "Pending CR at end of stream → line processed but incomplete event discarded":
    # "data: x\r" — CR at end is a valid line ending, so "data: x" is processed.
    # But no blank line follows, so the event block is incomplete → discarded.
    makeParser(events, p)
    p.feed("data: x\r")
    p.complete()
    check events.len == 0

  test "BOM-only stream → complete() resolves BOM, no events":
    makeParser(events, p)
    p.feed("\xEF\xBB\xBF")
    p.complete()
    check events.len == 0

  test "complete() clears parsing buffers":
    makeParser(events, p)
    p.feed("data: partial")
    p.complete()
    check events.len == 0
    # Feed a new complete event after complete — nothing from before leaks.
    p.feed("data: fresh\n\n")
    p.complete()
    # Won't work because bomPending is not re-enabled. This tests buffer clearing only.
    # After complete(), lineBuf/dataBuffer/eventTypeBuffer are cleared.
    # But bomPending is NOT reset (that's reset()'s job), so feeding after complete()
    # without reset() means BOM detection is already resolved. The new feed should work.
    check events.len == 1
    check events[0].data == "fresh"

  test "complete() is idempotent":
    makeParser(events, p)
    p.feed("data: x\n")
    p.complete()
    p.complete()
    check events.len == 0

# ---------------------------------------------------------------------------
# 11. Incremental / Chunked Feeding
# ---------------------------------------------------------------------------

suite "Incremental / Chunked Feeding":

  const testStream = "event: ping\nid: 7\ndata: hello world\ndata: second line\n\n" &
                     "data: next\n\n"

  test "Byte-at-a-time feeding produces identical results to whole-stream feeding":
    let whole = collectEvents(testStream)
    let bytewise = collectEvents(testStream, chunked = true)
    check whole == bytewise

  test "Arbitrary chunk boundaries: mid-field-name, mid-value, mid-line-ending, mid-BOM":
    let events = collectEventsMultiFeed(
      ["\xEF\xBB", "\xBFda", "ta: he", "llo\r", "\ndata: wor", "ld\n\n"])
    check events.len == 1
    check events[0].data == "hello\nworld"

  test "Empty chunks (feed \"\") are no-ops":
    makeParser(events, p)
    p.feed("")
    p.feed("data: x\n")
    p.feed("")
    p.feed("")
    p.feed("\n")
    p.feed("")
    p.complete()
    check events.len == 1
    check events[0].data == "x"

  test "Large single chunk (entire stream in one feed() call)":
    let events = collectEvents(testStream)
    check events.len == 2
    check events[0].eventType == "ping"
    check events[0].data == "hello world\nsecond line"
    check events[1].data == "next"

  test "Two-byte chunks":
    var events: seq[SseEvent] = @[]
    var p = initSseParser(
      proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: events.add(e))
    var i = 0
    while i < testStream.len:
      let endIdx = min(i + 2, testStream.len)
      p.feed(testStream[i ..< endIdx])
      i = endIdx
    p.complete()
    let whole = collectEvents(testStream)
    check events == whole

  test "Three-byte chunks":
    var events: seq[SseEvent] = @[]
    var p = initSseParser(
      proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: events.add(e))
    var i = 0
    while i < testStream.len:
      let endIdx = min(i + 3, testStream.len)
      p.feed(testStream[i ..< endIdx])
      i = endIdx
    p.complete()
    let whole = collectEvents(testStream)
    check events == whole

  test "Multi-byte UTF-8 characters split across chunk boundaries":
    # U+1F600 (😀) = \xF0\x9F\x98\x80 — split in the middle
    let events = collectEventsMultiFeed(
      ["data: \xF0\x9F", "\x98\x80\n\n"])
    check events.len == 1
    check events[0].data == "\xF0\x9F\x98\x80"

# ---------------------------------------------------------------------------
# 12. reset() Semantics (§6.3)
# ---------------------------------------------------------------------------

suite "reset() Semantics":

  test "Preserves lastEventId across reset":
    makeParser(events, p)
    p.feed("id: 42\ndata: x\n\n")
    p.complete()
    check p.lastEventId == "42"
    p.reset()
    check p.lastEventId == "42"

  test "Preserves reconnectionTime across reset":
    makeParser(events, p)
    p.feed("retry: 5000\ndata: x\n\n")
    p.complete()
    check p.reconnectionTime == 5000
    p.reset()
    check p.reconnectionTime == 5000

  test "Preserves callbacks across reset":
    makeParser(events, p)
    p.feed("data: before\n\n")
    p.complete()
    check events.len == 1
    p.reset()
    p.feed("data: after\n\n")
    p.complete()
    check events.len == 2
    check events[1].data == "after"

  test "Clears parsing buffers":
    makeParser(events, p)
    p.feed("data: partial")
    p.reset()
    p.feed("data: clean\n\n")
    p.complete()
    check events.len == 1
    check events[0].data == "clean"

  test "Re-enables BOM detection for the new stream":
    makeParser(events, p)
    p.feed("data: first\n\n")
    p.complete()
    p.reset()
    # New stream with its own BOM
    p.feed("\xEF\xBB\xBFdata: second\n\n")
    p.complete()
    check events.len == 2
    check events[1].data == "second"

  test "Full round-trip: reset → feed with BOM → dispatch with preserved lastEventId":
    makeParser(events, p)
    p.feed("id: original\ndata: first\n\n")
    p.complete()
    check events[0].lastEventId == "original"
    p.reset()
    p.feed("\xEF\xBB\xBFdata: reconnected\n\n")
    p.complete()
    check events.len == 2
    check events[1].data == "reconnected"
    check events[1].lastEventId == "original"

# ---------------------------------------------------------------------------
# 13. Callback Safety
# ---------------------------------------------------------------------------

suite "Callback Safety":

  test "nil onEvent → parser does not crash on dispatch":
    var p = initSseParser(nil)
    p.feed("data: hello\n\n")
    p.complete()

  test "nil onComment → parser does not crash on comment lines":
    makeParser(events, p)
    p.onComment = nil
    p.feed(": comment\ndata: x\n\n")
    p.complete()
    check events.len == 1

  test "Callback set after construction works correctly":
    var events: seq[SseEvent] = @[]
    var p = initSseParser(nil)
    p.onEvent = proc (e: SseEvent) {.closure, gcsafe.} =
      {.cast(gcsafe).}: events.add(e)
    p.feed("data: late\n\n")
    p.complete()
    check events.len == 1
    check events[0].data == "late"

  test "Callback replaced between events works correctly":
    var first: seq[SseEvent] = @[]
    var second: seq[SseEvent] = @[]
    var p = initSseParser(
      proc (e: SseEvent) {.closure, gcsafe.} = {.cast(gcsafe).}: first.add(e))
    p.feed("data: one\n\n")
    p.onEvent = proc (e: SseEvent) {.closure, gcsafe.} =
      {.cast(gcsafe).}: second.add(e)
    p.feed("data: two\n\n")
    p.complete()
    check first.len == 1
    check first[0].data == "one"
    check second.len == 1
    check second[0].data == "two"
