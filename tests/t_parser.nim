import std/unittest
import sse/[types, parser]

suite "parser - spec examples §9.2.6":
  test "multi-line data (YHOO example)":
    var p = initSseParser()
    let events = p.push("data: YHOO\ndata: +2\ndata: 10\n\n")
    check events.len == 1
    check events[0].data == "YHOO\n+2\n10"

  test "data with id":
    var p = initSseParser()
    let events = p.push("data: first event\nid: 1\n\n")
    check events.len == 1
    check events[0].data == "first event"
    check events[0].id == "1"

  test "data no space after colon, id set to empty":
    var p = initSseParser()
    let events = p.push("id: prev\ndata:second event\nid\n\n")
    check events.len == 1
    check events[0].data == "second event"
    check events[0].id == ""

  test "data with two leading spaces (one stripped)":
    var p = initSseParser()
    let events = p.push("data:  third event\n\n")
    check events.len == 1
    check events[0].data == " third event"

  test "empty data fields":
    var p = initSseParser()
    let events = p.push("data\n\ndata\ndata\n\ndata:\n")
    check events.len == 2
    check events[0].data == ""
    check events[1].data == "\n"

  test "space stripping after colon":
    var p = initSseParser()
    let events = p.push("data:test\n\ndata: test\n\n")
    check events.len == 2
    check events[0].data == "test"
    check events[1].data == "test"

  test "comment then empty line produces no event":
    var p = initSseParser()
    let events = p.push(": comment\n\n")
    check events.len == 0

suite "parser - line endings":
  test "LF line endings":
    var p = initSseParser()
    let events = p.push("data: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "CR line endings":
    var p = initSseParser()
    let events = p.push("data: hello\r\r")
    check events.len == 1
    check events[0].data == "hello"

  test "CRLF line endings":
    var p = initSseParser()
    let events = p.push("data: hello\r\n\r\n")
    check events.len == 1
    check events[0].data == "hello"

  test "mixed line endings":
    var p = initSseParser()
    let events = p.push("data: a\rdata: b\ndata: c\r\n\r\n")
    check events.len == 1
    check events[0].data == "a\nb\nc"

  test "CR at chunk boundary followed by LF":
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("data: hello\r", proc(e: SseEvent) = allEvents.add e)
    p.push("\n\r\n", proc(e: SseEvent) = allEvents.add e)
    check allEvents.len == 1
    check allEvents[0].data == "hello"

  test "CR at chunk boundary NOT followed by LF":
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("data: hello\r", proc(e: SseEvent) = allEvents.add e)
    p.push("\r", proc(e: SseEvent) = allEvents.add e)
    check allEvents.len == 1
    check allEvents[0].data == "hello"

suite "parser - BOM handling":
  test "UTF-8 BOM at start is stripped":
    var p = initSseParser()
    let events = p.push("\xEF\xBB\xBFdata: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "BOM mid-stream is NOT stripped":
    var p = initSseParser()
    let events = p.push("data: \xEF\xBB\xBFhello\n\n")
    check events.len == 1
    check events[0].data == "\xEF\xBB\xBFhello"

  test "no BOM present":
    var p = initSseParser()
    let events = p.push("data: test\n\n")
    check events.len == 1
    check events[0].data == "test"

  test "BOM split across chunks (1+2 bytes)":
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("\xEF", proc(e: SseEvent) = allEvents.add e)
    p.push("\xBB\xBFdata: hello\n\n", proc(e: SseEvent) = allEvents.add e)
    check allEvents.len == 1
    check allEvents[0].data == "hello"

  test "BOM split across chunks (2+1 bytes)":
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("\xEF\xBB", proc(e: SseEvent) = allEvents.add e)
    p.push("\xBFdata: hello\n\n", proc(e: SseEvent) = allEvents.add e)
    check allEvents.len == 1
    check allEvents[0].data == "hello"

  test "BOM split across 3 single-byte chunks":
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("\xEF", proc(e: SseEvent) = allEvents.add e)
    p.push("\xBB", proc(e: SseEvent) = allEvents.add e)
    p.push("\xBFdata: hello\n\n", proc(e: SseEvent) = allEvents.add e)
    check allEvents.len == 1
    check allEvents[0].data == "hello"

  test "one-byte BOM prefix mismatch flushes byte into stream":
    # \xEF consumed speculatively, then \n mismatches \xBB.
    # \xEF is flushed to lineBuf (unknown field, ignored), \n ends that line.
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("\xEF", proc(e: SseEvent) = allEvents.add e)
    p.push("\ndata: hello\n\n", proc(e: SseEvent) = allEvents.add e)
    check allEvents.len == 1
    check allEvents[0].data == "hello"

  test "two-byte BOM prefix mismatch flushes both bytes":
    # \xEF\xBB consumed speculatively, then 'd' mismatches \xBF.
    # Both bytes flushed to lineBuf, becoming part of a garbled field name.
    var p = initSseParser()
    var allEvents: seq[SseEvent] = @[]
    p.push("\xEF\xBB", proc(e: SseEvent) = allEvents.add e)
    p.push("data: hello\n\ndata: world\n\n", proc(e: SseEvent) = allEvents.add e)
    # First line is "\xEF\xBBdata: hello" — colon splits it into unknown field, ignored.
    # Second block dispatches normally.
    check allEvents.len == 1
    check allEvents[0].data == "world"

suite "parser - event type field":
  test "event type is set on dispatched event":
    var p = initSseParser()
    let events = p.push("event: custom\ndata: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"
    check events[0].eventType == "custom"

  test "event type resets between events":
    var p = initSseParser()
    let events = p.push("event: ping\ndata: a\n\ndata: b\n\n")
    check events.len == 2
    check events[0].eventType == "ping"
    check events[1].eventType == ""

  test "event type with no data does not dispatch":
    var p = initSseParser()
    let events = p.push("event: foo\n\ndata: x\n\n")
    check events.len == 1
    check events[0].data == "x"
    check events[0].eventType == ""

suite "parser - id field":
  test "id with NULL is ignored":
    var p = initSseParser()
    let events = p.push("id: prev\n\nid: abc\x00def\ndata: x\n\n")
    check events.len == 1
    check events[0].id == "prev"

  test "id persists through empty dispatch":
    var p = initSseParser()
    let events = p.push("id: 1\n\ndata: x\n\n")
    check events.len == 1
    check events[0].data == "x"
    check events[0].id == "1"

  test "id persists across multiple dispatched events":
    var p = initSseParser()
    let events = p.push("id: 42\ndata: a\n\ndata: b\n\n")
    check events.len == 2
    check events[0].id == "42"
    check events[1].id == "42"

  test "id can be reset to empty":
    var p = initSseParser()
    let events = p.push("id: 1\ndata: a\n\nid\ndata: b\n\n")
    check events.len == 2
    check events[0].id == "1"
    check events[1].id == ""

suite "parser - retry field":
  test "valid retry value":
    var p = initSseParser()
    let events = p.push("retry: 3000\ndata: x\n\n")
    check events.len == 1
    check events[0].retry == 3000

  test "retry with decimal is ignored":
    var p = initSseParser()
    let events = p.push("retry: 3.0\ndata: x\n\n")
    check events.len == 1
    check events[0].retry == -1

  test "retry with letters is ignored":
    var p = initSseParser()
    let events = p.push("retry: abc\ndata: x\n\n")
    check events.len == 1
    check events[0].retry == -1

  test "empty retry is ignored":
    var p = initSseParser()
    let events = p.push("retry: \ndata: x\n\n")
    check events.len == 1
    check events[0].retry == -1

  test "retry resets after dispatch":
    var p = initSseParser()
    let events = p.push("retry: 5000\ndata: a\n\ndata: b\n\n")
    check events.len == 2
    check events[0].retry == 5000
    check events[1].retry == -1

  test "retry zero is valid":
    var p = initSseParser()
    let events = p.push("retry: 0\ndata: x\n\n")
    check events.len == 1
    check events[0].retry == 0

  test "retry with leading zeros":
    var p = initSseParser()
    let events = p.push("retry: 042\ndata: x\n\n")
    check events.len == 1
    check events[0].retry == 42

  test "retry does not leak across empty dispatch":
    var p = initSseParser()
    let events = p.push("retry: 3000\n\ndata: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"
    check events[0].retry == -1

suite "parser - edge cases":
  test "unknown fields are ignored":
    var p = initSseParser()
    let events = p.push("foo: bar\ndata: x\n\n")
    check events.len == 1
    check events[0].data == "x"

  test "field with no colon":
    var p = initSseParser()
    let events = p.push("data\n\n")
    check events.len == 1
    check events[0].data == ""

  test "empty data dispatch skipped when event type set":
    var p = initSseParser()
    let events = p.push("event: foo\n\n")
    check events.len == 0

  test "comment mid-event is ignored":
    var p = initSseParser()
    let events = p.push("data: hello\n: a comment\ndata: world\n\n")
    check events.len == 1
    check events[0].data == "hello\nworld"

  test "bare colon is a valid comment":
    var p = initSseParser()
    let events = p.push(":\ndata: x\n\n")
    check events.len == 1
    check events[0].data == "x"

  test "field value with multiple colons preserved":
    var p = initSseParser()
    let events = p.push("data: hello: world: foo\n\n")
    check events.len == 1
    check events[0].data == "hello: world: foo"

  test "empty chunk is a no-op":
    var p = initSseParser()
    discard p.push("")
    let events = p.push("data: hello\n\n")
    check events.len == 1
    check events[0].data == "hello"

  test "end of stream discards incomplete event":
    var p = initSseParser()
    let events = p.push("data: incomplete")
    check events.len == 0

  test "incremental feeding byte-by-byte":
    var p1 = initSseParser()
    let bulk = p1.push("data: hello\nid: 42\n\n")

    var p2 = initSseParser()
    let input = "data: hello\nid: 42\n\n"
    var byteEvents: seq[SseEvent] = @[]
    for c in input:
      p2.push($c, proc(e: SseEvent) = byteEvents.add e)

    check byteEvents.len == bulk.len
    check byteEvents[0].data == bulk[0].data
    check byteEvents[0].id == bulk[0].id

  test "multiple events in one chunk":
    var p = initSseParser()
    let events = p.push("data: one\n\ndata: two\n\ndata: three\n\n")
    check events.len == 3
    check events[0].data == "one"
    check events[1].data == "two"
    check events[2].data == "three"

suite "parser - security limits":
  test "line exceeding maxLineLen raises SseLimitError":
    var cfg = initSseParserConfig()
    cfg.maxLineLen = 10
    var p = initSseParser(cfg)
    expect SseLimitError:
      discard p.push("data: this line is way too long\n\n")

  test "event data exceeding maxEventSize raises SseLimitError":
    var cfg = initSseParserConfig()
    cfg.maxEventSize = 20
    var p = initSseParser(cfg)
    expect SseLimitError:
      discard p.push("data: 12345678901\ndata: 12345678901\n\n")

suite "parser - reset":
  test "reset clears state and allows reuse":
    var p = initSseParser()
    discard p.push("id: 42\ndata: first\n\n")
    p.reset()

    let events = p.push("data: second\n\n")
    check events.len == 1
    check events[0].data == "second"
    check events[0].id == ""
    check events[0].retry == -1

  test "reset discards partial line buffer":
    var p = initSseParser()
    discard p.push("data: incom")
    p.reset()

    let events = p.push("data: fresh\n\n")
    check events.len == 1
    check events[0].data == "fresh"
