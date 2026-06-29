import std/[unittest, strutils]
import sse/[types, parser, writer]

suite "writer - serialize":
  test "single-line data":
    let event = SseEvent(data: "hello", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: hello\n\n"

  test "multi-line data splits into multiple data lines":
    let event = SseEvent(data: "line1\nline2\nline3", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: line1\ndata: line2\ndata: line3\n\n"

  test "all fields set":
    let event = SseEvent(data: "payload", eventType: "custom", id: "42", retry: 3000)
    check serialize(event) == "event: custom\nid: 42\nretry: 3000\ndata: payload\n\n"

  test "data only (minimal event)":
    let event = SseEvent(data: "test", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: test\n\n"

  test "empty data emits bare data field":
    let event = SseEvent(data: "", eventType: "", id: "", retry: -1)
    check serialize(event) == "data\n\n"

  test "retry zero is emitted":
    let event = SseEvent(data: "x", eventType: "", id: "", retry: 0)
    check serialize(event) == "retry: 0\ndata: x\n\n"

  test "negative retry is not emitted":
    let event = SseEvent(data: "x", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: x\n\n"

  test "empty eventType is not emitted":
    let event = SseEvent(data: "x", eventType: "", id: "1", retry: -1)
    check serialize(event) == "id: 1\ndata: x\n\n"

  test "empty id is not emitted":
    let event = SseEvent(data: "x", eventType: "ping", id: "", retry: -1)
    check serialize(event) == "event: ping\ndata: x\n\n"

  test "data ending with newline":
    let event = SseEvent(data: "hello\n", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: hello\ndata: \n\n"

  test "data that is just a newline":
    let event = SseEvent(data: "\n", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: \ndata: \n\n"

  test "data with standalone CR splits correctly":
    let event = SseEvent(data: "line1\rline2", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: line1\ndata: line2\n\n"

  test "data with CRLF splits correctly":
    let event = SseEvent(data: "line1\r\nline2", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: line1\ndata: line2\n\n"

  test "data with mixed line endings":
    let event = SseEvent(data: "a\rb\nc\r\nd", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: a\ndata: b\ndata: c\ndata: d\n\n"

  test "data with trailing CR":
    let event = SseEvent(data: "hello\r", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: hello\ndata: \n\n"

  test "data with trailing CRLF":
    let event = SseEvent(data: "hello\r\n", eventType: "", id: "", retry: -1)
    check serialize(event) == "data: hello\ndata: \n\n"

  test "field order: event, id, retry, data":
    let event = SseEvent(data: "d", eventType: "e", id: "i", retry: 1)
    let s = serialize(event)
    let eventPos = s.find("event:")
    let idPos = s.find("id:")
    let retryPos = s.find("retry:")
    let dataPos = s.find("data:")
    check eventPos < idPos
    check idPos < retryPos
    check retryPos < dataPos

suite "writer - field validation":
  test "id with LF raises SseError":
    expect SseError:
      discard serialize(SseEvent(data: "x", eventType: "", id: "abc\ndef", retry: -1))

  test "id with CR raises SseError":
    expect SseError:
      discard serialize(SseEvent(data: "x", eventType: "", id: "abc\rdef", retry: -1))

  test "id with NULL raises SseError":
    expect SseError:
      discard serialize(SseEvent(data: "x", eventType: "", id: "abc\x00def", retry: -1))

  test "eventType with LF raises SseError":
    expect SseError:
      discard serialize(SseEvent(data: "x", eventType: "foo\nbar", id: "", retry: -1))

  test "eventType with CR raises SseError":
    expect SseError:
      discard serialize(SseEvent(data: "x", eventType: "foo\rbar", id: "", retry: -1))

  test "valid id and eventType pass validation":
    let event = SseEvent(data: "x", eventType: "custom", id: "42", retry: -1)
    let s = serialize(event)
    check s == "event: custom\nid: 42\ndata: x\n\n"

  test "empty id and eventType skip validation":
    let event = SseEvent(data: "x", eventType: "", id: "", retry: -1)
    let s = serialize(event)
    check s == "data: x\n\n"

suite "writer - write to buffer":
  test "appends to existing buffer":
    var buf = "prefix"
    buf.write(SseEvent(data: "test", eventType: "", id: "", retry: -1))
    check buf == "prefixdata: test\n\n"

  test "multiple events appended sequentially":
    var buf = ""
    buf.write(SseEvent(data: "one", eventType: "", id: "", retry: -1))
    buf.write(SseEvent(data: "two", eventType: "", id: "", retry: -1))
    check buf == "data: one\n\ndata: two\n\n"

suite "writer - retry (metadata-only block)":
  test "serializeRetry produces non-dispatching block":
    check serializeRetry(5000) == "retry: 5000\n\n"

  test "serializeRetry zero":
    check serializeRetry(0) == "retry: 0\n\n"

  test "writeRetry appends to buffer":
    var buf = "prefix"
    buf.writeRetry(3000)
    check buf == "prefixretry: 3000\n\n"

  test "retry block does not dispatch an event when parsed":
    var p = initSseParser()
    let events = p.push(serializeRetry(5000))
    check events.len == 0

  test "retry block followed by event preserves retry semantics":
    var p = initSseParser()
    var buf = ""
    buf.writeRetry(5000)
    buf.write(SseEvent(data: "hello", eventType: "", id: "", retry: -1))
    let events = p.push(buf)
    check events.len == 1
    check events[0].data == "hello"
    check events[0].retry == -1  # retry was consumed by the silent block

suite "writer - comments":
  test "empty comment (keepalive)":
    check serializeComment() == ":\n\n"

  test "comment with text":
    check serializeComment("keepalive") == ": keepalive\n\n"

  test "multi-line comment":
    check serializeComment("line1\nline2") == ": line1\n: line2\n\n"

  test "comment with CR splits correctly":
    check serializeComment("line1\rline2") == ": line1\n: line2\n\n"

  test "comment with CRLF splits correctly":
    check serializeComment("line1\r\nline2") == ": line1\n: line2\n\n"

  test "writeComment appends to buffer":
    var buf = "prefix"
    buf.writeComment("ping")
    check buf == "prefix: ping\n\n"

  test "writeComment empty appends to buffer":
    var buf = "data: x\n\n"
    buf.writeComment()
    check buf == "data: x\n\n:\n\n"

suite "writer - roundtrip (parse ∘ serialize = id)":
  test "simple data":
    let original = SseEvent(data: "hello world", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == original.data
    check parsed[0].eventType == "message"
    check parsed[0].id == original.id
    check parsed[0].retry == original.retry

  test "multi-line data":
    let original = SseEvent(data: "line1\nline2\nline3", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == original.data
    check parsed[0].eventType == "message"
    check parsed[0].id == original.id
    check parsed[0].retry == original.retry

  test "all fields":
    let original = SseEvent(data: "payload", eventType: "update", id: "7", retry: 5000)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0] == original

  test "empty data":
    let original = SseEvent(data: "", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == original.data
    check parsed[0].eventType == "message"
    check parsed[0].id == original.id
    check parsed[0].retry == original.retry

  test "event type only (no id, no retry)":
    let original = SseEvent(data: "msg", eventType: "chat", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0] == original

  test "retry zero roundtrips":
    let original = SseEvent(data: "x", eventType: "", id: "", retry: 0)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == original.data
    check parsed[0].eventType == "message"
    check parsed[0].id == original.id
    check parsed[0].retry == original.retry

  test "data with trailing newline":
    let original = SseEvent(data: "hello\n", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == original.data
    check parsed[0].eventType == "message"
    check parsed[0].id == original.id
    check parsed[0].retry == original.retry

  test "multiple events in sequence":
    let events = @[
      SseEvent(data: "first", eventType: "a", id: "1", retry: 1000),
      SseEvent(data: "second\nwith newline", eventType: "b", id: "2", retry: -1),
      SseEvent(data: "third", eventType: "c", id: "3", retry: 0),
    ]
    var buf = ""
    for e in events:
      buf.write(e)
    var p = initSseParser()
    let parsed = p.push(buf)
    check parsed.len == 3
    for i in 0 ..< events.len:
      check parsed[i].data == events[i].data
      check parsed[i].eventType == events[i].eventType
      check parsed[i].id == events[i].id
      check parsed[i].retry == events[i].retry

  test "data with CR normalizes to LF on roundtrip":
    let original = SseEvent(data: "line1\rline2", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == "line1\nline2"
    check parsed[0].eventType == "message"

  test "data with CRLF normalizes to LF on roundtrip":
    let original = SseEvent(data: "line1\r\nline2", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == "line1\nline2"
    check parsed[0].eventType == "message"

  test "data with mixed line endings normalizes on roundtrip":
    let original = SseEvent(data: "a\rb\nc\r\nd", eventType: "", id: "", retry: -1)
    var p = initSseParser()
    let parsed = p.push(serialize(original))
    check parsed.len == 1
    check parsed[0].data == "a\nb\nc\nd"
    check parsed[0].eventType == "message"

  test "id persistence across events (spec behavior)":
    let e1 = SseEvent(data: "a", eventType: "", id: "42", retry: -1)
    let e2 = SseEvent(data: "b", eventType: "", id: "", retry: -1)
    var buf = ""
    buf.write(e1)
    buf.write(e2)
    var p = initSseParser()
    let parsed = p.push(buf)
    check parsed.len == 2
    check parsed[0].data == "a"
    check parsed[0].eventType == "message"
    check parsed[0].id == "42"
    check parsed[1].data == "b"
    check parsed[1].eventType == "message"
    check parsed[1].id == "42"
