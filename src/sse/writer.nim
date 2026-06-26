import ./types

proc validateSingleLine(value, fieldName: string) =
  for c in value:
    if c == '\r' or c == '\n':
      raise newException(SseError, fieldName & " must not contain CR or LF")

proc write*(buf: var string, event: SseEvent) =
  ## Appends the event as a text/event-stream block, including the
  ## trailing blank line. Data containing CR/LF/CRLF is split into
  ## multiple ``data:`` lines automatically.
  if event.eventType.len > 0:
    validateSingleLine(event.eventType, "eventType")
    buf.add "event: "
    buf.add event.eventType
    buf.add '\n'

  if event.id.len > 0:
    validateSingleLine(event.id, "id")
    if '\0' in event.id: raise newException(SseError, "id must not contain NULL")
    buf.add "id: "
    buf.add event.id
    buf.add '\n'

  if event.retry >= 0:
    buf.add "retry: "
    buf.add $event.retry
    buf.add '\n'

  if event.data.len == 0:
    buf.add "data\n"
  else:
    var lineStart = 0
    var i = 0
    while i < event.data.len:
      if event.data[i] == '\r':
        buf.add "data: "
        buf.add event.data[lineStart ..< i]
        buf.add '\n'
        if i + 1 < event.data.len and event.data[i + 1] == '\n':
          i += 2
        else:
          i += 1
        lineStart = i
      elif event.data[i] == '\n':
        buf.add "data: "
        buf.add event.data[lineStart ..< i]
        buf.add '\n'
        i += 1
        lineStart = i
      else:
        i += 1
    buf.add "data: "
    buf.add event.data[lineStart ..< event.data.len]
    buf.add '\n'

  buf.add '\n'

proc serialize*(event: SseEvent): string =
  ## Returns the text/event-stream representation of the event.
  result = ""
  result.write(event)

proc writeRetry*(buf: var string, ms: int) =
  ## Appends a ``retry:`` block that updates the client's reconnection
  ## time without dispatching an event.
  buf.add "retry: "
  buf.add $ms
  buf.add '\n'
  buf.add '\n'

proc serializeRetry*(ms: int): string =
  ## Returns a ``retry:`` block as a string.
  result = ""
  result.writeRetry(ms)

proc writeComment*(buf: var string, text = "") =
  ## Appends a comment block. Multi-line text is split into separate
  ## ``: `` lines. Useful for keep-alive / heartbeat.
  if text.len == 0:
    buf.add ":\n\n"
    return
  var lineStart = 0
  var i = 0
  while i < text.len:
    if text[i] == '\r':
      buf.add ": "
      buf.add text[lineStart ..< i]
      buf.add '\n'
      if i + 1 < text.len and text[i + 1] == '\n':
        i += 2
      else:
        i += 1
      lineStart = i
    elif text[i] == '\n':
      buf.add ": "
      buf.add text[lineStart ..< i]
      buf.add '\n'
      i += 1
      lineStart = i
    else:
      i += 1
  buf.add ": "
  buf.add text[lineStart ..< text.len]
  buf.add '\n'
  buf.add '\n'

proc serializeComment*(text = ""): string =
  ## Returns a comment block as a string.
  result = ""
  result.writeComment(text)
