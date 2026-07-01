## Incremental, pure (no I/O) parser for the Server-Sent Events wire format.
##
## Implements the parsing algorithm defined in the WHATWG HTML Living Standard,
## Section 9.2. The parser is transport-agnostic: callers feed it raw UTF-8
## chunks as they arrive from any source, and the parser emits complete events
## via callbacks.
##
## Key design properties:
## - Handles all three line-ending forms (CR, LF, CR LF) including splits
##   across chunk boundaries.
## - Strips a single leading UTF-8 BOM if present at the start of the stream.
## - Never sets `SseEvent.origin`; that is the client layer's responsibility.

import types

type
  SseParser* = object
    ## Incremental SSE event stream parser.
    ##
    ## Feed it chunks of raw UTF-8 data via `feed`. It will invoke `onEvent`
    ## each time a complete event is dispatched, and `onComment` for each
    ## comment line encountered.
    onEvent*: SseEventHandler
    onComment*: SseCommentHandler
    reconnectionTime*: int ## Reconnection delay in milliseconds, updated by
                           ## `retry` fields. Read by the client layer when
                           ## deciding how long to wait before reconnecting.

    # -- private state --
    dataBuffer: string
    eventTypeBuffer: string
    lastEventIdBuf: string
    lineBuf: string
    hasCr: bool     ## Previous chunk ended with CR; next byte decides if it
                    ## was CR LF (consume LF) or standalone CR (emit line).
    bomBuf: string  ## Accumulates the first 0–3 bytes of the stream for BOM
                    ## detection. Empty string after BOM resolution is complete.
    bomPending: bool ## True while BOM detection is still in progress (we have
                     ## not yet seen 3 bytes to decide).

const
  Bom = "\xEF\xBB\xBF" ## UTF-8 encoding of U+FEFF BYTE ORDER MARK

proc initSseParser*(onEvent: SseEventHandler;
                    reconnectionTime: int = 3000): SseParser =
  ## Create a new parser. `onEvent` is required; `onComment` can be set after
  ## construction if desired.
  result = SseParser(
    onEvent: onEvent,
    reconnectionTime: reconnectionTime,
    bomPending: true,
  )

proc lastEventId*(parser: SseParser): string =
  ## The current last event ID string. The client layer sends this as the
  ## `Last-Event-ID` header on reconnection (only if non-empty).
  parser.lastEventIdBuf

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

proc isAllAsciiDigits(s: string): bool =
  ## True if `s` is non-empty and every character is an ASCII digit.
  if s.len == 0:
    return false
  for ch in s:
    if ch < '0' or ch > '9':
      return false
  true

proc dispatchEvent(parser: var SseParser) =
  ## Spec §5.1 — event dispatch algorithm, invoked when a blank line is seen.
  # Step 1: Update the event source's last event ID string.
  # (Already stored in lastEventIdBuf; this is the value that lastEventId()
  # returns to the client layer.)

  # Step 2: If the data buffer is empty, reset and return without firing.
  if parser.dataBuffer.len == 0:
    parser.eventTypeBuffer = ""
    return

  # Step 3: Strip one trailing LF from the data buffer.
  if parser.dataBuffer[^1] == '\n':
    parser.dataBuffer.setLen(parser.dataBuffer.len - 1)

  # Step 4: Determine event type.
  let eventType =
    if parser.eventTypeBuffer.len > 0: parser.eventTypeBuffer
    else: "message"

  # Steps 4+6: Create the event and deliver it. We combine these because the
  # event object captures copies of the buffer contents (Nim value semantics),
  # so it's safe to reset the buffers afterwards.
  if parser.onEvent != nil:
    let event = SseEvent(
      eventType: eventType,
      data: parser.dataBuffer,
      lastEventId: parser.lastEventIdBuf,
      origin: "", # Client layer fills this in.
    )
    parser.onEvent(event)

  # Step 5: Reset data buffer and event type buffer. Do NOT reset lastEventIdBuf.
  parser.dataBuffer = ""
  parser.eventTypeBuffer = ""

proc processField(parser: var SseParser; field: string; value: string) =
  ## Spec §4 — interpret a single field/value pair.
  ## Field names are case-sensitive; only the four recognized names have effect.
  if field == "event":
    parser.eventTypeBuffer = value

  elif field == "data":
    parser.dataBuffer.add(value)
    parser.dataBuffer.add('\n')

  elif field == "id":
    # Reject if value contains U+0000 NULL (prevents ID injection).
    if '\0' notin value:
      parser.lastEventIdBuf = value

  elif field == "retry":
    if isAllAsciiDigits(value):
      var t: int = 0
      for ch in value:
        let digit = ord(ch) - ord('0')
        # Saturate at high instead of wrapping on overflow.
        if t > (high(int) - digit) div 10:
          t = high(int)
          break
        t = t * 10 + digit
      parser.reconnectionTime = t

  # All other field names are silently ignored.

proc processLine(parser: var SseParser; line: string) =
  ## Spec §3.4 — process a single complete line (line ending already removed).

  # Rule 1: Empty line → dispatch event.
  if line.len == 0:
    parser.dispatchEvent()
    return

  # Rule 2: Starts with ':' → comment.
  if line[0] == ':':
    if parser.onComment != nil:
      # Pass everything after the colon (no space stripping for comments).
      parser.onComment(line[1 .. ^1])
    return

  # Rule 3: Line contains ':' → field with value.
  let colonPos = line.find(':')
  if colonPos >= 0:
    let field = line[0 ..< colonPos]
    var value: string
    let afterColon = colonPos + 1
    if afterColon < line.len and line[afterColon] == ' ':
      # Strip exactly one leading space.
      value = line[afterColon + 1 .. ^1]
    else:
      value = line[afterColon .. ^1]
    parser.processField(field, value)
    return

  # Rule 4: No colon → entire line is field name, value is empty string.
  parser.processField(line, "")

proc scanLines(parser: var SseParser; data: string; start: int) =
  ## Core line-scanning loop. Splits `data[start ..< data.len]` into lines
  ## using CR / LF / CR LF endings, accumulating partial lines in `lineBuf`
  ## and dispatching complete lines via `processLine`.
  var pos = start

  # -- Handle pending CR from a previous call --
  if parser.hasCr:
    parser.hasCr = false
    if pos < data.len and data[pos] == '\n':
      # CR was followed by LF across boundary → consume the LF.
      inc pos
    # Either way, the CR terminated the previous line.
    let line = parser.lineBuf
    parser.lineBuf = ""
    parser.processLine(line)

  let dataLen = data.len
  while pos < dataLen:
    let ch = data[pos]

    if ch == '\n':
      let line = parser.lineBuf
      parser.lineBuf = ""
      parser.processLine(line)
      inc pos

    elif ch == '\r':
      let nextPos = pos + 1
      if nextPos < dataLen:
        if data[nextPos] == '\n':
          # CR LF — consume both.
          let line = parser.lineBuf
          parser.lineBuf = ""
          parser.processLine(line)
          pos = nextPos + 1
        else:
          # Standalone CR.
          let line = parser.lineBuf
          parser.lineBuf = ""
          parser.processLine(line)
          inc pos
      else:
        # CR at end of data — defer until next call.
        parser.hasCr = true
        inc pos

    else:
      parser.lineBuf.add(ch)
      inc pos

proc resolveBom(parser: var SseParser) =
  ## Called once we have >= 3 bytes in `bomBuf` (or at end-of-stream).
  ## Strips the BOM if present and feeds any remaining buffered bytes
  ## through `scanLines` so that line endings are correctly detected.
  parser.bomPending = false
  var start = 0
  if parser.bomBuf.len >= 3 and parser.bomBuf[0 ..< 3] == Bom:
    start = 3
  let remaining = parser.bomBuf.substr(start)
  parser.bomBuf = ""
  if remaining.len > 0:
    parser.scanLines(remaining, 0)

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc feed*(parser: var SseParser; chunk: string) =
  ## Feed a chunk of raw UTF-8 data into the parser.
  ##
  ## May invoke `onEvent` and/or `onComment` zero or more times, synchronously,
  ## before returning. Handles partial lines that span across multiple `feed`
  ## calls, and correctly resolves CR/LF/CRLF line endings even when split
  ## across chunk boundaries.
  if chunk.len == 0:
    return

  # -- BOM resolution (first bytes of the stream) --
  if parser.bomPending:
    let need = 3 - parser.bomBuf.len  # bytes still needed to decide
    let available = chunk.len
    if available < need:
      # Still not enough bytes to decide. Buffer and return.
      parser.bomBuf.add(chunk)
      return
    # We now have (or will have) enough bytes. Take what we need, resolve,
    # then scan the remainder of the chunk through the normal path.
    parser.bomBuf.add(chunk[0 ..< need])
    parser.resolveBom()
    parser.scanLines(chunk, need)
    return

  parser.scanLines(chunk, 0)

proc complete*(parser: var SseParser) =
  ## Signal that the stream has ended (connection closed).
  ##
  ## Per spec §3.5, any pending data (an incomplete event not followed by a
  ## blank line) is discarded. If BOM detection was still pending, it is
  ## resolved first. If `hasCr` is set, the pending line is processed
  ## (the CR was a valid line ending), but the resulting incomplete event
  ## block is still discarded since no final blank line followed.

  # Flush any bytes stuck in the BOM buffer.
  if parser.bomPending:
    parser.resolveBom()

  if parser.hasCr:
    parser.hasCr = false
    let line = parser.lineBuf
    parser.lineBuf = ""
    parser.processLine(line)

  # Discard any in-progress event that was never terminated by a blank line.
  parser.dataBuffer = ""
  parser.eventTypeBuffer = ""
  parser.lineBuf = ""

proc reset*(parser: var SseParser) =
  ## Reset parsing state for a new stream (e.g. after reconnection).
  ##
  ## Preserves `lastEventId`, `reconnectionTime`, and all callbacks — these
  ## carry over across reconnections per spec §6.3. Clears all parsing buffers
  ## so the parser is ready to consume a fresh byte stream.
  parser.dataBuffer = ""
  parser.eventTypeBuffer = ""
  parser.lineBuf = ""
  parser.bomBuf = ""
  parser.hasCr = false
  parser.bomPending = true # New stream may have its own BOM.
