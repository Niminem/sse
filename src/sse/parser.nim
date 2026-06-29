import ./types

type
  BomState = enum
    bsNeedFirst, bsNeedSecond, bsNeedThird, bsDone

  SseParser* = object
    config: SseParserConfig
    dataBuf: string
    eventTypeBuf: string
    lastEventIdBuf: string
    committedLastEventId: string
    retryValue: int
    lineBuf: string
    lastCharWasCr: bool
    bomState: BomState

proc initSseParser*(config = initSseParserConfig()): SseParser =
  result = SseParser(
    config: config,
    dataBuf: "",
    eventTypeBuf: "",
    lastEventIdBuf: "",
    committedLastEventId: "",
    retryValue: -1,
    lineBuf: "",
    lastCharWasCr: false,
    bomState: bsNeedFirst
  )

proc reset*(parser: var SseParser) =
  parser.dataBuf.setLen(0)
  parser.eventTypeBuf.setLen(0)
  parser.lastEventIdBuf.setLen(0)
  parser.committedLastEventId.setLen(0)
  parser.retryValue = -1
  parser.lineBuf.setLen(0)
  parser.lastCharWasCr = false
  parser.bomState = bsNeedFirst

proc processField(parser: var SseParser, line: openArray[char],
                   onRetry: proc(ms: int)) =
  var colonPos = -1
  for i in 0 ..< line.len:
    if line[i] == ':':
      colonPos = i
      break

  var fieldName = newStringOfCap(32)
  var fieldValue: string

  if colonPos == -1:
    for i in 0 ..< line.len:
      fieldName.add line[i]
    fieldValue = ""
  else:
    for i in 0 ..< colonPos:
      fieldName.add line[i]
    var valueStart = colonPos + 1
    if valueStart < line.len and line[valueStart] == ' ':
      inc valueStart
    fieldValue = newStringOfCap(line.len - valueStart)
    for i in valueStart ..< line.len:
      fieldValue.add line[i]

  case fieldName
  of "data":
    if parser.dataBuf.len + fieldValue.len + 1 > parser.config.maxEventSize:
      raise newException(SseLimitError, "event data exceeds maxEventSize")
    parser.dataBuf.add fieldValue
    parser.dataBuf.add '\n'
  of "event":
    parser.eventTypeBuf = fieldValue
  of "id":
    for c in fieldValue:
      if c == '\0':
        return
    parser.lastEventIdBuf = fieldValue
  of "retry":
    if fieldValue.len == 0:
      return
    for c in fieldValue:
      if c < '0' or c > '9':
        return
    var val = 0
    for c in fieldValue:
      let digit = ord(c) - ord('0')
      if val > (high(int) - digit) div 10:
        return
      val = val * 10 + digit
    parser.retryValue = val
    if onRetry != nil:
      onRetry(val)
  else:
    discard

proc dispatchEvent(parser: var SseParser, onEvent: proc(event: SseEvent)) =
  # §9.2.6 step 1: commit the last event ID unconditionally
  parser.committedLastEventId = parser.lastEventIdBuf

  if parser.dataBuf.len == 0:
    parser.eventTypeBuf.setLen(0)
    parser.retryValue = -1
    return

  if parser.dataBuf[^1] == '\n':
    parser.dataBuf.setLen(parser.dataBuf.len - 1)

  let eventType = if parser.eventTypeBuf.len == 0: "message"
                  else: parser.eventTypeBuf
  let event = SseEvent(
    data: parser.dataBuf,
    eventType: eventType,
    id: parser.lastEventIdBuf,
    retry: parser.retryValue
  )
  onEvent(event)

  parser.dataBuf.setLen(0)
  parser.eventTypeBuf.setLen(0)
  parser.retryValue = -1

proc processLine(parser: var SseParser, onEvent: proc(event: SseEvent),
                  onRetry: proc(ms: int)) =
  if parser.lineBuf.len == 0:
    parser.dispatchEvent(onEvent)
    return

  if parser.lineBuf[0] == ':':
    parser.lineBuf.setLen(0)
    return

  parser.processField(parser.lineBuf, onRetry)
  parser.lineBuf.setLen(0)

proc consumeBom(parser: var SseParser, chunk: openArray[char], pos: var int) =
  const bom = ['\xEF', '\xBB', '\xBF']
  while pos < chunk.len and parser.bomState != bsDone:
    let expected = case parser.bomState
      of bsNeedFirst: bom[0]
      of bsNeedSecond: bom[1]
      of bsNeedThird: bom[2]
      of bsDone: '\x00' # effectively dead code, but needed for case stmt
    if chunk[pos] == expected:
      inc pos
      case parser.bomState
      of bsNeedFirst: parser.bomState = bsNeedSecond
      of bsNeedSecond: parser.bomState = bsNeedThird
      of bsNeedThird: parser.bomState = bsDone
      else: discard
    else:
      # Mismatch — flush already-consumed BOM bytes back as data
      if parser.bomState == bsNeedSecond:
        parser.lineBuf.add bom[0]
      elif parser.bomState == bsNeedThird:
        parser.lineBuf.add bom[0]
        parser.lineBuf.add bom[1]
      parser.bomState = bsDone

proc push*(parser: var SseParser, chunk: openArray[char],
           onEvent: proc(event: SseEvent),
           onRetry: proc(ms: int) = nil) =
  if chunk.len == 0:
    return

  var i = 0

  if parser.bomState != bsDone:
    parser.consumeBom(chunk, i)

  if parser.lastCharWasCr:
    parser.lastCharWasCr = false
    if i < chunk.len and chunk[i] == '\n':
      inc i

  while i < chunk.len:
    let c = chunk[i]
    if c == '\r':
      parser.processLine(onEvent, onRetry)
      if i == chunk.len - 1:
        parser.lastCharWasCr = true
      else:
        if chunk[i + 1] == '\n':
          inc i
      inc i
    elif c == '\n':
      parser.processLine(onEvent, onRetry)
      inc i
    else:
      if parser.lineBuf.len >= parser.config.maxLineLen:
        raise newException(SseLimitError, "line exceeds maxLineLen")
      parser.lineBuf.add c
      inc i

proc push*(parser: var SseParser, chunk: openArray[char]): seq[SseEvent] =
  var events: seq[SseEvent]
  parser.push(chunk, proc(event: SseEvent) =
    events.add event
  )
  return events

proc lastEventId*(parser: SseParser): string =
  parser.committedLastEventId

proc `lastEventId=`*(parser: var SseParser, value: string) =
  parser.lastEventIdBuf = value
  parser.committedLastEventId = value
