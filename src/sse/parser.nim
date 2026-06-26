import ./types

type
  BomState = enum
    bsNeedFirst, bsNeedSecond, bsNeedThird, bsDone

  SseParser* = object
    config: SseParserConfig
    dataBuf: string
    eventTypeBuf: string
    lastEventIdBuf: string
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
    retryValue: -1,
    lineBuf: "",
    lastCharWasCr: false,
    bomState: bsNeedFirst
  )

proc reset*(parser: var SseParser) =
  parser.dataBuf.setLen(0)
  parser.eventTypeBuf.setLen(0)
  parser.lastEventIdBuf.setLen(0)
  parser.retryValue = -1
  parser.lineBuf.setLen(0)
  parser.lastCharWasCr = false
  parser.bomState = bsNeedFirst

proc processField(parser: var SseParser, line: openArray[char]) =
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
      val = val * 10 + (ord(c) - ord('0'))
    parser.retryValue = val
  else:
    discard

proc dispatchEvent(parser: var SseParser, onEvent: proc(event: SseEvent)) =
  if parser.dataBuf.len == 0:
    parser.eventTypeBuf.setLen(0)
    parser.retryValue = -1
    return

  if parser.dataBuf[^1] == '\n':
    parser.dataBuf.setLen(parser.dataBuf.len - 1)

  let event = SseEvent(
    data: parser.dataBuf,
    eventType: parser.eventTypeBuf,
    id: parser.lastEventIdBuf,
    retry: parser.retryValue
  )
  onEvent(event)

  parser.dataBuf.setLen(0)
  parser.eventTypeBuf.setLen(0)
  parser.retryValue = -1

proc processLine(parser: var SseParser, onEvent: proc(event: SseEvent)) =
  if parser.lineBuf.len == 0:
    parser.dispatchEvent(onEvent)
    return

  if parser.lineBuf[0] == ':':
    parser.lineBuf.setLen(0)
    return

  parser.processField(parser.lineBuf)
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
           onEvent: proc(event: SseEvent)) =
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
      parser.processLine(onEvent)
      if i == chunk.len - 1:
        parser.lastCharWasCr = true
      else:
        if chunk[i + 1] == '\n':
          inc i
      inc i
    elif c == '\n':
      parser.processLine(onEvent)
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
