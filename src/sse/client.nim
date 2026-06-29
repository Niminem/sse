import std/[net, uri, strutils, parseutils, httpcore, monotimes, times, os]
import ./types, ./parser

const
  ReadTimeoutMs = 1000
  MaxRedirects = 20
  RecvBufSize = 4096
  SleepGranularityMs = 200
  ConnectTimeoutMs = 30_000

type
  ChunkedState = enum
    csReadingSize
    csReadingData
    csReadingTrailer
    csDone

  EventSource* = ref object
    url*: Uri
    readyState*: ReadyState
    onOpen*: proc()
    onMessage*: proc(event: SseEvent)
    onError*: proc(msg: string)
    config: EventSourceConfig
    socket: Socket
    parser: SseParser
    lastEventId: string
    reconnectionTime: int
    reconnectAttempts: int
    closed: bool
    isChunked: bool
    chunkedState: ChunkedState
    chunkRemaining: int
    chunkSizeBuf: string

proc newEventSource*(url: string,
                     config = initEventSourceConfig()): EventSource =
  let parsed = parseUri(url)
  result = EventSource(
    url: parsed,
    readyState: rsConnecting,
    config: config,
    parser: initSseParser(config.parserConfig),
    reconnectionTime: config.reconnectionTime,
    closed: false,
    isChunked: false,
    chunkedState: csReadingSize,
    chunkSizeBuf: ""
  )

proc close*(es: EventSource) =
  es.readyState = rsClosed
  es.closed = true

proc isCancelledOrClosed(es: EventSource): bool =
  if es.closed: return true
  if es.config.cancelToken != nil and es.config.cancelToken.isCancelled:
    return true

proc announceConnection(es: EventSource) =
  if es.readyState == rsClosed: return
  es.readyState = rsOpen
  es.reconnectAttempts = 0
  if es.onOpen != nil:
    es.onOpen()

proc failConnection(es: EventSource, msg: string) =
  if es.readyState == rsClosed: return
  es.readyState = rsClosed
  es.closed = true
  if es.onError != nil:
    es.onError(msg)

proc reestablishConnection(es: EventSource): bool =
  if es.readyState == rsClosed: return false
  es.readyState = rsConnecting
  if es.onError != nil:
    es.onError("connection lost, reconnecting")
  if es.isCancelledOrClosed(): return false
  var waited = 0
  while waited < es.reconnectionTime:
    if es.isCancelledOrClosed(): return false
    let sleepTime = min(SleepGranularityMs, es.reconnectionTime - waited)
    sleep(sleepTime)
    waited += sleepTime
  if es.isCancelledOrClosed(): return false
  return true

# --- URL helpers ---

proc getPort(uri: Uri): Port =
  if uri.port.len > 0:
    return Port(parseInt(uri.port))
  if uri.scheme == "https":
    return Port(443)
  return Port(80)

proc buildRequestPath(uri: Uri): string =
  result = uri.path
  if result.len == 0:
    result = "/"
  if uri.query.len > 0:
    result.add '?'
    result.add uri.query

proc buildHostHeader(uri: Uri): string =
  result = uri.hostname
  if uri.port.len > 0:
    let port = parseInt(uri.port)
    if (uri.scheme == "http" and port != 80) or
       (uri.scheme == "https" and port != 443):
      result.add ':'
      result.add uri.port

# --- Origin comparison ---

proc getOrigin(uri: Uri): string =
  result = uri.scheme.toLowerAscii() & "://" & uri.hostname.toLowerAscii() & ":"
  if uri.port.len > 0:
    result.add uri.port
  elif uri.scheme.toLowerAscii() == "https":
    result.add "443"
  else:
    result.add "80"

# --- HTTP request/response ---

proc sendRequest(es: EventSource, targetUrl: Uri, includeLastEventId: bool) =
  var req = "GET " & buildRequestPath(targetUrl) & " HTTP/1.1\r\n"
  req.add "Host: " & buildHostHeader(targetUrl) & "\r\n"
  req.add "Accept: text/event-stream\r\n"
  req.add "Cache-Control: no-store\r\n"
  req.add "Connection: keep-alive\r\n"
  if includeLastEventId and es.lastEventId.len > 0:
    req.add "Last-Event-ID: " & es.lastEventId & "\r\n"
  req.add "\r\n"
  es.socket.send(req)

proc readResponseHeaders(es: EventSource): (int, HttpHeaders) =
  ## Returns (statusCode, headers).
  var buf = ""
  while true:
    let chunk = es.socket.recv(1, timeout = ConnectTimeoutMs)
    if chunk.len == 0:
      raise newException(OSError,
        "connection closed while reading headers")
    buf.add chunk
    if buf.len >= 4 and buf.endsWith("\r\n\r\n"):
      break
    if buf.len > 16_384:
      raise newException(SseConnectionError, "response headers too large")

  let headerEnd = buf.len - 4
  let headerText = buf[0 ..< headerEnd]

  let lines = headerText.split("\r\n")
  if lines.len == 0:
    raise newException(SseConnectionError, "empty response")

  # Parse status line: "HTTP/1.1 200 OK"
  let statusParts = lines[0].split(' ', maxsplit = 2)
  if statusParts.len < 2:
    raise newException(SseConnectionError,
      "invalid status line: " & lines[0])
  var statusCode: int
  try:
    statusCode = parseInt(statusParts[1])
  except ValueError:
    raise newException(SseConnectionError,
      "invalid status code: " & statusParts[1])

  var headers = newHttpHeaders()
  for i in 1 ..< lines.len:
    let line = lines[i]
    if line.len == 0: continue
    let colonPos = line.find(':')
    if colonPos < 0: continue
    let key = line[0 ..< colonPos].strip()
    let value = line[colonPos + 1 ..< line.len].strip()
    headers.add(key, value)

  return (statusCode, headers)

proc checkContentType(headers: HttpHeaders): bool =
  if not headers.hasKey("Content-Type"):
    return false
  let ct = headers["Content-Type"]
  let essence = ct.split(';')[0].strip().toLowerAscii()
  return essence == "text/event-stream"

proc isRedirect(code: int): bool = return code in [301, 302, 303, 307, 308]

proc isPermanentRedirect(code: int): bool = return code in [301, 308]

proc resolveRedirect(base: Uri, location: string): Uri =
  let loc = parseUri(location)
  if loc.scheme.len > 0:
    return loc
  result = combine(base, loc)

# --- Chunked transfer decoding ---

proc resetChunkedState(es: EventSource) =
  es.isChunked = false
  es.chunkedState = csReadingSize
  es.chunkRemaining = 0
  es.chunkSizeBuf.setLen(0)

proc decodeChunked(es: EventSource, raw: string): string =
  result = ""
  var i = 0
  while i < raw.len:
    case es.chunkedState
    of csReadingSize:
      if raw[i] == '\r':
        inc i
      elif raw[i] == '\n':
        var sizeStr = es.chunkSizeBuf
        # Strip chunk extensions
        let semiPos = sizeStr.find(';')
        if semiPos >= 0:
          sizeStr = sizeStr[0 ..< semiPos]
        sizeStr = sizeStr.strip()
        if sizeStr.len == 0:
          raise newException(SseConnectionError, "empty chunk size")
        var val: int
        let parsed = parseHex(sizeStr, val)
        if parsed == 0:
          raise newException(SseConnectionError,
            "invalid chunk size: " & sizeStr)
        es.chunkRemaining = val
        es.chunkSizeBuf.setLen(0)
        if es.chunkRemaining == 0:
          es.chunkedState = csDone
          return
        es.chunkedState = csReadingData
        inc i
      else:
        es.chunkSizeBuf.add raw[i]
        inc i
    of csReadingData:
      let available = raw.len - i
      let toRead = min(available, es.chunkRemaining)
      result.add raw[i ..< i + toRead]
      i += toRead
      es.chunkRemaining -= toRead
      if es.chunkRemaining == 0:
        es.chunkedState = csReadingTrailer
    of csReadingTrailer:
      if raw[i] == '\r':
        inc i
      elif raw[i] == '\n':
        es.chunkedState = csReadingSize
        inc i
      else:
        inc i
    of csDone:
      return

# --- Socket connection ---

proc openSocket(es: EventSource, targetUrl: Uri) =
  let host = targetUrl.hostname
  let port = getPort(targetUrl)

  if targetUrl.scheme == "https":
    when defined(ssl):
      es.socket = newSocket()
      let ctx = newContext(verifyMode = CVerifyNone)
      ctx.wrapSocket(es.socket)
      es.socket.connect(host, port, timeout = ConnectTimeoutMs)
    else:
      raise newException(SseConnectionError,
        "HTTPS requires compilation with -d:ssl")
  else:
    es.socket = newSocket()
    es.socket.connect(host, port, timeout = ConnectTimeoutMs)

proc closeSocket(es: EventSource) =
  if es.socket != nil:
    try:
      es.socket.close()
    except CatchableError:
      discard
    es.socket = nil

# --- Body streaming ---

proc streamBody(es: EventSource) =
  var lastDataTime = getMonoTime()

  let onEvent = proc(event: SseEvent) =
    if es.closed: return
    if es.onMessage != nil:
      es.onMessage(event)

  let onRetry = proc(ms: int) =
    es.reconnectionTime = clamp(ms,
      es.config.minReconnectTime, es.config.maxReconnectTime)

  while not es.isCancelledOrClosed():
    var raw: string
    try:
      raw = es.socket.recv(RecvBufSize, timeout = ReadTimeoutMs)
    except TimeoutError:
      if es.config.inactivityTimeout > 0:
        let elapsed = (getMonoTime() - lastDataTime).inMilliseconds
        if elapsed >= es.config.inactivityTimeout.int64:
          break
      continue
    except OSError:
      break

    if raw.len == 0:
      break # EOF

    lastDataTime = getMonoTime()

    var body: string
    var streamEnded = false

    if es.isChunked:
      try:
        body = es.decodeChunked(raw)
      except SseConnectionError as e:
        es.failConnection(e.msg)
        return
      if es.chunkedState == csDone:
        streamEnded = true
    else:
      body = raw

    if body.len > 0:
      try:
        es.parser.push(body, onEvent, onRetry)
      except SseLimitError:
        es.failConnection("parser limit exceeded")
        return

    if streamEnded:
      break

# --- Connection attempt (single try, handles redirects) ---

proc doConnect(es: EventSource): bool =
  ## Attempts a single connection (following redirects).
  ## Returns true if the connection was successfully announced.
  ## On fatal failure, calls failConnection and returns false.
  var targetUrl = es.url
  var redirects = 0
  let originalOrigin = getOrigin(es.url)
  var crossedOrigin = false

  while true:
    if es.isCancelledOrClosed(): return false

    es.resetChunkedState()
    es.parser.reset()
    es.parser.lastEventId = es.lastEventId

    try:
      es.openSocket(targetUrl)
    except OSError, TimeoutError:
      es.closeSocket()
      return false
    except SseConnectionError as e:
      es.closeSocket()
      es.failConnection(e.msg)
      return false

    let includeId = not (es.config.stripCrossOriginHeaders and crossedOrigin)
    try:
      es.sendRequest(targetUrl, includeId)
    except OSError:
      es.closeSocket()
      return false

    var statusCode: int
    var headers: HttpHeaders
    try:
      (statusCode, headers) = es.readResponseHeaders()
    except SseConnectionError as e:
      es.closeSocket()
      es.failConnection(e.msg)
      return false
    except OSError:
      es.closeSocket()
      return false
    except TimeoutError:
      es.closeSocket()
      return false

    # Handle redirects
    if isRedirect(statusCode):
      es.closeSocket()
      inc redirects
      if redirects > MaxRedirects:
        es.failConnection("too many redirects")
        return false
      if not headers.hasKey("Location"):
        es.failConnection("redirect without Location header")
        return false
      let newUrl = resolveRedirect(targetUrl, headers["Location"])
      if isPermanentRedirect(statusCode):
        es.url = newUrl
      if getOrigin(newUrl) != originalOrigin:
        crossedOrigin = true
      targetUrl = newUrl
      continue

    # Must be 200 with correct content-type
    if statusCode != 200:
      es.closeSocket()
      es.failConnection("unexpected status code: " & $statusCode)
      return false

    if not checkContentType(headers):
      es.closeSocket()
      es.failConnection("invalid Content-Type")
      return false

    # Check for chunked transfer encoding
    if headers.hasKey("Transfer-Encoding"):
      let te = headers["Transfer-Encoding"].toLowerAscii()
      if "chunked" in te:
        es.isChunked = true

    # Connection established successfully
    es.announceConnection()
    return true

# --- Main public API ---

proc connect*(es: EventSource) =
  ## Blocking. Connects to the server, reads events in a loop, and
  ## auto-reconnects per spec. Returns only when the connection is
  ## closed (via close(), cancel token, or fatal error).
  while true:
    if es.isCancelledOrClosed():
      break

    let connected = es.doConnect()

    if es.isCancelledOrClosed():
      es.closeSocket()
      break

    if not connected:
      # doConnect returns false either because it called failConnection
      # (readyState is now CLOSED) or because of a retriable network error.
      if es.readyState == rsClosed:
        break
      # Retriable — try to reestablish
      es.lastEventId = es.parser.lastEventId
      es.closeSocket()
      inc es.reconnectAttempts
      if es.config.maxReconnectAttempts > 0 and
         es.reconnectAttempts > es.config.maxReconnectAttempts:
        es.failConnection("max reconnect attempts exceeded")
        break
      if not es.reestablishConnection():
        break
      continue

    # Connected — stream body
    es.streamBody()

    # streamBody exited — save state before reconnect
    es.lastEventId = es.parser.lastEventId
    es.closeSocket()

    if es.isCancelledOrClosed():
      break

    # Reestablish connection per spec
    inc es.reconnectAttempts
    if es.config.maxReconnectAttempts > 0 and
       es.reconnectAttempts > es.config.maxReconnectAttempts:
      es.failConnection("max reconnect attempts exceeded")
      break
    if not es.reestablishConnection():
      break
