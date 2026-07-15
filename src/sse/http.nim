## Client-side HTTP protocol layer for SSE connections.
##
## Pure (no I/O, no sockets) helpers for URL handling, HTTP request/response
## processing, and chunked transfer encoding decoding. Both the async and
## sync client modules import this module and perform actual socket I/O
## themselves.
##
## Uses `std/uri` for RFC 3986 URL parsing and reference resolution.

import std/[uri, strutils, httpcore]

# Re-export so users get `HttpMethod` and its values (HttpGet, HttpPost, ...)
# from `import sse` without a separate `import std/httpcore`.
export httpcore

# ---------------------------------------------------------------------------
# Types & Constants
# ---------------------------------------------------------------------------

type
  SseUrl* = object
    ## Parsed and validated URL for an SSE connection.
    scheme*: string   ## `"http"` or `"https"` (always lowercase).
    host*: string     ## Hostname without port.
    port*: int        ## Effective port (80/443 default, or explicit).
    path*: string     ## Request target: path + query (e.g. `"/events?t=1"`).
    useTls*: bool     ## `true` when scheme is `"https"`.

  HttpResponse* = object
    ## Parsed HTTP response status and headers.
    statusCode*: int
    headers*: seq[(string, string)]  ## Names are lowercased at parse time.

  SseHttpResponseHandler* = proc (resp: HttpResponse) {.closure, gcsafe.}
    ## Callback fired by the clients once per HTTP response received —
    ## including error statuses, each redirect hop, and every reconnection
    ## attempt — after the status line and headers are parsed but before
    ## the response is validated or its body is consumed. Lives here
    ## (rather than in `sse/types`) because it references `HttpResponse`.

  HeaderParser* = object
    ## Incremental HTTP response header accumulator.
    ##
    ## Feed raw socket bytes via `feed` until `isComplete` or `hasFailed`.
    ## Then call `parse` to obtain the structured `HttpResponse`.
    buf: string
    headerEnd: int
    complete: bool
    failed: bool
    failReason: string
    maxSize*: int  ## Maximum header block size in bytes.

  SseConnectResult* = enum
    scrOk           ## 200 + `text/event-stream` content-type.
    scrRedirect     ## 3xx redirect with `Location` header.
    scrFail         ## Bad status or wrong content-type.

  TransferMode* = enum
    tmIdentity       ## No framing; read until connection close.
    tmChunked        ## `Transfer-Encoding: chunked`.
    tmContentLength  ## `Content-Length` present; known body size.

  ChunkState = enum
    csReadSize       ## Accumulating hex chunk-size line.
    csReadSizeLf     ## Consumed `\r` in size line; expecting `\n`.
    csReadData       ## Reading chunk body bytes.
    csReadDataCr     ## Chunk data fully consumed; expecting `\r` or `\n`.
    csReadDataLf     ## Consumed `\r` after data; expecting `\n`.
    csFinished       ## Terminal zero-length chunk processed.

  ChunkedDecoder* = object
    ## Incremental decoder for HTTP chunked transfer encoding.
    ##
    ## Feed raw bytes via `feed`; it returns decoded body bytes with the
    ## chunked framing stripped. Check `isFinished` for end-of-body and
    ## `hasFailed` for protocol errors after each call.
    state: ChunkState
    sizeBuf: string
    remaining: int
    failed: bool
    failReason: string
    maxChunkSize*: int  ## Maximum allowed single-chunk size.

const
  MaxSizeLineLen = 512 ## Hard limit on chunk-size line length (hex digits
                       ## plus optional extensions).

# ---------------------------------------------------------------------------
# URL
# ---------------------------------------------------------------------------

proc parseSseUrl*(rawUrl: string): SseUrl =
  ## Parse and validate a URL for an SSE connection.
  ##
  ## Only `http` and `https` schemes are accepted. Raises `ValueError` if
  ## the URL is missing a scheme, has an unsupported scheme, or has no
  ## hostname. The path defaults to `"/"` when absent. The port defaults
  ## to 80 (http) or 443 (https) when not specified.
  let parsed = parseUri(rawUrl)
  if parsed.scheme.len == 0:
    raise newException(ValueError, "missing scheme in URL: " & rawUrl)
  let scheme = parsed.scheme.toLowerAscii()
  if scheme != "http" and scheme != "https":
    raise newException(ValueError,
      "unsupported scheme '" & parsed.scheme & "'; expected http or https")
  if parsed.hostname.len == 0:
    raise newException(ValueError, "missing hostname in URL: " & rawUrl)
  let useTls = scheme == "https"
  var port: int
  if parsed.port.len == 0:
    port = if useTls: 443 else: 80
  else:
    try:
      port = parseInt(parsed.port)
    except ValueError:
      raise newException(ValueError, "invalid port: " & parsed.port)
    if port < 1 or port > 65535:
      raise newException(ValueError, "port out of range: " & $port)
  var path = parsed.path
  if path.len == 0:
    path = "/"
  if parsed.query.len > 0:
    path.add('?')
    path.add(parsed.query)
  result = SseUrl(scheme: scheme, host: parsed.hostname, port: port,
                  path: path, useTls: useTls)

func isDefaultPort*(url: SseUrl): bool =
  ## True when the port is the default for the scheme (80/443).
  result = (url.useTls and url.port == 443) or
           (not url.useTls and url.port == 80)

func origin*(url: SseUrl): string =
  ## Serialized origin for `SseEvent.origin`.
  ##
  ## Returns `"scheme://host"` when the port is default, or
  ## `"scheme://host:port"` otherwise.
  result = url.scheme & "://" & url.host
  if not url.isDefaultPort:
    result.add(':')
    result.add($url.port)

func hostHeader*(url: SseUrl): string =
  ## Value for the HTTP `Host` request header.
  ##
  ## Includes the port only when it differs from the scheme default.
  result = url.host
  if not url.isDefaultPort:
    result.add(':')
    result.add($url.port)

func `$`*(url: SseUrl): string =
  ## Serialize the URL to its canonical string form.
  result = url.origin
  result.add(url.path)

func toUri*(url: SseUrl): Uri =
  ## Convert to `std/uri.Uri` for redirect resolution via `combine`.
  result.scheme = url.scheme
  result.hostname = url.host
  if not url.isDefaultPort:
    result.port = $url.port
  let qPos = url.path.find('?')
  if qPos >= 0:
    result.path = url.path[0 ..< qPos]
    result.query = url.path[qPos + 1 .. ^1]
  else:
    result.path = url.path

proc resolveRedirect*(baseUrl: SseUrl; location: string): SseUrl =
  ## Resolve a `Location` header value against the current URL.
  ##
  ## Handles absolute URLs, root-relative paths, and path-relative
  ## references via RFC 3986 resolution (`std/uri.combine`). Raises
  ## `ValueError` if the resolved URL is invalid or uses an unsupported
  ## scheme.
  result = parseSseUrl($combine(baseUrl.toUri, parseUri(location)))

# ---------------------------------------------------------------------------
# HTTP Request
# ---------------------------------------------------------------------------

const ReservedRequestHeaders* = [
  ## Header names `buildRequest` emits itself. User-supplied extra headers
  ## must not collide with these: `Host`/`Accept`/`Cache-Control` are fixed
  ## by the EventSource spec, `Last-Event-ID` is managed by the reconnection
  ## logic, and `Content-Length` is computed from the request body.
  "host", "accept", "cache-control", "last-event-id", "content-length",
]

func validateRequest*(httpMethod: HttpMethod;
                      extraHeaders: seq[tuple[key, val: string]];
                      body: string) =
  ## Validate custom request options before any network activity.
  ##
  ## Raises `ValueError` when a header name is empty or reserved (see
  ## `ReservedRequestHeaders`), when a header name or value contains a
  ## CR/LF (which would allow request injection, since headers are spliced
  ## verbatim into the wire request), or when a body is supplied with a
  ## bodiless method (GET/HEAD).
  for (key, val) in extraHeaders:
    if key.len == 0:
      raise newException(ValueError, "empty header name")
    if '\r' in key or '\n' in key:
      raise newException(ValueError,
        "header name contains CR/LF: " & key)
    if '\r' in val or '\n' in val:
      raise newException(ValueError,
        "value of header '" & key & "' contains CR/LF")
    if key.toLowerAscii() in ReservedRequestHeaders:
      raise newException(ValueError,
        "header '" & key & "' is managed by the client and cannot be " &
        "overridden")
  if body.len > 0 and httpMethod in {HttpGet, HttpHead}:
    raise newException(ValueError,
      "request body not allowed with method " & $httpMethod)

func buildRequest*(url: SseUrl; lastEventId = "";
                   httpMethod = HttpGet;
                   extraHeaders: seq[tuple[key, val: string]] = @[];
                   body = ""): string =
  ## Build an HTTP/1.1 request for an SSE endpoint.
  ##
  ## Always includes `Host`, `Accept: text/event-stream`, and
  ## `Cache-Control: no-store` headers. Adds `Last-Event-ID` only when
  ## `lastEventId` is non-empty (spec §7.4: sent only during reconnection,
  ## only if non-empty).
  ##
  ## `httpMethod`, `extraHeaders`, and `body` support non-spec endpoints
  ## that stream SSE in response to a custom request (e.g. LLM APIs that
  ## expect a POST with a JSON payload and auth headers). Extra headers
  ## are emitted after the built-in ones; a non-empty body adds a
  ## `Content-Length` header and appends the payload. Callers are expected
  ## to have run `validateRequest` on these options first — this proc does
  ## not re-validate.
  result = $httpMethod & " " & url.path & " HTTP/1.1\r\n"
  result.add("Host: " & url.hostHeader & "\r\n")
  result.add("Accept: text/event-stream\r\n")
  result.add("Cache-Control: no-store\r\n")
  if lastEventId.len > 0:
    result.add("Last-Event-ID: " & lastEventId & "\r\n")
  for (key, val) in extraHeaders:
    result.add(key & ": " & val & "\r\n")
  if body.len > 0:
    result.add("Content-Length: " & $body.len & "\r\n")
  result.add("\r\n")
  if body.len > 0:
    result.add(body)

# ---------------------------------------------------------------------------
# HTTP Response Header Parsing
# ---------------------------------------------------------------------------

proc initHeaderParser*(maxSize = 65536): HeaderParser =
  ## Create a new header parser.
  ##
  ## `maxSize` caps the header buffer to prevent unbounded allocation from
  ## a malicious or misconfigured server.
  result = HeaderParser(maxSize: maxSize)

func findHeaderEnd(buf: string; start: int): int =
  ## Scan for the blank line that terminates HTTP headers.
  ##
  ## Recognises both `\r\n\r\n` (standard) and `\n\n` (bare-LF) as well
  ## as the mixed `\n\r\n` form. Returns the byte position immediately
  ## after the terminator, or -1 if not yet found.
  var i = start
  while i < buf.len - 1:
    if buf[i] == '\n':
      if buf[i + 1] == '\n':
        return i + 2
      if buf[i + 1] == '\r' and i + 2 < buf.len and buf[i + 2] == '\n':
        return i + 3
    inc i
  return -1

proc feed*(hp: var HeaderParser; data: string): string =
  ## Feed raw socket bytes into the parser.
  ##
  ## Returns unconsumed body bytes when the header terminator is found
  ## within `data` (empty string otherwise). After calling, check
  ## `isComplete` and `hasFailed`.
  if hp.complete or hp.failed or data.len == 0:
    return ""
  let oldLen = hp.buf.len
  hp.buf.add(data)
  let searchStart = max(0, oldLen - 2)
  let endPos = findHeaderEnd(hp.buf, searchStart)
  if endPos >= 0:
    if endPos > hp.maxSize:
      hp.failed = true
      hp.failReason = "response headers exceed " & $hp.maxSize & " bytes"
      return ""
    hp.complete = true
    hp.headerEnd = endPos
    hp.buf.setLen(endPos)
    let bodyOffset = endPos - oldLen
    if bodyOffset < data.len:
      return data[bodyOffset .. ^1]
    return ""
  elif hp.buf.len > hp.maxSize:
    hp.failed = true
    hp.failReason = "response headers exceed " & $hp.maxSize & " bytes"
  return ""

func isComplete*(hp: HeaderParser): bool =
  result = hp.complete

func hasFailed*(hp: HeaderParser): bool =
  result = hp.failed

func failMessage*(hp: HeaderParser): string =
  result = hp.failReason

proc parse*(hp: var HeaderParser): HttpResponse =
  ## Parse the accumulated header block into an `HttpResponse`.
  ##
  ## On malformed input, sets `hasFailed` and `failMessage` instead of
  ## raising. Call only after `isComplete` returns true.
  if hp.failed:
    return result
  if not hp.complete:
    hp.failed = true
    hp.failReason = "parse called before headers are complete"
    return result

  var pos = 0

  # --- Status line: "HTTP/x.y NNN Reason-Phrase" ---
  while pos < hp.buf.len and hp.buf[pos] != '\r' and hp.buf[pos] != '\n':
    inc pos
  let statusLine = hp.buf[0 ..< pos]
  if pos < hp.buf.len and hp.buf[pos] == '\r': inc pos
  if pos < hp.buf.len and hp.buf[pos] == '\n': inc pos

  let sp = statusLine.find(' ')
  if sp < 0:
    hp.failed = true
    hp.failReason = "malformed status line: " & statusLine
    return result
  var codeStart = sp + 1
  while codeStart < statusLine.len and statusLine[codeStart] == ' ':
    inc codeStart
  var codeEnd = codeStart
  while codeEnd < statusLine.len and statusLine[codeEnd] in {'0'..'9'}:
    inc codeEnd
  if codeEnd == codeStart:
    hp.failed = true
    hp.failReason = "missing status code in: " & statusLine
    return result
  try:
    result.statusCode = parseInt(statusLine[codeStart ..< codeEnd])
  except ValueError:
    hp.failed = true
    hp.failReason = "invalid status code in: " & statusLine
    return result

  # --- Headers ---
  while pos < hp.buf.len:
    var lineEnd = pos
    while lineEnd < hp.buf.len and hp.buf[lineEnd] != '\r' and
          hp.buf[lineEnd] != '\n':
      inc lineEnd
    let line = hp.buf[pos ..< lineEnd]
    if lineEnd < hp.buf.len and hp.buf[lineEnd] == '\r': inc lineEnd
    if lineEnd < hp.buf.len and hp.buf[lineEnd] == '\n': inc lineEnd
    pos = lineEnd
    if line.len == 0:
      break
    let colonPos = line.find(':')
    if colonPos > 0:
      let name = line[0 ..< colonPos].strip().toLowerAscii()
      let value = line[colonPos + 1 .. ^1].strip()
      result.headers.add((name, value))

# ---------------------------------------------------------------------------
# Response Helpers
# ---------------------------------------------------------------------------

func getHeader*(resp: HttpResponse; name: string): string =
  ## First value for the named header, or `""` if absent.
  ##
  ## `name` must be lowercase (all header names are lowercased at parse
  ## time).
  for (n, v) in resp.headers:
    if n == name:
      return v
  return ""

func hasHeader*(resp: HttpResponse; name: string): bool =
  ## True if the named header is present. `name` must be lowercase.
  for (n, _) in resp.headers:
    if n == name:
      return true
  return false

# ---------------------------------------------------------------------------
# Response Validation
# ---------------------------------------------------------------------------

func mimeEssence(contentType: string): string =
  ## Extract the MIME type essence (`type/subtype`), lowercased, ignoring
  ## parameters like `charset=utf-8`.
  var s = contentType
  let semi = s.find(';')
  if semi >= 0:
    s.setLen(semi)
  result = s.strip().toLowerAscii()

func isRedirectStatus(code: int): bool {.inline.} =
  result = code == 301 or code == 302 or code == 303 or
           code == 307 or code == 308

func validateResponse*(resp: HttpResponse): SseConnectResult =
  ## Determine the SSE connection outcome from the HTTP response.
  ##
  ## Returns `scrOk` for 200 with a `text/event-stream` content-type
  ## (MIME essence match, case-insensitive, parameters ignored per spec
  ## §8.2). Returns `scrRedirect` for 301/302/303/307/308. Returns
  ## `scrFail` for all other statuses or 200 with wrong content-type.
  if isRedirectStatus(resp.statusCode):
    return scrRedirect
  if resp.statusCode != 200:
    return scrFail
  if mimeEssence(resp.getHeader("content-type")) != "text/event-stream":
    return scrFail
  return scrOk

func isRedirect*(resp: HttpResponse): bool =
  ## True for any recognised redirect status (301/302/303/307/308).
  result = isRedirectStatus(resp.statusCode)

func isPermanentRedirect*(resp: HttpResponse): bool =
  ## True for 301 (Moved Permanently) and 308 (Permanent Redirect).
  ##
  ## The client should update its stored URL so that future reconnections
  ## go directly to the new location.
  result = resp.statusCode == 301 or resp.statusCode == 308

func redirectLocation*(resp: HttpResponse): string =
  ## Extract the `Location` header value for redirect following.
  result = resp.getHeader("location")

# ---------------------------------------------------------------------------
# Transfer Mode
# ---------------------------------------------------------------------------

func detectTransferMode*(resp: HttpResponse): TransferMode =
  ## Determine how the response body is framed.
  let te = resp.getHeader("transfer-encoding").toLowerAscii()
  if "chunked" in te:
    return tmChunked
  if resp.hasHeader("content-length"):
    return tmContentLength
  return tmIdentity

func contentLength*(resp: HttpResponse): int =
  ## Parse the `Content-Length` header. Returns -1 if absent or invalid.
  let val = resp.getHeader("content-length")
  if val.len == 0:
    return -1
  try:
    result = parseInt(val)
  except ValueError:
    result = -1

# ---------------------------------------------------------------------------
# Chunked Transfer Decoding
# ---------------------------------------------------------------------------

proc initChunkedDecoder*(maxChunkSize = 16 * 1024 * 1024): ChunkedDecoder =
  ## Create a new chunked decoder.
  ##
  ## `maxChunkSize` caps the size of any single chunk to prevent unbounded
  ## allocation (default 16 MiB).
  result = ChunkedDecoder(state: csReadSize, maxChunkSize: maxChunkSize)

func isFinished*(dec: ChunkedDecoder): bool =
  result = dec.state == csFinished

func hasFailed*(dec: ChunkedDecoder): bool =
  result = dec.failed

func failMessage*(dec: ChunkedDecoder): string =
  result = dec.failReason

proc parseChunkSize(dec: var ChunkedDecoder): int =
  ## Parse the accumulated hex chunk size from `sizeBuf`. Returns the
  ## parsed size, or -1 on error (sets `failed` and `failReason`).
  var s = dec.sizeBuf
  dec.sizeBuf = ""
  # Strip chunk extensions (`;ext-name=ext-value ...`).
  let semi = s.find(';')
  if semi >= 0:
    s.setLen(semi)
  s = s.strip()
  if s.len == 0:
    dec.failed = true
    dec.failReason = "empty chunk size"
    return -1
  var size = 0
  for ch in s:
    let digit = case ch
      of '0'..'9': ord(ch) - ord('0')
      of 'a'..'f': ord(ch) - ord('a') + 10
      of 'A'..'F': ord(ch) - ord('A') + 10
      else:
        dec.failed = true
        dec.failReason = "invalid hex in chunk size: " & s
        return -1
    if size > (high(int) - digit) div 16:
      dec.failed = true
      dec.failReason = "chunk size overflow"
      return -1
    size = size * 16 + digit
  if size > dec.maxChunkSize:
    dec.failed = true
    dec.failReason = "chunk size " & $size &
                     " exceeds limit of " & $dec.maxChunkSize
    return -1
  return size

proc finishSize(dec: var ChunkedDecoder) =
  ## Parse the buffered chunk size and transition to the appropriate next
  ## state (`csReadData` or `csFinished`).
  let size = dec.parseChunkSize()
  if size < 0:
    return
  if size == 0:
    dec.state = csFinished
  else:
    dec.remaining = size
    dec.state = csReadData

proc feed*(dec: var ChunkedDecoder; data: string): string =
  ## Feed raw chunked-encoded bytes. Returns decoded body bytes (chunked
  ## framing stripped). May return an empty string when in the middle of
  ## chunk framing.
  ##
  ## Check `isFinished` and `hasFailed` after each call.
  if dec.failed or dec.state == csFinished or data.len == 0:
    return ""
  var pos = 0
  let dataLen = data.len
  while pos < dataLen and not dec.failed and dec.state != csFinished:
    case dec.state

    of csReadSize:
      let ch = data[pos]
      if ch == '\r':
        inc pos
        dec.state = csReadSizeLf
      elif ch == '\n':
        inc pos
        dec.finishSize()
      else:
        dec.sizeBuf.add(ch)
        inc pos
        if dec.sizeBuf.len > MaxSizeLineLen:
          dec.failed = true
          dec.failReason = "chunk size line exceeds " &
                           $MaxSizeLineLen & " bytes"

    of csReadSizeLf:
      if data[pos] == '\n':
        inc pos
      dec.finishSize()

    of csReadData:
      let available = dataLen - pos
      let take = min(available, dec.remaining)
      result.add(data[pos ..< pos + take])
      dec.remaining -= take
      pos += take
      if dec.remaining == 0:
        dec.state = csReadDataCr

    of csReadDataCr:
      let ch = data[pos]
      if ch == '\r':
        inc pos
        dec.state = csReadDataLf
      elif ch == '\n':
        inc pos
        dec.state = csReadSize
      else:
        dec.failed = true
        dec.failReason = "expected CRLF after chunk data, got byte " &
                         $ord(ch)

    of csReadDataLf:
      if data[pos] == '\n':
        inc pos
      dec.state = csReadSize

    of csFinished:
      discard
