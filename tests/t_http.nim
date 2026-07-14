import std/[unittest, strutils]
import sse/http

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

proc parseRawResponse(raw: string): HttpResponse =
  ## Feed a complete raw HTTP response header block through the parser and
  ## return the structured result.
  var hp = initHeaderParser()
  discard hp.feed(raw)
  doAssert hp.isComplete, "expected headers to be complete"
  doAssert not hp.hasFailed, "header parser failed: " & hp.failMessage
  hp.parse()

proc decodeAllChunked(raw: string): string =
  ## Decode a complete chunked-encoded body in one shot.
  var dec = initChunkedDecoder()
  result = dec.feed(raw)
  doAssert not dec.hasFailed, "chunked decoder failed: " & dec.failMessage

proc decodeChunkedBytewise(raw: string): string =
  ## Decode a chunked body one byte at a time.
  var dec = initChunkedDecoder()
  for ch in raw:
    result.add(dec.feed($ch))
    if dec.hasFailed or dec.isFinished:
      break

# ---------------------------------------------------------------------------
# 1. URL Parsing — Happy Paths
# ---------------------------------------------------------------------------

suite "parseSseUrl — Happy Paths":

  test "Basic http URL":
    let url = parseSseUrl("http://example.com/events")
    check url.scheme == "http"
    check url.host == "example.com"
    check url.port == 80
    check url.path == "/events"
    check url.useTls == false

  test "Basic https URL":
    let url = parseSseUrl("https://example.com/events")
    check url.scheme == "https"
    check url.host == "example.com"
    check url.port == 443
    check url.path == "/events"
    check url.useTls == true

  test "Explicit port":
    let url = parseSseUrl("http://localhost:8080/stream")
    check url.host == "localhost"
    check url.port == 8080
    check url.path == "/stream"

  test "Query string preserved in path":
    let url = parseSseUrl("http://example.com/events?token=abc&v=2")
    check url.path == "/events?token=abc&v=2"

  test "No path defaults to /":
    let url = parseSseUrl("http://example.com")
    check url.path == "/"

  test "Trailing slash":
    let url = parseSseUrl("http://example.com/")
    check url.path == "/"

  test "Scheme case-insensitivity":
    let u1 = parseSseUrl("HTTP://example.com/a")
    check u1.scheme == "http"
    let u2 = parseSseUrl("Https://example.com/a")
    check u2.scheme == "https"

  test "Explicit default port 80":
    let url = parseSseUrl("http://example.com:80/path")
    check url.port == 80
    check url.isDefaultPort == true

  test "Explicit default port 443":
    let url = parseSseUrl("https://example.com:443/path")
    check url.port == 443
    check url.isDefaultPort == true

  test "Port boundaries":
    let u1 = parseSseUrl("http://h:1/")
    check u1.port == 1
    let u2 = parseSseUrl("http://h:65535/")
    check u2.port == 65535

  test "Fragment is dropped":
    let url = parseSseUrl("http://example.com/path#fragment")
    check '?' notin url.path or "#" notin url.path
    check "#" notin url.path

  test "Query and fragment":
    let url = parseSseUrl("http://example.com/path?q=1#frag")
    check "q=1" in url.path
    check "#" notin url.path

# ---------------------------------------------------------------------------
# 2. URL Parsing — Validation Failures
# ---------------------------------------------------------------------------

suite "parseSseUrl — Validation":

  test "Missing scheme raises ValueError":
    expect(ValueError):
      discard parseSseUrl("example.com/path")

  test "Unsupported scheme ftp":
    expect(ValueError):
      discard parseSseUrl("ftp://example.com/file")

  test "Unsupported scheme ws":
    expect(ValueError):
      discard parseSseUrl("ws://example.com/socket")

  test "Missing hostname":
    expect(ValueError):
      discard parseSseUrl("http:///path")

  test "Port out of range — 0":
    expect(ValueError):
      discard parseSseUrl("http://host:0/")

  test "Port out of range — 65536":
    expect(ValueError):
      discard parseSseUrl("http://host:65536/")

  test "Port out of range — large":
    expect(ValueError):
      discard parseSseUrl("http://host:99999/")

  test "Non-numeric port":
    expect(ValueError):
      discard parseSseUrl("http://host:abc/")

# ---------------------------------------------------------------------------
# 3. URL Helpers
# ---------------------------------------------------------------------------

suite "URL Helpers":

  test "isDefaultPort — http 80":
    check parseSseUrl("http://h/").isDefaultPort == true

  test "isDefaultPort — https 443":
    check parseSseUrl("https://h/").isDefaultPort == true

  test "isDefaultPort — non-default":
    check parseSseUrl("http://h:8080/").isDefaultPort == false

  test "origin — default port omitted":
    check parseSseUrl("http://example.com/path").origin == "http://example.com"

  test "origin — non-default port included":
    check parseSseUrl("http://example.com:9090/path").origin == "http://example.com:9090"

  test "origin — https default port omitted":
    check parseSseUrl("https://example.com/").origin == "https://example.com"

  test "origin — https non-default port":
    check parseSseUrl("https://example.com:8443/").origin == "https://example.com:8443"

  test "hostHeader — default port":
    check parseSseUrl("http://example.com/").hostHeader == "example.com"

  test "hostHeader — non-default port":
    check parseSseUrl("http://example.com:3000/").hostHeader == "example.com:3000"

  test "$ serialization — default port":
    check $parseSseUrl("http://example.com/events") == "http://example.com/events"

  test "$ serialization — non-default port":
    check $parseSseUrl("http://example.com:9090/events") == "http://example.com:9090/events"

  test "$ serialization — query string":
    check $parseSseUrl("http://h/p?q=1") == "http://h/p?q=1"

  test "toUri round-trip preserves components":
    let url = parseSseUrl("http://example.com:8080/path?q=1")
    let u = url.toUri
    check u.scheme == "http"
    check u.hostname == "example.com"
    check u.port == "8080"
    check u.path == "/path"
    check u.query == "q=1"

  test "toUri omits port when default":
    let url = parseSseUrl("http://example.com/path")
    check url.toUri.port == ""

# ---------------------------------------------------------------------------
# 4. Redirect Resolution
# ---------------------------------------------------------------------------

suite "resolveRedirect":

  test "Absolute URL":
    let base = parseSseUrl("http://old.com/events")
    let r = base.resolveRedirect("http://new.com/stream")
    check r.host == "new.com"
    check r.path == "/stream"

  test "Root-relative path":
    let base = parseSseUrl("http://example.com/old/path")
    let r = base.resolveRedirect("/new-path")
    check r.host == "example.com"
    check r.path == "/new-path"

  test "Relative path":
    let base = parseSseUrl("http://example.com/a/b")
    let r = base.resolveRedirect("c")
    check r.host == "example.com"
    check r.path == "/a/c"

  test "Query-only redirect":
    let base = parseSseUrl("http://example.com/path?old=1")
    let r = base.resolveRedirect("?new=2")
    check r.host == "example.com"
    check "new=2" in r.path

  test "Scheme change http → https":
    let base = parseSseUrl("http://example.com/path")
    let r = base.resolveRedirect("https://example.com/secure")
    check r.useTls == true
    check r.scheme == "https"
    check r.port == 443

  test "Invalid redirect scheme raises ValueError":
    let base = parseSseUrl("http://example.com/path")
    expect(ValueError):
      discard base.resolveRedirect("ftp://other.com/file")

# ---------------------------------------------------------------------------
# 5. Request Building
# ---------------------------------------------------------------------------

suite "buildRequest":

  test "Basic GET request structure":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url)
    check req.startsWith("GET /events HTTP/1.1\r\n")
    check "Host: example.com\r\n" in req
    check "Accept: text/event-stream\r\n" in req
    check "Cache-Control: no-store\r\n" in req
    check req.endsWith("\r\n\r\n")

  test "Non-default port in Host header":
    let url = parseSseUrl("http://example.com:8080/events")
    let req = buildRequest(url)
    check "Host: example.com:8080\r\n" in req

  test "Path includes query string":
    let url = parseSseUrl("http://example.com/events?t=1")
    let req = buildRequest(url)
    check req.startsWith("GET /events?t=1 HTTP/1.1\r\n")

  test "No Last-Event-ID when empty":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url)
    check "Last-Event-ID" notin req

  test "Last-Event-ID present when non-empty":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url, lastEventId = "42")
    check "Last-Event-ID: 42\r\n" in req

  test "Last-Event-ID with complex value":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url, lastEventId = "2024-01-01T12:00:00Z")
    check "Last-Event-ID: 2024-01-01T12:00:00Z\r\n" in req

  test "Request uses CRLF line endings":
    let url = parseSseUrl("http://example.com/")
    let req = buildRequest(url)
    let withoutCrlf = req.replace("\r\n", "")
    check '\r' notin withoutCrlf
    check '\n' notin withoutCrlf

# ---------------------------------------------------------------------------
# 5b. Request Building — Custom Method / Headers / Body
# ---------------------------------------------------------------------------

suite "buildRequest — Custom Requests":

  test "POST request line":
    let url = parseSseUrl("http://example.com/v1/messages")
    let req = buildRequest(url, httpMethod = HttpPost)
    check req.startsWith("POST /v1/messages HTTP/1.1\r\n")

  test "extra headers emitted verbatim after built-ins":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url,
      extraHeaders = @[("x-api-key", "secret"),
                       ("anthropic-version", "2023-06-01")])
    check "x-api-key: secret\r\n" in req
    check "anthropic-version: 2023-06-01\r\n" in req
    check req.find("Cache-Control:") < req.find("x-api-key:")

  test "duplicate extra header names both emitted":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url,
      extraHeaders = @[("X-Multi", "one"), ("X-Multi", "two")])
    check "X-Multi: one\r\n" in req
    check "X-Multi: two\r\n" in req

  test "body appended after blank line with Content-Length":
    let url = parseSseUrl("http://example.com/v1/messages")
    let body = """{"stream":true}"""
    let req = buildRequest(url, httpMethod = HttpPost, body = body)
    check "Content-Length: " & $body.len & "\r\n" in req
    check req.endsWith("\r\n\r\n" & body)

  test "empty body adds no Content-Length":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url, httpMethod = HttpPost)
    check "Content-Length" notin req
    check req.endsWith("\r\n\r\n")

  test "Last-Event-ID still emitted alongside custom options":
    let url = parseSseUrl("http://example.com/events")
    let req = buildRequest(url, lastEventId = "42", httpMethod = HttpPost,
                           extraHeaders = @[("X-A", "b")], body = "payload")
    check "Last-Event-ID: 42\r\n" in req
    check "X-A: b\r\n" in req
    check req.endsWith("payload")

  test "defaults produce byte-identical spec-compliant GET request":
    let url = parseSseUrl("http://example.com/events?t=1")
    check buildRequest(url, lastEventId = "7") ==
      buildRequest(url, "7", HttpGet, @[], "")

suite "validateRequest":

  test "defaults pass":
    validateRequest(HttpGet, @[], "")

  test "POST with body and headers passes":
    validateRequest(HttpPost,
      @[("content-type", "application/json"), ("x-api-key", "k")],
      """{"stream":true}""")

  test "empty header name rejected":
    expect(ValueError):
      validateRequest(HttpGet, @[("", "value")], "")

  test "CR/LF in header name rejected":
    expect(ValueError):
      validateRequest(HttpGet, @[("X-Bad\r\nInjected", "v")], "")

  test "CR/LF in header value rejected":
    expect(ValueError):
      validateRequest(HttpGet, @[("X-Bad", "v\r\nInjected: yes")], "")
    expect(ValueError):
      validateRequest(HttpGet, @[("X-Bad", "v\ninjected")], "")

  test "reserved headers rejected case-insensitively":
    for name in ["Host", "host", "Accept", "ACCEPT", "Cache-Control",
                 "Last-Event-ID", "last-event-id", "Content-Length"]:
      expect(ValueError):
        validateRequest(HttpGet, @[(name, "v")], "")

  test "non-reserved headers accepted":
    validateRequest(HttpGet, @[("Content-Type", "application/json"),
                               ("Authorization", "Bearer t")], "")

  test "body with GET rejected":
    expect(ValueError):
      validateRequest(HttpGet, @[], "payload")

  test "body with HEAD rejected":
    expect(ValueError):
      validateRequest(HttpHead, @[], "payload")

  test "body with POST/PUT/PATCH/DELETE accepted":
    for m in [HttpPost, HttpPut, HttpPatch, HttpDelete]:
      validateRequest(m, @[], "payload")

# ---------------------------------------------------------------------------
# 6. HeaderParser — Single Feed
# ---------------------------------------------------------------------------

suite "HeaderParser — Single Feed":

  test "Standard CRLF response":
    var hp = initHeaderParser()
    let body = hp.feed("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
    check hp.isComplete
    check not hp.hasFailed
    check body == ""

  test "Bare-LF response":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 200 OK\nContent-Type: text/event-stream\n\n")
    check hp.isComplete

  test "Body bytes returned":
    var hp = initHeaderParser()
    let body = hp.feed("HTTP/1.1 200 OK\r\n\r\nbody here")
    check hp.isComplete
    check body == "body here"

  test "No body bytes when headers end at data boundary":
    var hp = initHeaderParser()
    let body = hp.feed("HTTP/1.1 200 OK\r\n\r\n")
    check hp.isComplete
    check body == ""

  test "Mixed CRLF and LF terminator":
    var hp = initHeaderParser()
    let body = hp.feed("HTTP/1.1 200 OK\nContent-Type: text/event-stream\n\r\n")
    check hp.isComplete
    check body == ""

  test "Empty feed is no-op":
    var hp = initHeaderParser()
    let body = hp.feed("")
    check not hp.isComplete
    check body == ""

  test "Subsequent feeds after complete are no-ops":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 200 OK\r\n\r\n")
    check hp.isComplete
    let body2 = hp.feed("more data")
    check body2 == ""

# ---------------------------------------------------------------------------
# 7. HeaderParser — Incremental / Split Feed
# ---------------------------------------------------------------------------

suite "HeaderParser — Split Feed":

  test "Terminator \\r\\n|\\r\\n split across feeds":
    var hp = initHeaderParser()
    let b1 = hp.feed("HTTP/1.1 200 OK\r\n")
    check not hp.isComplete
    check b1 == ""
    let b2 = hp.feed("\r\n")
    check hp.isComplete
    check b2 == ""

  test "Terminator \\r\\n\\r|\\n split across feeds":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 200 OK\r\n\r")
    check not hp.isComplete
    let b2 = hp.feed("\nbody")
    check hp.isComplete
    check b2 == "body"

  test "Terminator \\n|\\n split across feeds":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 200 OK\n")
    check not hp.isComplete
    discard hp.feed("\n")
    check hp.isComplete

  test "Terminator \\n\\r|\\n split across feeds":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 200 OK\n\r")
    check not hp.isComplete
    let b2 = hp.feed("\nrest")
    check hp.isComplete
    check b2 == "rest"

  test "Byte-at-a-time feeding":
    let raw = "HTTP/1.1 200 OK\r\nHost: x\r\n\r\nbody"
    var hp = initHeaderParser()
    var bodyBytes = ""
    for ch in raw:
      let b = hp.feed($ch)
      bodyBytes.add(b)
      if hp.isComplete:
        break
    check hp.isComplete
    let resp = hp.parse()
    check resp.statusCode == 200

  test "Headers with body split at boundary":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n")
    check not hp.isComplete
    discard hp.feed("Transfer-Encoding: chunked\r\n")
    check not hp.isComplete
    let body = hp.feed("\r\nchunk data")
    check hp.isComplete
    check body == "chunk data"

  test "Multiple small feeds accumulate":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/")
    discard hp.feed("1.1 ")
    discard hp.feed("200 OK")
    discard hp.feed("\r\n")
    discard hp.feed("X: y")
    discard hp.feed("\r\n")
    discard hp.feed("\r\n")
    check hp.isComplete
    let resp = hp.parse()
    check resp.statusCode == 200

# ---------------------------------------------------------------------------
# 8. HeaderParser — parse() Status Line
# ---------------------------------------------------------------------------

suite "HeaderParser — Status Line Parsing":

  test "HTTP/1.1 200 OK":
    let resp = parseRawResponse("HTTP/1.1 200 OK\r\n\r\n")
    check resp.statusCode == 200

  test "HTTP/1.0 301 Moved Permanently":
    let resp = parseRawResponse("HTTP/1.0 301 Moved Permanently\r\n\r\n")
    check resp.statusCode == 301

  test "HTTP/1.1 204 No Content":
    let resp = parseRawResponse("HTTP/1.1 204 No Content\r\n\r\n")
    check resp.statusCode == 204

  test "HTTP/1.1 404 Not Found":
    let resp = parseRawResponse("HTTP/1.1 404 Not Found\r\n\r\n")
    check resp.statusCode == 404

  test "HTTP/1.1 500 Internal Server Error":
    let resp = parseRawResponse("HTTP/1.1 500 Internal Server Error\r\n\r\n")
    check resp.statusCode == 500

  test "No reason phrase":
    let resp = parseRawResponse("HTTP/1.1 200\r\n\r\n")
    check resp.statusCode == 200

  test "Extra spaces before status code":
    let resp = parseRawResponse("HTTP/1.1  200 OK\r\n\r\n")
    check resp.statusCode == 200

  test "Malformed status line — no space":
    var hp = initHeaderParser()
    discard hp.feed("INVALID\r\n\r\n")
    check hp.isComplete
    discard hp.parse()
    check hp.hasFailed
    check "malformed status line" in hp.failMessage

  test "Missing status code":
    var hp = initHeaderParser()
    discard hp.feed("HTTP/1.1 \r\n\r\n")
    check hp.isComplete
    discard hp.parse()
    check hp.hasFailed
    check "missing status code" in hp.failMessage

  test "parse called before complete":
    var hp = initHeaderParser()
    discard hp.parse()
    check hp.hasFailed
    check "before headers are complete" in hp.failMessage

# ---------------------------------------------------------------------------
# 9. HeaderParser — Header Parsing
# ---------------------------------------------------------------------------

suite "HeaderParser — Header Parsing":

  test "Multiple headers parsed and lowercased":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "Content-Type: text/event-stream\r\n" &
      "Transfer-Encoding: chunked\r\n" &
      "\r\n")
    check resp.getHeader("content-type") == "text/event-stream"
    check resp.getHeader("transfer-encoding") == "chunked"

  test "Header names lowercased":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "X-Custom-Header: SomeValue\r\n" &
      "\r\n")
    check resp.getHeader("x-custom-header") == "SomeValue"

  test "Header value whitespace trimmed":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "X-Test:   spaced   \r\n" &
      "\r\n")
    check resp.getHeader("x-test") == "spaced"

  test "Duplicate headers both stored":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "Set-Cookie: a=1\r\n" &
      "Set-Cookie: b=2\r\n" &
      "\r\n")
    var values: seq[string] = @[]
    for (n, v) in resp.headers:
      if n == "set-cookie":
        values.add(v)
    check values.len == 2
    check values[0] == "a=1"
    check values[1] == "b=2"

  test "getHeader returns first value for duplicates":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "X-Dup: first\r\n" &
      "X-Dup: second\r\n" &
      "\r\n")
    check resp.getHeader("x-dup") == "first"

  test "hasHeader true for present header":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "Content-Type: text/plain\r\n" &
      "\r\n")
    check resp.hasHeader("content-type") == true

  test "hasHeader false for absent header":
    let resp = parseRawResponse("HTTP/1.1 200 OK\r\n\r\n")
    check resp.hasHeader("content-type") == false

  test "getHeader returns empty for absent header":
    let resp = parseRawResponse("HTTP/1.1 200 OK\r\n\r\n")
    check resp.getHeader("x-missing") == ""

  test "Header with empty value":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "X-Empty:\r\n" &
      "\r\n")
    check resp.hasHeader("x-empty") == true
    check resp.getHeader("x-empty") == ""

  test "Header value with colons":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "X-Time: 2024-01-01T12:00:00Z\r\n" &
      "\r\n")
    check resp.getHeader("x-time") == "2024-01-01T12:00:00Z"

  test "Header line without colon is skipped":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\r\n" &
      "MalformedLine\r\n" &
      "Good-Header: value\r\n" &
      "\r\n")
    check resp.headers.len == 1
    check resp.getHeader("good-header") == "value"

  test "Bare-LF header parsing":
    let resp = parseRawResponse(
      "HTTP/1.1 200 OK\n" &
      "Content-Type: text/event-stream\n" &
      "\n")
    check resp.statusCode == 200
    check resp.getHeader("content-type") == "text/event-stream"

# ---------------------------------------------------------------------------
# 10. HeaderParser — Overflow Protection
# ---------------------------------------------------------------------------

suite "HeaderParser — Overflow":

  test "Terminated headers exceeding maxSize are rejected":
    var hp = initHeaderParser(maxSize = 32)
    discard hp.feed("HTTP/1.1 200 OK\r\nX-Long: " & repeat('x', 100) & "\r\n\r\n")
    check hp.hasFailed
    check not hp.isComplete
    check "exceed" in hp.failMessage

  test "Multiple feeds accumulating past maxSize — unterminated":
    var hp = initHeaderParser(maxSize = 50)
    discard hp.feed("HTTP/1.1 200 OK\r\n")
    check not hp.hasFailed
    discard hp.feed("X-Header: " & repeat('a', 100) & "\r\n")
    check hp.hasFailed

  test "Multiple feeds accumulating past maxSize — terminated":
    var hp = initHeaderParser(maxSize = 50)
    discard hp.feed("HTTP/1.1 200 OK\r\n")
    check not hp.hasFailed
    discard hp.feed("X-Header: " & repeat('a', 100) & "\r\n\r\n")
    check hp.hasFailed
    check not hp.isComplete

  test "Headers within maxSize with terminator succeed":
    var hp = initHeaderParser(maxSize = 100)
    discard hp.feed("HTTP/1.1 200 OK\r\n\r\n")
    check hp.isComplete
    check not hp.hasFailed

  test "Unterminated stream exceeding maxSize fails":
    var hp = initHeaderParser(maxSize = 20)
    discard hp.feed(repeat('x', 21))
    check hp.hasFailed

  test "Body bytes beyond maxSize don't trigger failure":
    var hp = initHeaderParser(maxSize = 30)
    let body = hp.feed("HTTP/1.1 200 OK\r\n\r\n" & repeat('B', 200))
    check hp.isComplete
    check not hp.hasFailed
    check body == repeat('B', 200)

# ---------------------------------------------------------------------------
# 11. Response Validation
# ---------------------------------------------------------------------------

suite "validateResponse — scrOk":

  test "200 + text/event-stream":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "text/event-stream")])
    check validateResponse(resp) == scrOk

  test "200 + text/event-stream with charset parameter":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "text/event-stream; charset=utf-8")])
    check validateResponse(resp) == scrOk

  test "200 + case-insensitive content-type":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "Text/Event-Stream")])
    check validateResponse(resp) == scrOk

  test "200 + content-type with whitespace around semicolon":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "text/event-stream ; charset=utf-8")])
    check validateResponse(resp) == scrOk

  test "200 + content-type with multiple parameters":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "text/event-stream; charset=utf-8; boundary=something")])
    check validateResponse(resp) == scrOk

suite "validateResponse — scrRedirect":

  test "301 Moved Permanently":
    let resp = HttpResponse(statusCode: 301, headers: @[])
    check validateResponse(resp) == scrRedirect

  test "302 Found":
    let resp = HttpResponse(statusCode: 302, headers: @[])
    check validateResponse(resp) == scrRedirect

  test "303 See Other":
    let resp = HttpResponse(statusCode: 303, headers: @[])
    check validateResponse(resp) == scrRedirect

  test "307 Temporary Redirect":
    let resp = HttpResponse(statusCode: 307, headers: @[])
    check validateResponse(resp) == scrRedirect

  test "308 Permanent Redirect":
    let resp = HttpResponse(statusCode: 308, headers: @[])
    check validateResponse(resp) == scrRedirect

suite "validateResponse — scrFail":

  test "200 + wrong content-type":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "text/plain")])
    check validateResponse(resp) == scrFail

  test "200 + application/json":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-type", "application/json")])
    check validateResponse(resp) == scrFail

  test "200 + missing content-type":
    let resp = HttpResponse(statusCode: 200, headers: @[])
    check validateResponse(resp) == scrFail

  test "204 No Content — stops reconnection":
    let resp = HttpResponse(statusCode: 204, headers: @[])
    check validateResponse(resp) == scrFail

  test "404 Not Found":
    let resp = HttpResponse(statusCode: 404, headers: @[])
    check validateResponse(resp) == scrFail

  test "500 Internal Server Error":
    let resp = HttpResponse(statusCode: 500, headers: @[])
    check validateResponse(resp) == scrFail

  test "100 Continue":
    let resp = HttpResponse(statusCode: 100, headers: @[])
    check validateResponse(resp) == scrFail

  test "101 Switching Protocols":
    let resp = HttpResponse(statusCode: 101, headers: @[])
    check validateResponse(resp) == scrFail

# ---------------------------------------------------------------------------
# 12. Redirect Helpers
# ---------------------------------------------------------------------------

suite "Redirect Helpers":

  test "isRedirect — true for all redirect codes":
    for code in [301, 302, 303, 307, 308]:
      let resp = HttpResponse(statusCode: code, headers: @[])
      check resp.isRedirect == true

  test "isRedirect — false for non-redirect codes":
    for code in [200, 204, 304, 400, 404, 500]:
      let resp = HttpResponse(statusCode: code, headers: @[])
      check resp.isRedirect == false

  test "isPermanentRedirect — 301":
    check HttpResponse(statusCode: 301, headers: @[]).isPermanentRedirect == true

  test "isPermanentRedirect — 308":
    check HttpResponse(statusCode: 308, headers: @[]).isPermanentRedirect == true

  test "isPermanentRedirect — false for temporary redirects":
    for code in [302, 303, 307]:
      check HttpResponse(statusCode: code, headers: @[]).isPermanentRedirect == false

  test "redirectLocation extracts Location header":
    let resp = HttpResponse(statusCode: 301,
      headers: @[("location", "http://new.com/path")])
    check resp.redirectLocation == "http://new.com/path"

  test "redirectLocation returns empty when missing":
    let resp = HttpResponse(statusCode: 301, headers: @[])
    check resp.redirectLocation == ""

# ---------------------------------------------------------------------------
# 13. Transfer Mode Detection
# ---------------------------------------------------------------------------

suite "Transfer Mode Detection":

  test "Chunked Transfer-Encoding":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("transfer-encoding", "chunked")])
    check detectTransferMode(resp) == tmChunked

  test "Chunked with other encodings (gzip, chunked)":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("transfer-encoding", "gzip, chunked")])
    check detectTransferMode(resp) == tmChunked

  test "Content-Length without chunked":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-length", "1234")])
    check detectTransferMode(resp) == tmContentLength

  test "Neither header → identity":
    let resp = HttpResponse(statusCode: 200, headers: @[])
    check detectTransferMode(resp) == tmIdentity

  test "Both chunked and content-length → chunked wins":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("transfer-encoding", "chunked"),
                  ("content-length", "1234")])
    check detectTransferMode(resp) == tmChunked

  test "Case-insensitive transfer-encoding":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("transfer-encoding", "Chunked")])
    check detectTransferMode(resp) == tmChunked

suite "contentLength":

  test "Valid Content-Length":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-length", "1234")])
    check contentLength(resp) == 1234

  test "Missing Content-Length":
    let resp = HttpResponse(statusCode: 200, headers: @[])
    check contentLength(resp) == -1

  test "Invalid Content-Length":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-length", "abc")])
    check contentLength(resp) == -1

  test "Zero Content-Length":
    let resp = HttpResponse(statusCode: 200,
      headers: @[("content-length", "0")])
    check contentLength(resp) == 0

# ---------------------------------------------------------------------------
# 14. ChunkedDecoder — Basic Decoding
# ---------------------------------------------------------------------------

suite "ChunkedDecoder — Basic":

  test "Single chunk + zero terminator":
    let decoded = decodeAllChunked("5\r\nhello\r\n0\r\n\r\n")
    check decoded == "hello"

  test "Multiple chunks":
    let decoded = decodeAllChunked(
      "5\r\nhello\r\n" &
      "6\r\n world\r\n" &
      "0\r\n\r\n")
    check decoded == "hello world"

  test "Zero-length chunk terminates":
    var dec = initChunkedDecoder()
    discard dec.feed("0\r\n\r\n")
    check dec.isFinished

  test "Hex chunk size — lowercase":
    let decoded = decodeAllChunked("a\r\n0123456789\r\n0\r\n\r\n")
    check decoded == "0123456789"
    check decoded.len == 10

  test "Hex chunk size — uppercase":
    let decoded = decodeAllChunked("A\r\n0123456789\r\n0\r\n\r\n")
    check decoded == "0123456789"

  test "Hex chunk size — mixed case":
    let decoded = decodeAllChunked("1a\r\n" & repeat('x', 26) & "\r\n0\r\n\r\n")
    check decoded == repeat('x', 26)

  test "Chunk extensions stripped":
    let decoded = decodeAllChunked("5;ext=val\r\nhello\r\n0\r\n\r\n")
    check decoded == "hello"

  test "Bare LF line terminator (lenient)":
    let decoded = decodeAllChunked("5\nhello\n0\n\n")
    check decoded == "hello"

  test "Feed after finished returns empty":
    var dec = initChunkedDecoder()
    discard dec.feed("0\r\n\r\n")
    check dec.isFinished
    check dec.feed("more data") == ""

  test "Feed empty string is no-op":
    var dec = initChunkedDecoder()
    check dec.feed("") == ""
    check not dec.isFinished
    check not dec.hasFailed

# ---------------------------------------------------------------------------
# 15. ChunkedDecoder — Incremental / Split Feed
# ---------------------------------------------------------------------------

suite "ChunkedDecoder — Incremental":

  test "Chunk size split across feeds":
    var dec = initChunkedDecoder()
    var result = ""
    result.add(dec.feed("5"))
    result.add(dec.feed("\r\n"))
    result.add(dec.feed("hello"))
    result.add(dec.feed("\r\n"))
    result.add(dec.feed("0\r\n\r\n"))
    check result == "hello"
    check dec.isFinished

  test "Chunk data split across feeds":
    var dec = initChunkedDecoder()
    var result = ""
    result.add(dec.feed("5\r\n"))
    result.add(dec.feed("hel"))
    result.add(dec.feed("lo"))
    result.add(dec.feed("\r\n0\r\n\r\n"))
    check result == "hello"
    check dec.isFinished

  test "CRLF after data split across feeds":
    var dec = initChunkedDecoder()
    var result = ""
    result.add(dec.feed("5\r\nhello\r"))
    result.add(dec.feed("\n0\r\n\r\n"))
    check result == "hello"
    check dec.isFinished

  test "Byte-at-a-time decoding":
    let raw = "5\r\nhello\r\n3\r\nfoo\r\n0\r\n\r\n"
    let decoded = decodeChunkedBytewise(raw)
    check decoded == "hellofoo"

  test "Multiple chunks across many feeds":
    var dec = initChunkedDecoder()
    var result = ""
    result.add(dec.feed("5\r\nhello\r\n"))
    check not dec.isFinished
    result.add(dec.feed("5\r\nworld\r\n"))
    check not dec.isFinished
    result.add(dec.feed("0\r\n\r\n"))
    check dec.isFinished
    check result == "helloworld"

  test "Large chunk data across feeds":
    let data = repeat('A', 1000)
    var dec = initChunkedDecoder()
    var result = ""
    result.add(dec.feed("3e8\r\n"))   # 0x3e8 = 1000
    result.add(dec.feed(data[0 ..< 500]))
    result.add(dec.feed(data[500 .. ^1]))
    result.add(dec.feed("\r\n0\r\n\r\n"))
    check result == data
    check dec.isFinished

# ---------------------------------------------------------------------------
# 16. ChunkedDecoder — Error Cases
# ---------------------------------------------------------------------------

suite "ChunkedDecoder — Errors":

  test "Invalid hex character":
    var dec = initChunkedDecoder()
    discard dec.feed("xyz\r\n")
    check dec.hasFailed
    check "invalid hex" in dec.failMessage

  test "Empty chunk size":
    var dec = initChunkedDecoder()
    discard dec.feed("\r\n")
    check dec.hasFailed
    check "empty chunk size" in dec.failMessage

  test "Chunk size exceeds maxChunkSize":
    var dec = initChunkedDecoder(maxChunkSize = 10)
    discard dec.feed("ff\r\n")  # 255 > 10
    check dec.hasFailed
    check "exceeds limit" in dec.failMessage

  test "Size line exceeds MaxSizeLineLen":
    var dec = initChunkedDecoder()
    discard dec.feed(repeat('a', 600))
    check dec.hasFailed
    check "size line exceeds" in dec.failMessage

  test "Overflow on huge hex value":
    var dec = initChunkedDecoder(maxChunkSize = high(int))
    discard dec.feed("ffffffffffffffffffffffff\r\n")
    check dec.hasFailed
    check "overflow" in dec.failMessage

  test "Unexpected byte after chunk data":
    var dec = initChunkedDecoder()
    discard dec.feed("5\r\nhelloX")
    check dec.hasFailed
    check "expected CRLF" in dec.failMessage

  test "Feed after failure returns empty":
    var dec = initChunkedDecoder()
    discard dec.feed("invalid\r\n")
    check dec.hasFailed
    check dec.feed("more") == ""

# ---------------------------------------------------------------------------
# 17. End-to-End — Full HTTP Response through HeaderParser + Validation
# ---------------------------------------------------------------------------

suite "End-to-End — HeaderParser + Validation":

  test "Parse and validate a valid SSE response":
    let raw = "HTTP/1.1 200 OK\r\n" &
              "Content-Type: text/event-stream\r\n" &
              "Cache-Control: no-cache\r\n" &
              "\r\n"
    let resp = parseRawResponse(raw)
    check resp.statusCode == 200
    check validateResponse(resp) == scrOk

  test "Parse and validate a redirect response":
    let raw = "HTTP/1.1 301 Moved Permanently\r\n" &
              "Location: http://new.com/events\r\n" &
              "\r\n"
    let resp = parseRawResponse(raw)
    check validateResponse(resp) == scrRedirect
    check resp.redirectLocation == "http://new.com/events"

  test "Parse and validate a 204 response (fail, stop reconnection)":
    let raw = "HTTP/1.1 204 No Content\r\n\r\n"
    let resp = parseRawResponse(raw)
    check validateResponse(resp) == scrFail

  test "Parse and detect chunked transfer":
    let raw = "HTTP/1.1 200 OK\r\n" &
              "Content-Type: text/event-stream\r\n" &
              "Transfer-Encoding: chunked\r\n" &
              "\r\n"
    let resp = parseRawResponse(raw)
    check validateResponse(resp) == scrOk
    check detectTransferMode(resp) == tmChunked

  test "Full pipeline: parse headers → validate → detect transfer → decode chunk":
    let raw = "HTTP/1.1 200 OK\r\n" &
              "Content-Type: text/event-stream\r\n" &
              "Transfer-Encoding: chunked\r\n" &
              "\r\n" &
              "d\r\ndata: hello\n\n\r\n0\r\n\r\n"
    var hp = initHeaderParser()
    let body = hp.feed(raw)
    check hp.isComplete
    let resp = hp.parse()
    check validateResponse(resp) == scrOk
    check detectTransferMode(resp) == tmChunked
    var dec = initChunkedDecoder()
    let decoded = dec.feed(body)
    check decoded == "data: hello\n\n"
    check dec.isFinished
