## Async SSE client implementing the EventSource connection lifecycle.
##
## Manages the full state machine: connect → open → read → reconnect,
## with automatic exponential backoff, redirect following, and optional
## stall detection. Delivers events via callbacks set on the client object.
##
## Uses std/asyncdispatch + std/asyncnet for non-blocking I/O. The parser
## and HTTP layers are pure (no I/O) and imported from sibling modules.

import std/[asyncdispatch, asyncnet]
import types, parser, http

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

type
  AsyncSseClientConfig* = object
    ## Configuration for the async SSE client.
    autoReconnect*: bool      ## Reconnect on connection loss (spec-compliant).
                              ## Set false for single-shot streams.
    maxRedirects*: int        ## Maximum redirects per connection attempt.
    maxReconnectDelay*: int   ## Backoff cap in milliseconds.
    stallTimeout*: int        ## Inactivity timeout in ms (0 = disabled).
    recvSize*: int            ## Socket recv buffer size hint in bytes.

const
  DefaultConfig* = AsyncSseClientConfig(
    autoReconnect: true,
    maxRedirects: 10,
    maxReconnectDelay: 60_000,
    stallTimeout: 0,
    recvSize: 4096,
  )

# ---------------------------------------------------------------------------
# Client Type
# ---------------------------------------------------------------------------

type
  ConnectOutcome = enum
    coSuccess  ## 200 + text/event-stream; body ready to stream.
    coRetry    ## Non-fatal failure; should reconnect.
    coFatal    ## Fatal failure; connection is closed permanently.

  AsyncSseClient* = ref object
    ## An async SSE connection with automatic reconnection.
    ##
    ## Set callbacks (`onEvent`, `onOpen`, `onError`, `onComment`) before
    ## calling `connect`. The `connect` future runs until `close()` is
    ## called or a fatal (non-recoverable) error occurs.

    # -- Public state --
    readyState*: ReadyState
    url*: SseUrl              ## Reconnection URL (updated only by permanent redirects).
    config*: AsyncSseClientConfig

    # -- Callbacks --
    onEvent*: SseEventHandler
    onComment*: SseCommentHandler
    onOpen*: SseNotifyHandler
    onError*: SseErrorHandler

    # -- Private --
    parser: SseParser
    socket: AsyncSocket
    cancelToken: CancelToken
    consecutiveFailures: int
    transferMode: TransferMode
    chunkedDecoder: ChunkedDecoder
    contentRemaining: int

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newAsyncSseClient*(rawUrl: string;
                        config = DefaultConfig;
                        cancelToken: CancelToken = nil): AsyncSseClient =
  ## Create a new async SSE client.
  ##
  ## Validates the URL synchronously (spec §6.1 steps 1–3). Raises
  ## `ValueError` if the URL is invalid or uses an unsupported scheme.
  ## No network activity occurs until `connect` is called.
  let parsedUrl = parseSseUrl(rawUrl)
  result = AsyncSseClient(
    readyState: Connecting,
    url: parsedUrl,
    config: config,
    cancelToken: cancelToken,
    parser: initSseParser(onEvent = nil),
  )

# ---------------------------------------------------------------------------
# Internal: Cancellation Check
# ---------------------------------------------------------------------------

proc isCancelled(client: AsyncSseClient): bool =
  client.readyState == Closed or
    (client.cancelToken != nil and client.cancelToken.cancelled)

# ---------------------------------------------------------------------------
# Internal: Callback Wiring
# ---------------------------------------------------------------------------

proc wireParser(client: AsyncSseClient; effectiveUrl: SseUrl) =
  ## Wire the parser's callbacks to the client's callbacks, injecting
  ## the origin into each event (spec §5.1 step 4).
  let origin = effectiveUrl.origin
  client.parser.onEvent = proc(ev: SseEvent) =
    if client.readyState == Closed: return
    var event = ev
    event.origin = origin
    if client.onEvent != nil:
      client.onEvent(event)
  client.parser.onComment = proc(comment: string) =
    if client.readyState == Closed: return
    if client.onComment != nil:
      client.onComment(comment)

# ---------------------------------------------------------------------------
# Internal: Fire Lifecycle Events
# ---------------------------------------------------------------------------

proc fireOpen(client: AsyncSseClient) =
  if client.readyState == Closed: return
  client.readyState = Open
  client.consecutiveFailures = 0
  if client.onOpen != nil:
    client.onOpen()

proc fireError(client: AsyncSseClient; msg: string) =
  ## Fire the error callback unconditionally (spec §6.4 fires error after
  ## setting CLOSED). No readyState guard here — close() never calls this
  ## proc, and all internal callers are protected by isCancelled checks.
  if client.onError != nil:
    client.onError(msg)

# ---------------------------------------------------------------------------
# Internal: Socket Lifecycle
# ---------------------------------------------------------------------------

proc closeSocket(client: AsyncSseClient) =
  if client.socket != nil:
    client.socket.close()
    client.socket = nil

proc openSocket(client: AsyncSseClient; target: SseUrl): Future[bool] {.async.} =
  ## Open a TCP connection to the target host:port.
  ## Returns true on success, false on network error.
  try:
    client.socket = newAsyncSocket()
    await client.socket.connect(target.host, Port(target.port))
    # TLS wrapping deferred to Phase 8:
    # if target.useTls:
    #   let ctx = newContext(...)
    #   wrapConnectedSocket(ctx, client.socket, handshakeAsClient, target.host)
    return true
  except CatchableError:
    client.closeSocket()
    return false

# ---------------------------------------------------------------------------
# Internal: Body Feeding
# ---------------------------------------------------------------------------

proc feedBody(client: AsyncSseClient; raw: string) =
  ## Decode (if chunked) and feed bytes to the parser.
  case client.transferMode
  of tmChunked:
    let decoded = client.chunkedDecoder.feed(raw)
    if decoded.len > 0:
      client.parser.feed(decoded)
  of tmContentLength:
    let take = min(raw.len, client.contentRemaining)
    if take > 0:
      client.parser.feed(raw[0 ..< take])
      client.contentRemaining -= take
  of tmIdentity:
    client.parser.feed(raw)

proc isBodyFinished(client: AsyncSseClient): bool =
  case client.transferMode
  of tmChunked:
    client.chunkedDecoder.isFinished or client.chunkedDecoder.hasFailed
  of tmContentLength:
    client.contentRemaining <= 0
  of tmIdentity:
    false

# ---------------------------------------------------------------------------
# Internal: Single Connection Attempt
# ---------------------------------------------------------------------------

proc attemptConnect(client: AsyncSseClient): Future[ConnectOutcome] {.async.} =
  ## Perform one connection attempt: open socket, send request, parse
  ## response headers, follow redirects, validate.
  ##
  ## On success: socket is open, transfer state initialized, any body
  ## bytes that arrived with headers already fed to parser.
  var currentUrl = client.url
  var redirectCount = 0

  while true:
    if client.isCancelled:
      return coFatal

    # -- Open socket --
    if not await client.openSocket(currentUrl):
      return coRetry

    if client.isCancelled:
      client.closeSocket()
      return coFatal

    # -- Send request --
    let lastId = client.parser.lastEventId
    let request = buildRequest(currentUrl, lastId)
    try:
      await client.socket.send(request)
    except CatchableError:
      client.closeSocket()
      return coRetry

    if client.isCancelled:
      client.closeSocket()
      return coFatal

    # -- Read response headers --
    var hp = initHeaderParser()
    var bodyRemainder = ""
    while not hp.isComplete and not hp.hasFailed:
      if client.isCancelled:
        client.closeSocket()
        return coFatal
      var data: string
      try:
        data = await client.socket.recv(client.config.recvSize)
      except CatchableError:
        client.closeSocket()
        return coRetry
      if data.len == 0:
        client.closeSocket()
        return coRetry
      bodyRemainder = hp.feed(data)

    if hp.hasFailed:
      client.closeSocket()
      return coRetry

    var resp = hp.parse()
    if hp.hasFailed:
      client.closeSocket()
      return coRetry

    # -- Validate response --
    case validateResponse(resp)

    of scrOk:
      # Wire parser with correct origin for the final URL.
      # Do NOT set client.url here — it is the reconnection URL and must
      # only be updated by permanent redirects (lines above).
      client.wireParser(currentUrl)
      client.parser.reset()
      client.transferMode = detectTransferMode(resp)
      if client.transferMode == tmChunked:
        client.chunkedDecoder = initChunkedDecoder()
      client.contentRemaining = contentLength(resp)
      if bodyRemainder.len > 0:
        client.feedBody(bodyRemainder)
      return coSuccess

    of scrRedirect:
      client.closeSocket()
      inc redirectCount
      if redirectCount > client.config.maxRedirects:
        client.readyState = Closed
        client.fireError("too many redirects")
        return coFatal
      let location = redirectLocation(resp)
      if location.len == 0:
        client.readyState = Closed
        client.fireError("redirect with no Location header")
        return coFatal
      try:
        let newUrl = resolveRedirect(currentUrl, location)
        if isPermanentRedirect(resp):
          client.url = newUrl
        currentUrl = newUrl
      except ValueError:
        client.readyState = Closed
        client.fireError("invalid redirect location: " & location)
        return coFatal

    of scrFail:
      client.closeSocket()
      client.readyState = Closed
      client.fireError("connection failed: HTTP " & $resp.statusCode)
      return coFatal

# ---------------------------------------------------------------------------
# Internal: Body Read Loop
# ---------------------------------------------------------------------------

proc readBody(client: AsyncSseClient) {.async.} =
  ## Read the response body, feeding chunks to the parser.
  ## Returns when: connection closes, body finishes, stall timeout
  ## fires, or client is cancelled. Calls `parser.complete()` on all
  ## non-cancel exit paths to flush any pending CR line ending and
  ## discard incomplete events per spec §3.5. On stall timeout, the
  ## socket is closed immediately and the orphaned recv future's error
  ## is consumed to prevent unhandled-exception warnings from
  ## asyncdispatch.
  while true:
    if client.isCancelled:
      return
    if client.isBodyFinished:
      client.parser.complete()
      return

    var data: string
    try:
      if client.config.stallTimeout > 0:
        let recvFut = client.socket.recv(client.config.recvSize)
        let completed = await withTimeout(recvFut, client.config.stallTimeout)
        if not completed:
          recvFut.addCallback(proc(f: Future[string]) =
            if f.failed: discard f.readError)
          client.closeSocket()
          client.parser.complete()
          return
        data = recvFut.read()
      else:
        data = await client.socket.recv(client.config.recvSize)
    except CatchableError:
      client.parser.complete()
      return

    if data.len == 0:
      client.parser.complete()
      return

    client.feedBody(data)

# ---------------------------------------------------------------------------
# Internal: Backoff Delay
# ---------------------------------------------------------------------------

proc reconnectDelay(client: AsyncSseClient): int =
  ## Compute reconnection delay with exponential backoff.
  ## When consecutiveFailures is 0 (e.g. after a clean end-of-body), returns
  ## the plain reconnectionTime per spec §6.3 step 2. Backoff only applies
  ## after consecutive failed connection attempts (coRetry).
  ## Formula: min(reconnectionTime * 2^failures, maxReconnectDelay)
  let cap = client.config.maxReconnectDelay
  let base = client.parser.reconnectionTime
  if base <= 0:
    return base
  let shift = min(client.consecutiveFailures, 30)
  let multiplier = 1 shl shift
  if base > cap div multiplier:
    return cap
  let delay = base * multiplier
  if delay > cap:
    return cap
  return delay

proc cancelAwareSleep(client: AsyncSseClient; ms: int) {.async.} =
  ## Sleep for `ms` milliseconds, but return early if cancelled.
  ## Polls every 250ms.
  var remaining = ms
  while remaining > 0:
    if client.isCancelled: return
    let chunk = min(remaining, 250)
    await sleepAsync(chunk)
    remaining -= chunk

# ---------------------------------------------------------------------------
# Public: Connect (Main Event Loop)
# ---------------------------------------------------------------------------

proc connect*(client: AsyncSseClient) {.async.} =
  ## Begin the SSE connection lifecycle.
  ##
  ## This future runs until `close()` is called or a fatal error occurs.
  ## It internally handles reconnection with exponential backoff when
  ## `autoReconnect` is true. Set all callbacks before calling this.
  client.readyState = Connecting

  while not client.isCancelled:
    let outcome = await client.attemptConnect()

    case outcome
    of coFatal:
      break
    of coRetry:
      if client.isCancelled:
        break
      if not client.config.autoReconnect:
        client.readyState = Closed
        client.fireError("connection failed")
        break
      client.readyState = Connecting
      client.fireError("connection error; reconnecting")
      inc client.consecutiveFailures
      let delay = client.reconnectDelay()
      await client.cancelAwareSleep(delay)
      continue
    of coSuccess:
      if client.isCancelled:
        break

    # -- Connected successfully --
    client.fireOpen()

    if client.isCancelled:
      break

    # -- Body read loop --
    await client.readBody()

    # -- Connection ended --
    client.closeSocket()

    if client.isCancelled:
      break

    if not client.config.autoReconnect:
      client.readyState = Closed
      client.fireError("stream ended")
      break

    # Reestablish (spec §6.3): first reconnect uses plain reconnectionTime;
    # backoff only grows via consecutiveFailures incremented in the coRetry
    # path when the connection attempt itself fails.
    client.readyState = Connecting
    client.fireError("connection lost; reconnecting")
    let delay = client.reconnectDelay()
    await client.cancelAwareSleep(delay)

  # Final cleanup
  client.closeSocket()
  if client.readyState != Closed:
    client.readyState = Closed

# ---------------------------------------------------------------------------
# Public: Close
# ---------------------------------------------------------------------------

proc close*(client: AsyncSseClient) =
  ## Close the connection immediately (spec §6.5).
  ##
  ## Sets readyState to Closed and aborts any in-flight socket operation.
  ## No error event is fired. The `connect` future will complete shortly
  ## after this call.
  if client.readyState == Closed:
    return
  client.readyState = Closed
  client.closeSocket()

# ---------------------------------------------------------------------------
# Public: Accessors
# ---------------------------------------------------------------------------

proc lastEventId*(client: AsyncSseClient): string =
  ## The current last event ID (sent as Last-Event-ID on reconnection).
  client.parser.lastEventId

proc reconnectionTime*(client: AsyncSseClient): int =
  ## Current reconnection time in ms (may have been updated by server).
  client.parser.reconnectionTime
