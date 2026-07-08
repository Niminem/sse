## Async SSE client implementing the EventSource connection lifecycle.
##
## Manages the full state machine: connect → open → read → reconnect,
## with automatic exponential backoff, redirect following, and optional
## stall detection. Delivers events via callbacks set on the client object.
##
## Uses std/asyncdispatch + std/asyncnet for non-blocking I/O. The parser,
## HTTP, and shared client-logic layers are pure (no I/O) and imported
## from sibling modules (`parser`, `http`, `client_shared`).

import std/[asyncdispatch, asyncnet]
import types, parser, http, client_shared
when defined(ssl):
  import tls

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
                              ## Applies to the response body only; the
                              ## header-read phase has no deadline (but
                              ## remains cancellable).
    recvSize*: int            ## Socket recv buffer size hint in bytes.
    when defined(ssl):
      verifyHostname*: bool = true
                              ## Check that the server certificate matches
                              ## the request hostname (independent of the
                              ## chain verification configured on the
                              ## context). Disable together with a
                              ## CVerifyNone context (see the client's
                              ## `sslContext` field) to accept self-signed
                              ## certificates, e.g. in development.

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

    # -- TLS --
    when defined(ssl):
      sslContext*: SslContext ## Custom TLS context for https connections
                              ## (custom CA bundle, client certificates,
                              ## CVerifyNone, ...); set before `connect`.
                              ## nil uses a per-client default: CVerifyPeer
                              ## + system CA roots. A supplied context is
                              ## owned by the caller and never destroyed
                              ## by the client. Lives on the client rather
                              ## than the config because ref fields would
                              ## make the config object unusable in consts.

    # -- Private --
    parser: SseParser
    socket: AsyncSocket
    cancelToken: CancelToken
    consecutiveFailures: int
    body: BodyDecoder
    pendingBody: string       ## Body bytes that arrived with the response
                              ## headers; delivered only after the open
                              ## event fires (spec §6.2 orders the open
                              ## announcement before stream interpretation).
    when defined(ssl):
      defaultTlsCtx: SslContext ## Lazily-created default context, cached
                                ## so the CA store is scanned once per
                                ## client, not per reconnect attempt.
                                ## Lives as long as the client.

# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

proc newAsyncSseClient*(rawUrl: string;
                        config = DefaultConfig;
                        cancelToken: CancelToken = nil): AsyncSseClient =
  ## Create a new async SSE client.
  ##
  ## Validates the URL synchronously (spec §6.1 steps 1–3). Raises
  ## `ValueError` if the URL is invalid or uses an unsupported scheme,
  ## or if the URL is `https` and the library was built without TLS
  ## support (`-d:ssl`). No network activity occurs until `connect`
  ## is called.
  let parsedUrl = parseSseUrl(rawUrl)
  when not defined(ssl):
    if parsedUrl.useTls:
      raise newException(ValueError,
        "https URLs require building with -d:ssl: " & rawUrl)
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
  ## the origin into each event (spec §5.1 step 4). The isCancelled guard
  ## implements the delivery-time check of spec §5.1 step 6: a callback
  ## that calls `close()` mid-stream — or a cancel token firing — suppresses
  ## all later deliveries.
  let origin = effectiveUrl.origin
  client.parser.onEvent = proc(ev: SseEvent) =
    if client.isCancelled: return
    var event = ev
    event.origin = origin
    if client.onEvent != nil:
      client.onEvent(event)
  client.parser.onComment = proc(comment: string) =
    if client.isCancelled: return
    if client.onComment != nil:
      client.onComment(comment)

# ---------------------------------------------------------------------------
# Internal: Fire Lifecycle Events
# ---------------------------------------------------------------------------

proc fireOpen(client: AsyncSseClient) =
  if client.isCancelled: return
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

proc openSocket(client: AsyncSseClient;
                target: SseUrl): Future[ConnectOutcome] {.async.} =
  ## Open a TCP — and for https targets, TLS — connection to host:port.
  ##
  ## coSuccess: socket connected; for TLS, handshake completed and the
  ## certificate hostname verified. coRetry: transient network failure.
  ## coFatal: TLS configuration or verification failure — deterministic,
  ## so retrying cannot succeed; readyState is set to Closed and the
  ## error event fired here (as in attemptConnect's other fatal paths).
  when not defined(ssl):
    if target.useTls:
      # Only reachable via a cross-scheme redirect: the constructor
      # rejects https URLs outright in non-SSL builds.
      client.readyState = Closed
      client.fireError("https redirect target requires building with -d:ssl")
      return coFatal

  try:
    # Unbuffered is load-bearing: asyncnet's buffered recv(N) loops until
    # it accumulates all N bytes or the peer closes, which stalls event
    # delivery indefinitely on a live SSE stream that trickles data and
    # never closes. Unbuffered recv returns whatever the OS (or the TLS
    # record layer) has available, matching the spec's line-buffering
    # guidance (§3.2) for timely dispatch.
    client.socket = newAsyncSocket(buffered = false)
  except CatchableError:
    return coRetry

  when defined(ssl):
    if target.useTls:
      try:
        # Wrap before connect: asyncnet's connect then drives the
        # non-blocking handshake itself (setting SNI for non-IP hosts),
        # so handshake errors surface in the connect call below.
        wrapSocket(resolveTlsContext(client.sslContext,
                                     client.defaultTlsCtx),
                   client.socket)
      except CatchableError as e:
        # Context creation (missing CA bundle, OpenSSL not loadable) or
        # SSL_new failure. Deterministic; a retry loop never heals it.
        client.closeSocket()
        client.readyState = Closed
        client.fireError("TLS setup failed: " & e.msg)
        return coFatal

  try:
    await client.socket.connect(target.host, Port(target.port))
  except CatchableError as e:
    client.closeSocket()
    when defined(ssl):
      # TCP-level failures (OSError) are transient and retried; SslError
      # from the handshake (untrusted chain, protocol mismatch, bad
      # certificate) is deterministic and fatal.
      if e of SslError:
        client.readyState = Closed
        client.fireError("TLS handshake failed: " & e.msg)
        return coFatal
    return coRetry

  when defined(ssl):
    if target.useTls and client.config.verifyHostname:
      # std/asyncnet performs no certificate hostname check on any
      # platform; without this a valid certificate for any domain would
      # be accepted for every host. See sse/tls.
      try:
        verifyPeerHostname(client.socket.sslHandle, target.host)
      except SslError as e:
        client.closeSocket()
        client.readyState = Closed
        client.fireError("TLS certificate verification failed: " & e.msg)
        return coFatal

  return coSuccess

# ---------------------------------------------------------------------------
# Internal: Body Feeding
# ---------------------------------------------------------------------------

proc feedBody(client: AsyncSseClient; raw: string) =
  ## Decode (strip transfer framing) and feed bytes to the parser.
  let decoded = client.body.decode(raw)
  if decoded.len > 0:
    client.parser.feed(decoded)

proc isBodyFinished(client: AsyncSseClient): bool =
  client.body.isFinished

# ---------------------------------------------------------------------------
# Internal: Single Connection Attempt
# ---------------------------------------------------------------------------

proc attemptConnect(client: AsyncSseClient): Future[ConnectOutcome] {.async.} =
  ## Perform one connection attempt: open socket, send request, parse
  ## response headers, follow redirects, validate.
  ##
  ## On success: socket is open, transfer state initialized, any body
  ## bytes that arrived with the headers stashed in `pendingBody` for
  ## delivery after the open event fires.
  var currentUrl = client.url
  var redirectCount = 0

  while true:
    if client.isCancelled:
      return coFatal

    # -- Open socket (TCP + TLS handshake for https) --
    case await client.openSocket(currentUrl)
    of coRetry: return coRetry
    of coFatal: return coFatal
    of coSuccess: discard

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
      client.body = initBodyDecoder(resp)
      # Do NOT feed bodyRemainder here: the open event has not fired yet,
      # and spec §6.2 requires it to precede any message events.
      client.pendingBody = bodyRemainder
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
  ## Compute reconnection delay via the shared backoff formula (see
  ## `client_shared.backoffDelay`). Backoff only applies after consecutive
  ## failed connection attempts (coRetry); a clean end-of-body reconnects
  ## with the plain reconnectionTime.
  backoffDelay(client.parser.reconnectionTime,
               client.consecutiveFailures,
               client.config.maxReconnectDelay)

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
  ##
  ## Returns immediately if the client is already closed or its cancel
  ## token has fired: Closed is terminal (spec §6), so `close()` before
  ## `connect` prevents any network activity.
  if client.isCancelled:
    return
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

    # Deliver body bytes that arrived with the headers, now that the open
    # event has fired. Delivery is still guarded per event: if onOpen
    # called close() or fired the cancel token, wireParser's isCancelled
    # check drops these.
    if client.pendingBody.len > 0:
      let pending = client.pendingBody
      client.pendingBody = ""
      client.feedBody(pending)

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
