## Sync (blocking) SSE client implementing the EventSource connection
## lifecycle.
##
## Manages the full state machine: connect → open → read → reconnect,
## with automatic exponential backoff, redirect following, and optional
## stall detection. Delivers events via callbacks set on the client
## object. Structural mirror of `client_async`; the parser, HTTP, and
## shared client-logic layers are pure (no I/O) and imported from
## sibling modules (`parser`, `http`, `client_shared`).
##
## Uses std/net blocking sockets. `connect` blocks the calling thread
## for the entire connection lifecycle; callbacks fire on that thread.
## Cancellation is cooperative: every socket read uses a short timeout
## (`pollInterval`) so the loop can observe `close()` (same thread, from
## a callback) or a `CancelToken` (from another thread) without staying
## blocked in `recv`.

import std/[net, os]
import types, parser, http, client_shared
when defined(ssl):
  import tls

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

type
  SyncSseClientConfig* = object
    ## Configuration for the sync SSE client.
    autoReconnect*: bool      ## Reconnect on connection loss (spec-compliant).
                              ## Set false for single-shot streams.
    maxRedirects*: int        ## Maximum redirects per connection attempt.
    maxReconnectDelay*: int   ## Backoff cap in milliseconds.
    stallTimeout*: int        ## Inactivity timeout in ms (0 = disabled).
                              ## Applies to the response body only; the
                              ## header-read phase has no deadline (but
                              ## remains cancellable).
    recvSize*: int            ## Socket recv buffer size hint in bytes.
    pollInterval*: int        ## Per-recv timeout in ms; bounds how quickly
                              ## cancellation is observed while blocked.
    connectTimeout*: int      ## TCP connect timeout in ms (0 = OS default).
                              ## Covers the TCP connect only; the blocking
                              ## TLS handshake that follows has no timeout
                              ## and is not cancellable mid-attempt
                              ## (std/net exposes none).
    httpMethod*: HttpMethod = HttpGet
                              ## Request method. The EventSource spec only
                              ## uses GET; POST etc. support non-spec
                              ## endpoints that stream SSE in response to
                              ## a custom request (e.g. LLM APIs).
    extraHeaders*: seq[tuple[key, val: string]]
                              ## Additional request headers, sent verbatim
                              ## on every connection attempt (including
                              ## reconnects and redirect hops). Names that
                              ## collide with the client's own headers
                              ## (Host, Accept, Cache-Control,
                              ## Last-Event-ID, Content-Length) are
                              ## rejected at construction.
    body*: string             ## Request body, re-sent on every connection
                              ## attempt. Non-empty adds a Content-Length
                              ## header; supply Content-Type via
                              ## `extraHeaders`. Rejected at construction
                              ## for GET/HEAD. For one-shot request/stream
                              ## endpoints (LLM APIs), also set
                              ## `autoReconnect: false` — reconnecting
                              ## re-sends the body and re-triggers the
                              ## work server-side.
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
  DefaultSyncConfig* = SyncSseClientConfig(
    autoReconnect: true,
    maxRedirects: 10,
    maxReconnectDelay: 60_000,
    stallTimeout: 0,
    recvSize: 4096,
    pollInterval: 250,
    connectTimeout: 30_000,
  )

# ---------------------------------------------------------------------------
# Client Type
# ---------------------------------------------------------------------------

type
  ConnectOutcome = enum
    coSuccess  ## 200 + text/event-stream; body ready to stream.
    coRetry    ## Non-fatal failure; should reconnect.
    coFatal    ## Fatal failure; connection is closed permanently.

  RecvOutcome = enum
    roData       ## Bytes received.
    roClosed     ## Connection closed or socket error (end-of-body).
    roStalled    ## No bytes within the stall timeout.
    roCancelled  ## close() or cancel token observed.

  SyncSseClient* = ref object
    ## A blocking SSE connection with automatic reconnection.
    ##
    ## Set callbacks (`onEvent`, `onOpen`, `onError`, `onComment`,
    ## `onHttpResponse`) before calling `connect`. The `connect` call
    ## blocks until `close()` is called, the cancel token fires, or a
    ## fatal (non-recoverable) error occurs.

    # -- Public state --
    readyState*: ReadyState
    url*: SseUrl              ## Reconnection URL (updated only by permanent redirects).
    config*: SyncSseClientConfig

    # -- Callbacks --
    onEvent*: SseEventHandler
    onComment*: SseCommentHandler
    onOpen*: SseNotifyHandler
    onError*: SseErrorHandler
    onHttpResponse*: SseHttpResponseHandler
                              ## Fired once per HTTP response received,
                              ## with the parsed status and headers,
                              ## before the response is validated or its
                              ## body is consumed. Fires for every
                              ## response the server sends: 200s, error
                              ## statuses (e.g. a 429 whose Retry-After
                              ## header is otherwise invisible), each
                              ## redirect hop, and every reconnection
                              ## attempt. Observation-only — it cannot
                              ## alter how the response is handled — but
                              ## calling `close()` (or firing the cancel
                              ## token) from inside it aborts the
                              ## connection quietly: no open or error
                              ## event follows.

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
    socket: Socket
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

proc newSyncSseClient*(rawUrl: string;
                       config = DefaultSyncConfig;
                       cancelToken: CancelToken = nil): SyncSseClient =
  ## Create a new sync SSE client.
  ##
  ## Validates the URL synchronously (spec §6.1 steps 1–3). Raises
  ## `ValueError` if the URL is invalid or uses an unsupported scheme,
  ## or if the URL is `https` and the library was built without TLS
  ## support (`-d:ssl`). Also validates the custom request options
  ## (`httpMethod`/`extraHeaders`/`body`, see `validateRequest`).
  ## No network activity occurs until `connect` is called.
  let parsedUrl = parseSseUrl(rawUrl)
  validateRequest(config.httpMethod, config.extraHeaders, config.body)
  when not defined(ssl):
    if parsedUrl.useTls:
      raise newException(ValueError,
        "https URLs require building with -d:ssl: " & rawUrl)
  result = SyncSseClient(
    readyState: Connecting,
    url: parsedUrl,
    config: config,
    cancelToken: cancelToken,
    parser: initSseParser(onEvent = nil),
  )

# ---------------------------------------------------------------------------
# Internal: Cancellation Check
# ---------------------------------------------------------------------------

proc isCancelled(client: SyncSseClient): bool =
  client.readyState == Closed or
    (client.cancelToken != nil and client.cancelToken.cancelled)

# ---------------------------------------------------------------------------
# Internal: Callback Wiring
# ---------------------------------------------------------------------------

proc wireParser(client: SyncSseClient; effectiveUrl: SseUrl) =
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

proc fireOpen(client: SyncSseClient) =
  if client.isCancelled: return
  client.readyState = Open
  client.consecutiveFailures = 0
  if client.onOpen != nil:
    client.onOpen()

proc fireError(client: SyncSseClient; msg: string) =
  ## Fire the error callback unconditionally (spec §6.4 fires error after
  ## setting CLOSED). No readyState guard here — close() never calls this
  ## proc, and all internal callers are protected by isCancelled checks.
  if client.onError != nil:
    client.onError(msg)

# ---------------------------------------------------------------------------
# Internal: Socket Lifecycle
# ---------------------------------------------------------------------------

proc closeSocket(client: SyncSseClient) =
  if client.socket != nil:
    client.socket.close()
    client.socket = nil

proc openSocket(client: SyncSseClient; target: SseUrl): ConnectOutcome =
  ## Open a TCP — and for https targets, TLS — connection to host:port.
  ##
  ## coSuccess: socket connected; for TLS, handshake completed and the
  ## certificate hostname verified. coRetry: transient network failure
  ## or connect timeout. coFatal: TLS configuration or verification
  ## failure — deterministic, so retrying cannot succeed; readyState is
  ## set to Closed and the error event fired here (as in attemptConnect's
  ## other fatal paths).
  ##
  ## Note: a blocking connect is not cancellable mid-attempt; the
  ## `connectTimeout` bounds how long the TCP connect can hold up the
  ## loop. The TLS handshake that follows has no timeout (std/net
  ## exposes none) and can block until the OS gives up on the peer.
  try:
    client.socket = newSocket()
    if client.config.connectTimeout > 0:
      client.socket.connect(target.host, Port(target.port),
                            client.config.connectTimeout)
    else:
      client.socket.connect(target.host, Port(target.port))
  except CatchableError:
    client.closeSocket()
    return coRetry

  when defined(ssl):
    if target.useTls:
      var ctx: SslContext
      try:
        ctx = resolveTlsContext(client.sslContext,
                                client.defaultTlsCtx)
      except CatchableError as e:
        # Context creation (missing CA bundle, OpenSSL not loadable).
        # Deterministic; a retry loop never heals it.
        client.closeSocket()
        client.readyState = Closed
        client.fireError("TLS setup failed: " & e.msg)
        return coFatal
      try:
        tlsHandshake(client.socket, ctx, target.host)
        if client.config.verifyHostname:
          # std/net's own hostname check is compiled out on Windows;
          # ours runs unconditionally on every platform. See sse/tls.
          verifyPeerHostname(client.socket.sslHandle, target.host)
      except SslError as e:
        # Untrusted chain, protocol mismatch, or certificate/hostname
        # mismatch: deterministic and fatal.
        client.closeSocket()
        client.readyState = Closed
        client.fireError("TLS handshake failed: " & e.msg)
        return coFatal
      except CatchableError:
        # Transport-level failure mid-handshake (reset, close).
        client.closeSocket()
        return coRetry

  coSuccess

# ---------------------------------------------------------------------------
# Internal: Polling Receive
# ---------------------------------------------------------------------------

proc pollRecv(client: SyncSseClient; stallTimeout: int;
              data: var string): RecvOutcome =
  ## Blocking recv that wakes every `pollInterval` to check cancellation.
  ##
  ## `stallTimeout` > 0 bounds the total time spent waiting for bytes;
  ## exceeding it returns `roStalled`. 0 waits indefinitely (until data,
  ## close, or cancellation).
  ##
  ## Waits for a single byte with the poll timeout, then drains the
  ## socket's internal buffer non-blockingly. A larger recv-with-timeout
  ## cannot be used here: std/net's timeout recv loops trying to fill the
  ## full requested size and raises TimeoutError on a partial read,
  ## silently discarding the bytes already consumed — which would lose
  ## data whenever the server pauses mid-stream.
  let interval = max(client.config.pollInterval, 1)
  var idleMs = 0
  while true:
    if client.isCancelled:
      return roCancelled
    var first: string
    try:
      first = client.socket.recv(1, timeout = interval)
    except TimeoutError:
      idleMs += interval
      if stallTimeout > 0 and idleMs >= stallTimeout:
        return roStalled
      continue
    except CatchableError:
      return roClosed
    if first.len == 0:
      return roClosed
    data = first
    # Drain already-buffered bytes without blocking. `hasDataBuffered`
    # guarantees the low-level pointer recv is served straight from the
    # socket's internal buffer (a memcpy — no syscall, no per-byte string
    # allocation). Anything beyond the buffer is picked up instantly on
    # the next pollRecv call. The pointer recv reports errors via its
    # return value; on anything but 1, deliver what we have and let the
    # next call surface the failure.
    var c: char
    while data.len < client.config.recvSize and
          client.socket.hasDataBuffered:
      if client.socket.recv(addr c, 1) != 1:
        break
      data.add(c)
    return roData

# ---------------------------------------------------------------------------
# Internal: Body Feeding
# ---------------------------------------------------------------------------

proc feedBody(client: SyncSseClient; raw: string) =
  ## Decode (strip transfer framing) and feed bytes to the parser.
  let decoded = client.body.decode(raw)
  if decoded.len > 0:
    client.parser.feed(decoded)

proc isBodyFinished(client: SyncSseClient): bool =
  client.body.isFinished

# ---------------------------------------------------------------------------
# Internal: Single Connection Attempt
# ---------------------------------------------------------------------------

proc attemptConnect(client: SyncSseClient): ConnectOutcome =
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
    case client.openSocket(currentUrl)
    of coRetry: return coRetry
    of coFatal: return coFatal
    of coSuccess: discard

    if client.isCancelled:
      client.closeSocket()
      return coFatal

    # -- Send request --
    let lastId = client.parser.lastEventId
    let request = buildRequest(currentUrl, lastId,
                               client.config.httpMethod,
                               client.config.extraHeaders,
                               client.config.body)
    try:
      client.socket.send(request)
    except CatchableError:
      client.closeSocket()
      return coRetry

    if client.isCancelled:
      client.closeSocket()
      return coFatal

    # -- Read response headers --
    # No stall timeout here (parity with the async client, which applies
    # stallTimeout only to the body); cancellation is still observed at
    # every pollInterval.
    var hp = initHeaderParser()
    var bodyRemainder = ""
    while not hp.isComplete and not hp.hasFailed:
      var data: string
      case client.pollRecv(0, data)
      of roData:
        bodyRemainder = hp.feed(data)
      of roCancelled:
        client.closeSocket()
        return coFatal
      of roClosed, roStalled:
        client.closeSocket()
        return coRetry

    if hp.hasFailed:
      client.closeSocket()
      return coRetry

    var resp = hp.parse()
    if hp.hasFailed:
      client.closeSocket()
      return coRetry

    # -- Response hook --
    # Fired before validation so the handler sees every response the
    # server actually sent — 200s, error statuses, and each redirect hop
    # — not just the ones that validate as an SSE stream.
    if client.onHttpResponse != nil:
      client.onHttpResponse(resp)
      if client.isCancelled:
        # The handler closed the client (or the token fired): tear down
        # quietly, before validation can fire open/error events.
        client.closeSocket()
        return coFatal

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

proc readBody(client: SyncSseClient) =
  ## Read the response body, feeding chunks to the parser.
  ## Returns when: connection closes, body finishes, stall timeout
  ## fires, or client is cancelled. Calls `parser.complete()` on all
  ## non-cancel exit paths to flush any pending CR line ending and
  ## discard incomplete events per spec §3.5.
  while true:
    if client.isCancelled:
      return
    if client.isBodyFinished:
      client.parser.complete()
      return

    var data: string
    case client.pollRecv(client.config.stallTimeout, data)
    of roData:
      client.feedBody(data)
    of roCancelled:
      return
    of roClosed, roStalled:
      client.parser.complete()
      return

# ---------------------------------------------------------------------------
# Internal: Backoff Delay
# ---------------------------------------------------------------------------

proc reconnectDelay(client: SyncSseClient): int =
  ## Compute reconnection delay via the shared backoff formula (see
  ## `client_shared.backoffDelay`). Backoff only applies after consecutive
  ## failed connection attempts (coRetry); a clean end-of-body reconnects
  ## with the plain reconnectionTime.
  backoffDelay(client.parser.reconnectionTime,
               client.consecutiveFailures,
               client.config.maxReconnectDelay)

proc cancelAwareSleep(client: SyncSseClient; ms: int) =
  ## Sleep for `ms` milliseconds, but return early if cancelled.
  ## Wakes every `pollInterval` to check the flag.
  let interval = max(client.config.pollInterval, 1)
  var remaining = ms
  while remaining > 0:
    if client.isCancelled: return
    let chunk = min(remaining, interval)
    sleep(chunk)
    remaining -= chunk

# ---------------------------------------------------------------------------
# Public: Connect (Main Loop)
# ---------------------------------------------------------------------------

proc connect*(client: SyncSseClient) =
  ## Begin the SSE connection lifecycle. Blocking.
  ##
  ## This call runs until `close()` is called (from a callback), the
  ## cancel token fires (possibly from another thread), or a fatal error
  ## occurs. It internally handles reconnection with exponential backoff
  ## when `autoReconnect` is true. Set all callbacks before calling this.
  ##
  ## Returns immediately if the client is already closed or its cancel
  ## token has fired: Closed is terminal (spec §6), so `close()` before
  ## `connect` prevents any network activity.
  if client.isCancelled:
    return
  client.readyState = Connecting

  while not client.isCancelled:
    let outcome = client.attemptConnect()

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
      client.cancelAwareSleep(client.reconnectDelay())
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
    client.readBody()

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
    client.cancelAwareSleep(client.reconnectDelay())

  # Final cleanup
  client.closeSocket()
  if client.readyState != Closed:
    client.readyState = Closed

# ---------------------------------------------------------------------------
# Public: Close
# ---------------------------------------------------------------------------

proc close*(client: SyncSseClient) =
  ## Close the connection (spec §6.5). No error event is fired.
  ##
  ## Sets readyState to Closed; the blocking `connect` loop observes it
  ## within one `pollInterval` and tears down the socket on its own
  ## thread. Call this from the client's thread only — typically from
  ## inside a callback while `connect` is running, or before/after
  ## `connect`. For cross-thread cancellation, use a `CancelToken`
  ## instead (its flag is atomic; readyState is not).
  if client.readyState == Closed:
    return
  client.readyState = Closed

# ---------------------------------------------------------------------------
# Public: Accessors
# ---------------------------------------------------------------------------

proc lastEventId*(client: SyncSseClient): string =
  ## The current last event ID (sent as Last-Event-ID on reconnection).
  client.parser.lastEventId

proc reconnectionTime*(client: SyncSseClient): int =
  ## Current reconnection time in ms (may have been updated by server).
  client.parser.reconnectionTime
