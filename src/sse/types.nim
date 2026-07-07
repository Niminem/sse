type
  ReadyState* = enum
    Connecting = 0
    Open = 1
    Closed = 2

  SseEvent* = object
    eventType*: string    ## "message" if no event field was set
    data*: string         ## payload (trailing LF stripped)
    lastEventId*: string  ## persists across events until explicitly changed
    origin*: string       ## origin of the event stream's final URL

  CancelToken* = ref object
    ## Cooperative cancellation signal. Shared across one or more clients
    ## to cancel them all with a single call to `cancel()`.
    cancelled*: bool

  SseEventHandler* = proc (event: SseEvent) {.closure, gcsafe.}
  SseCommentHandler* = proc (comment: string) {.closure, gcsafe.}
  SseNotifyHandler* = proc () {.closure, gcsafe.}
  SseErrorHandler* = proc (msg: string) {.closure, gcsafe.}

proc newCancelToken*(): CancelToken =
  ## Create a new cancel token in the non-cancelled state.
  CancelToken(cancelled: false)

proc cancel*(token: CancelToken) =
  ## Signal cancellation. All clients sharing this token will observe
  ## cancellation at their next yield point.
  token.cancelled = true
