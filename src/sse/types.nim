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

  SseEventHandler* = proc (event: SseEvent) {.closure, gcsafe.}
  SseCommentHandler* = proc (comment: string) {.closure, gcsafe.}
  SseNotifyHandler* = proc () {.closure, gcsafe.}
  SseErrorHandler* = proc (msg: string) {.closure, gcsafe.}
