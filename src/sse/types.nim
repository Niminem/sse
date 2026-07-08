## Core public types shared by every module in the library: events,
## connection states, cancellation, and the callback signatures used
## for event delivery. Pure data definitions — no logic, no I/O.

import std/atomics

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
    ##
    ## The flag itself is atomic, so `cancel()` may be called from another
    ## thread (e.g. to cancel a sync client blocked in its read loop).
    ## Lifetime rule for cross-thread use: create the token *before*
    ## spawning the thread, keep a reference alive on the creating thread
    ## until all clients using it are done, and do not copy references
    ## between threads mid-flight (reference *counting* is not atomic
    ## under ORC).
    flag: Atomic[bool]

  SseEventHandler* = proc (event: SseEvent) {.closure, gcsafe.}
  SseCommentHandler* = proc (comment: string) {.closure, gcsafe.}
  SseNotifyHandler* = proc () {.closure, gcsafe.}
  SseErrorHandler* = proc (msg: string) {.closure, gcsafe.}

proc newCancelToken*(): CancelToken =
  ## Create a new cancel token in the non-cancelled state.
  CancelToken()

proc cancel*(token: CancelToken) =
  ## Signal cancellation. All clients sharing this token will observe
  ## cancellation at their next yield point (async) or poll (sync).
  ## Safe to call from another thread.
  token.flag.store(true)

proc cancelled*(token: CancelToken): bool =
  ## True once `cancel()` has been called. Safe to read from any thread.
  token.flag.load()
