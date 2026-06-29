type
  SseEvent* = object
    data*: string
    eventType*: string
    id*: string
    retry*: int  ## Reconnection time in ms. -1 = not set by this event.

  ReadyState* = enum
    rsConnecting = 0
    rsOpen = 1
    rsClosed = 2

  SseError* = object of CatchableError
  SseLimitError* = object of SseError
  SseConnectionError* = object of SseError

  CancelToken* = ref object
    cancelled: bool

  SseParserConfig* = object
    maxLineLen*: int
    maxEventSize*: int

  EventSourceConfig* = object
    reconnectionTime*: int
    minReconnectTime*: int
    maxReconnectTime*: int
    maxReconnectAttempts*: int
    inactivityTimeout*: int
    stripCrossOriginHeaders*: bool
    cancelToken*: CancelToken
    parserConfig*: SseParserConfig

  SseServerConfig* = object
    heartbeatInterval*: int

const
  MaxLineLen* = 65_536 ## 64 KiB
  MaxEventSize* = 1_048_576 ## 1 MiB

proc initSseEvent*(): SseEvent =
  SseEvent(data: "", eventType: "", id: "", retry: -1)

proc newCancelToken*(): CancelToken =
  result = CancelToken(cancelled: false)

proc cancel*(token: CancelToken) =
  token.cancelled = true

proc isCancelled*(token: CancelToken): bool =
  token.cancelled

proc initSseParserConfig*(maxLineLen = MaxLineLen,
                          maxEventSize = MaxEventSize): SseParserConfig =
  result = SseParserConfig(maxLineLen: maxLineLen, maxEventSize: maxEventSize)

proc initEventSourceConfig*(reconnectionTime = 3000,
                            minReconnectTime = 1000,
                            maxReconnectTime = 60_000,
                            maxReconnectAttempts = 0,
                            inactivityTimeout = 0,
                            stripCrossOriginHeaders = true,
                            cancelToken: CancelToken = nil,
                            parserConfig = initSseParserConfig()): EventSourceConfig =
  result = EventSourceConfig(
    reconnectionTime: reconnectionTime,
    minReconnectTime: minReconnectTime,
    maxReconnectTime: maxReconnectTime,
    maxReconnectAttempts: maxReconnectAttempts,
    inactivityTimeout: inactivityTimeout,
    stripCrossOriginHeaders: stripCrossOriginHeaders,
    cancelToken: cancelToken,
    parserConfig: parserConfig
  )

proc initSseServerConfig*(heartbeatInterval = 15_000): SseServerConfig =
  result = SseServerConfig(heartbeatInterval: heartbeatInterval)
