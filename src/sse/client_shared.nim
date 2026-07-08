## Shared, pure (no I/O) helpers for the SSE client implementations.
##
## Both the async and sync clients import this module for logic that is
## independent of the I/O model: response-body decoding across the three
## HTTP transfer modes, and the exponential-backoff delay formula.

import http

# ---------------------------------------------------------------------------
# Body Decoding
# ---------------------------------------------------------------------------

type
  BodyDecoder* = object
    ## Decodes raw response-body bytes according to the transfer mode
    ## negotiated in the response headers (chunked / content-length /
    ## identity). Initialize with `initBodyDecoder` once headers are
    ## validated, then pass every raw socket read through `decode` and
    ## check `isFinished` to detect end-of-body.
    mode: TransferMode
    chunked: ChunkedDecoder
    remaining: int

proc initBodyDecoder*(resp: HttpResponse): BodyDecoder =
  ## Build a decoder from validated response headers.
  result.mode = detectTransferMode(resp)
  if result.mode == tmChunked:
    result.chunked = initChunkedDecoder()
  result.remaining = contentLength(resp)

proc decode*(dec: var BodyDecoder; raw: string): string =
  ## Decode raw socket bytes, returning body bytes with any transfer
  ## framing stripped. May return an empty string mid-framing.
  case dec.mode
  of tmChunked:
    result = dec.chunked.feed(raw)
  of tmContentLength:
    let take = min(raw.len, dec.remaining)
    if take > 0:
      result = raw[0 ..< take]
      dec.remaining -= take
  of tmIdentity:
    result = raw

func isFinished*(dec: BodyDecoder): bool =
  ## True when the body is complete (terminal chunk seen, content-length
  ## exhausted, or the chunked decoder failed). Identity bodies never
  ## finish; they end when the connection closes.
  case dec.mode
  of tmChunked:
    dec.chunked.isFinished or dec.chunked.hasFailed
  of tmContentLength:
    dec.remaining <= 0
  of tmIdentity:
    false

# ---------------------------------------------------------------------------
# Reconnection Backoff
# ---------------------------------------------------------------------------

func backoffDelay*(base, failures, cap: int): int =
  ## Compute a reconnection delay with exponential backoff.
  ##
  ## When `failures` is 0 (e.g. after a clean end-of-body), returns the
  ## plain `base` reconnection time per spec §6.3 step 2. Backoff only
  ## grows with consecutive failed connection attempts.
  ## Formula: `min(base * 2^failures, cap)`, overflow-safe.
  if base <= 0:
    return base
  let shift = min(failures, 30)
  let multiplier = 1 shl shift
  if base > cap div multiplier:
    return cap
  let delay = base * multiplier
  if delay > cap:
    return cap
  return delay
