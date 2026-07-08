## TLS test suite for Phase 8: `sse/tls` plus the https paths of both
## clients (`client_async`, `client_sync`).
##
## Structure mirrors the plaintext client suites: threaded loopback
## servers, but wrapped in TLS via std/net server-side handshakes.
##
## Certificates: two self-signed test certificates (100-year validity)
## are embedded below as PEM constants and written to a temp directory
## at startup:
## - localhost cert:  SAN `DNS:localhost` — the "right" certificate.
## - wronghost cert:  SAN `DNS:wronghost.invalid` — trustable chain,
##   wrong name; used to prove the hostname check rejects it.
##
## Clients connect to `https://localhost:PORT`, NOT `https://127.0.0.1`:
## `verifyPeerHostname` skips IP-literal targets by design, so an
## IP-based URL would silently bypass the very check under test. The
## client sockets are AF_INET, so "localhost" always resolves to
## 127.0.0.1 (where the loopback servers bind) on every platform.
##
## Regenerating the certificates (should not be needed for ~100 years):
##   openssl req -x509 -newkey rsa:2048 -sha256 -days 36500 -nodes \
##     -keyout localhost.key -out localhost.pem \
##     -subj "/CN=sse-test-localhost" -addext "subjectAltName=DNS:localhost"
##   openssl req -x509 -newkey rsa:2048 -sha256 -days 36500 -nodes \
##     -keyout wronghost.key -out wronghost.pem \
##     -subj "/CN=sse-test-wronghost" -addext "subjectAltName=DNS:wronghost.invalid"

when not defined(ssl):
  {.error: "tests/t_tls requires building with -d:ssl (see tests/config.nims)".}

import std/[unittest, net, os, strutils, asyncdispatch]
from std/openssl import SslPtr
import sse/[types, http, client_async, client_sync, tls]

# ===========================================================================
# Certificate Fixtures
# ===========================================================================

const LocalhostCertPem = """
-----BEGIN CERTIFICATE-----
MIIDMzCCAhugAwIBAgIUUQ3S+OhxJKx2BB/CORX77Z40an0wDQYJKoZIhvcNAQEL
BQAwHTEbMBkGA1UEAwwSc3NlLXRlc3QtbG9jYWxob3N0MCAXDTI2MDcwODIwNDY1
NloYDzIxMjYwNjE0MjA0NjU2WjAdMRswGQYDVQQDDBJzc2UtdGVzdC1sb2NhbGhv
c3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDTnIXbq91ehSE5z10/
IBEI0Y3jMCov7XnetYXencG7RsKm+AoxTzDoa1IHmzfJ/zz20Sif31HcKfLuQ1PH
+xFD708a9AaQRmxpLsrRwhlT2AP0Y8xa0vYMO3R2J7gsozh2Wp6eODYBUQOL4V8F
y/teYL6PqZmP9Ex2/PQOBFE1zOHbgVV8Q596D7EXAwoRP1+za5ud5WhmyfXLQjct
Yg7fkNPAwje/lMmwilaGG8o33hFPARgrchkEBixzQTAld8mYf/YzBSmdWZMitZR8
eB1OIbAOS4rLdxP6YvbO/wCT6cyS41NolChfJaz1xvYVf1On39rPbNfjxk6JprA7
TAXZAgMBAAGjaTBnMB0GA1UdDgQWBBShanQfBfpo/pOw2uI0DDEy/n3BejAfBgNV
HSMEGDAWgBShanQfBfpo/pOw2uI0DDEy/n3BejAPBgNVHRMBAf8EBTADAQH/MBQG
A1UdEQQNMAuCCWxvY2FsaG9zdDANBgkqhkiG9w0BAQsFAAOCAQEAkSAE0qNmyL0R
1n6noxfakhs4LMBkX4L7/wVEc1rWp0FYsZE4XsydphUMFLAf6Za98bxwW5/cw8gi
cb8xabzLgihTe3iSWpWE5HLKz4cQsTKZA1nSqQn1cCFwAb0Nh9vuQOjzHUyB7eRB
s7zpufZE2F8x2myfozeQXYEVtW9yuVvxy1hp1apkDXlLYw6Rk59FRb7+xIruh5Hx
nNmzBSS2IpPSZGXNst1RovXE7jvuas/LMM2A+pF5cn5HJeAbFkOkHgPN0VEW01dN
weRnevSYIkNUJpcDTdW7hK4pxozJPHDZpsZVUsgkD0LziQChFVpi/dNnChbiYaBr
0DfvgMsCig==
-----END CERTIFICATE-----
"""

const LocalhostKeyPem = """
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDTnIXbq91ehSE5
z10/IBEI0Y3jMCov7XnetYXencG7RsKm+AoxTzDoa1IHmzfJ/zz20Sif31HcKfLu
Q1PH+xFD708a9AaQRmxpLsrRwhlT2AP0Y8xa0vYMO3R2J7gsozh2Wp6eODYBUQOL
4V8Fy/teYL6PqZmP9Ex2/PQOBFE1zOHbgVV8Q596D7EXAwoRP1+za5ud5WhmyfXL
QjctYg7fkNPAwje/lMmwilaGG8o33hFPARgrchkEBixzQTAld8mYf/YzBSmdWZMi
tZR8eB1OIbAOS4rLdxP6YvbO/wCT6cyS41NolChfJaz1xvYVf1On39rPbNfjxk6J
prA7TAXZAgMBAAECggEAEmbDESYx2t1S1Vcd8bZVJOIsJ3xxvrc06NP6LzCivylZ
FfHt2Pnc8+kZOgYaZNtoLsJjePuPf8i6ElQSfgpsJu8xftOHmpY1KTRjeLgbQbpA
Jclj5OYBdXDaIMg1cNREva4FqxaSQ60K1OglPjjNxBg/mfiSnP0OhS5b/rgLY9Uj
rqRu6sxAuu2Hv3Q+V0jsMhlylk6HuOvhtOYfkHtPLds3Ew0ICQcOyjJBd4rN4fr5
0OnJLqgZ93YCh04YdGZVDMjox+FZ/7s0i8Jrhk2o94dXP5kZqkgNmeOQ301JPgdx
FqjF/knc3fJFMhnV52N4wnJNXizBjdFTPs8vbJBQHQKBgQDy280MIuOc3HZJ8qjE
U6Z8LjV1teGa1+Pw7ihZcdVfPwr7+FZdUb+u2Pjah/431G4OCVHAmzi9NImZaIFI
8av3VO4hIChGYadKGSPGoHIUT6aMP2k5RsUb95Xvo4emYzyWJcHsi52jNKqKakaS
19ofnKqHsNQyE1R+A+OeFs5I/wKBgQDfD97M/+5tpvnQ7f/m+vJCHwi/dFcVaQDx
YNCJHusrGumdGu9Up3X43vpjVy4VVyLCXhgucQlmnGnX2HS3sxCHa/RoG1jIaYF9
U3b5WsesfrbZSdCbqhTX5UflN1FqTWUQx1Yb1/u5U6qncm9F2lrakQXOt+c5qmcT
Zw/rNUIZJwKBgFYa4ERhSlofQEFPq86P6gM1TvcBGZAQ41xU1DGCTqiKbZaQoe5M
Qa34d6LjfJrG0o5fd4DWh/NtYSvnBY+bP+FdV2CfSihKD0oSR8DwugHBi8dF5ETO
dqxHx/1C+aFgpeaGVI9NoQJwddbFf2go58k1frDFXTaz680SC+3NIC55AoGAVG4H
YQTGVI7NI/3RzLXTmJ9yijUY+SujhbmldSFU1h37qtGVIS+5gZe0ooFPGhi/OV6w
PHPgoQw3K9Hsa9PFP9zmx7kCI+l7b9p1v/BSf8H+p8Q+x4zbEtTgH6yOjnP4+x7Y
QvW/e+hlQbgY0hHSLcSStCn7xWHwXfG2nsqr2CUCgYAQQzw85GnfekIroVvdODUm
yNQ6yoFHP3NOU1+TrQIYAxPhsYs+W27L/LoAhN/Q+qc1xu9IhgIsFiD8LUiQBBJC
zpg6DGOZiBPdS2aB7y+yeBLQlGQsVWHyXC2WiafuGC+ARjPez/DfRAlFOX3jLAUT
ssBXvShW6iFbtJPApbvF8g==
-----END PRIVATE KEY-----
"""

const WronghostCertPem = """
-----BEGIN CERTIFICATE-----
MIIDOzCCAiOgAwIBAgIUMt1rjoJxUcPtOF3on6goGRaC8rMwDQYJKoZIhvcNAQEL
BQAwHTEbMBkGA1UEAwwSc3NlLXRlc3Qtd3Jvbmdob3N0MCAXDTI2MDcwODIwNDY1
NloYDzIxMjYwNjE0MjA0NjU2WjAdMRswGQYDVQQDDBJzc2UtdGVzdC13cm9uZ2hv
c3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDAlORqdeEf4KOz3KYx
5y8Bd32HkAo+y3SqHjx1WK/WwF0bHW98dBNl+Nq3cfQrx17sxSEQ5itMdvWhGQ5i
racQhyC20yDQstxmo+OKX6mafUiBO5y6gHqL4/3kmf/lMh9oNUeLmG2Y6BpfyT3Q
YrGGVVXFwLHNGOPpZlHawAD68493TM1FG2+0PSEnoxmPXB5IbZPHwVe6V/R7Cyir
LfOp+CtoeCTlOdHS/qZVE5D28twN42002GsTzn8GrrUAA5pdgr/0ILf2go+2Bi7T
S+fbvm4GZyIgL5hpJ/C0X4ZMgtCpE3gjWzCvPQHs8+CgBYZFtjyu/1ciK+XW1gq9
x5GfAgMBAAGjcTBvMB0GA1UdDgQWBBSRNoAQnj6hDVVsJiRWhBu2TLrYlzAfBgNV
HSMEGDAWgBSRNoAQnj6hDVVsJiRWhBu2TLrYlzAPBgNVHRMBAf8EBTADAQH/MBwG
A1UdEQQVMBOCEXdyb25naG9zdC5pbnZhbGlkMA0GCSqGSIb3DQEBCwUAA4IBAQB1
okxs5rJlL/k+eP6LORmW/fd9XQRSJBTb+ZcCs7VP3Nf4tedN4xWtPUlIMaIhxvmU
TAnr0mfi9d3SFMNsW0p08fFwKsKZ7AYtNH9tKNmBJ4KemSEvQ2TlZRGPlPO3W8SZ
2Qz9J1Qo+FsU7cw4LRUdB8RFJ3a6bm2cexytUX/KH8YQ6kTvkEFCNPKoIEsDx00l
aYSYRusiPffxlUad6BVr05rSRonyGtwgN++xd/xxPcwni8jAvk7OgyhzY385RISj
cB41b0nZVgexIq5yGpfClyEkvQyuQuMu5x1Yq7l68Dqx3G7aaVJQyupIucEX5Glx
cFOqk/cvts9+s5+ej5Sf
-----END CERTIFICATE-----
"""

const WronghostKeyPem = """
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDAlORqdeEf4KOz
3KYx5y8Bd32HkAo+y3SqHjx1WK/WwF0bHW98dBNl+Nq3cfQrx17sxSEQ5itMdvWh
GQ5iracQhyC20yDQstxmo+OKX6mafUiBO5y6gHqL4/3kmf/lMh9oNUeLmG2Y6Bpf
yT3QYrGGVVXFwLHNGOPpZlHawAD68493TM1FG2+0PSEnoxmPXB5IbZPHwVe6V/R7
CyirLfOp+CtoeCTlOdHS/qZVE5D28twN42002GsTzn8GrrUAA5pdgr/0ILf2go+2
Bi7TS+fbvm4GZyIgL5hpJ/C0X4ZMgtCpE3gjWzCvPQHs8+CgBYZFtjyu/1ciK+XW
1gq9x5GfAgMBAAECggEBAKePZQZ0NkKENCtEqp305mNCUkhnPWLRp1p1IGmBls0l
Zl03SU9ht5cb5UGDts14lNEx28lWWwoW3zUiFfAp45hv2jj4ji7H0RjFNC+reGhg
5Xr8ISvsqbOZ1cjXbV1ANS94XB3LrGBEYt5AvLXZiVi2WwDK3mjzZSooysOoF7JA
QUJlrS7uEAV7ZI8OrHa7MqR/HpBT1nxH+Pj1CJ0Jsj+bfvFNT63Oyz50wHFO/T8R
uUOtKt8hNO7kNg965RvBSaclu7Lio8YctI/I2+qjBbHRqEMUvs0/sBxkCsGrubvR
KWe9UxWstrhfg9WKqxJ38/AkYEloHNxxOx5GftfTZLECgYEA8OU6LbnEJGKnN/Is
XwnAMQaS0+JsDwQnORN/RS8eqttJtQ5R9Du3KEyu89xtwbr+5t+3h4jHzoim41N/
t+vuU4/eO9epoWqzBjQSomlIeJOhKQRr1JYL6iUoFNfmy/n96ve4bGORWBZ9OIMt
N7Lqg2xZzBCrrg6e0W72hcX58pUCgYEAzKgl26yDRN+y3jdvAkH7GREuMhDxOjys
9bEhFd/XgpccllBTNNLI4dXUS3tjisTla1QfrJ5IIyWtkgo4umgSIKrljlp7Rz6r
h0K7nqTLbmXh8ev80iQwkYYFcpV2rJ9ZLEyuD9too0P8h1maQ9XqysiopIboZW9l
kvnPdb9FOmMCgYA9ut/vu7zRfh5PrdVE/WCsr3yMo2cBahULAT4J4os/STZYGTVT
GsJSp2PlxcSyclLDouyK5Lge6tGpS0pdPS4zZ5iRSTCE1qzNmCZ2S/hwPZR8yE9B
iLnA5Iii3rib6JHy1kwCKnmiUxD/nE3ICiciSm+wZU05KzHzhTXTDeqBCQKBgFcY
3Hu/0GHYXfQqXUj1sZo6cJGIGlFxjx5E0tLPX5VJIDZsWuzpI+rQqLey/GsLXqOt
uokpF4Q1vcegKAJU1honAOjzYvGwHynCYbyjX5xNKbped0srcawNAHaCW24dpsKu
o4lGFCMfhPJJVNi3ymOgv/y9GVxJ602AmSUAUMMHAoGAED3HK8LUgzD0lHiunmGi
G0L11fHEJTH7HwRIVmgCV+babn6xc5WSnWAsHSFRlhuVwUeT/PQRw4dT98W73Q0I
ZerB9+zaTHE2zJ+0BdirqIMGmsyveVILW1b4xKLlLG8p7xzwN8E/DCYMho2nJLLf
JtOwsHz5Qsn1mdYCcjIGp0o=
-----END PRIVATE KEY-----
"""

# Written once at startup; removed at the end of the module.
let certDir = getTempDir() / "sse_t_tls_" & $getCurrentProcessId()
createDir(certDir)
let localhostCert = certDir / "localhost.pem"
let localhostKey = certDir / "localhost.key"
let wronghostCert = certDir / "wronghost.pem"
let wronghostKey = certDir / "wronghost.key"
writeFile(localhostCert, LocalhostCertPem)
writeFile(localhostKey, LocalhostKeyPem)
writeFile(wronghostCert, WronghostCertPem)
writeFile(wronghostKey, WronghostKeyPem)

# ===========================================================================
# 1. Unit — resolveTlsContext
# ===========================================================================

suite "resolveTlsContext":

  test "configured context wins; cache stays untouched":
    let configured = newContext(verifyMode = CVerifyNone)
    var cache: SslContext = nil
    check resolveTlsContext(configured, cache) == configured
    check cache == nil

  test "existing cache reused when no context configured":
    var cache = newContext(verifyMode = CVerifyNone)
    let before = cache
    check resolveTlsContext(nil, cache) == before
    check cache == before

  test "default context lazily created once and cached":
    # Requires a loadable system CA store — the same requirement the
    # library's default https path has.
    var cache: SslContext = nil
    let first = resolveTlsContext(nil, cache)
    check first != nil
    check cache == first
    check resolveTlsContext(nil, cache) == first

# ===========================================================================
# 2. Unit — verifyPeerHostname early exits
# ===========================================================================

suite "verifyPeerHostname early exits":

  test "empty hostname and IP literals skip the check entirely":
    # These return before touching the SSL handle, so nil is safe. Any
    # regression that removes the early return would crash here.
    verifyPeerHostname(SslPtr(nil), "")
    verifyPeerHostname(SslPtr(nil), "127.0.0.1")
    verifyPeerHostname(SslPtr(nil), "::1")
    check true

# ===========================================================================
# Test Infrastructure — TLS Loopback Servers
# ===========================================================================

proc sseResponse(body: string; contentType = "text/event-stream"): string =
  "HTTP/1.1 200 OK\r\n" &
  "Content-Type: " & contentType & "\r\n" &
  "\r\n" &
  body

proc findFreePort(): int =
  let sock = newSocket()
  sock.setSockOpt(OptReuseAddr, true)
  sock.bindAddr(Port(0), address = "127.0.0.1")
  result = int(sock.getLocalAddr()[1])
  sock.close()

proc tlsUrl(port: int; path = "/events"): string =
  ## https URL using "localhost" (not 127.0.0.1) so the hostname check
  ## actually runs — verifyPeerHostname skips IP literals.
  "https://localhost:" & $port & path

proc trustedCtx(caFile: string): SslContext =
  ## Client context that trusts exactly one (self-signed) certificate.
  newContext(verifyMode = CVerifyPeer, caFile = caFile)

proc insecureCtx(): SslContext =
  newContext(verifyMode = CVerifyNone)

type TlsServerConfig = object
  port: int
  certFile, keyFile: string
  resp1, resp2: string
  count: int
  reqBuf: ptr array[2, string]  # nil unless the test captures requests

proc serveTls(cfg: TlsServerConfig) {.thread.} =
  ## Accept `count` sequential connections; TLS-handshake each, read the
  ## request, send the canned response. A failed client handshake (e.g.
  ## the client rejecting our certificate) is swallowed and ends that
  ## connection only.
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  var ctx: SslContext
  try:
    ctx = newContext(verifyMode = CVerifyNone,
                     certFile = cfg.certFile, keyFile = cfg.keyFile)
  except CatchableError:
    server.close()
    return
  let responses = [cfg.resp1, cfg.resp2]
  for i in 0 ..< min(cfg.count, 2):
    var client: Socket
    try:
      server.accept(client)
    except CatchableError:
      break
    try:
      ctx.wrapConnectedSocket(client, handshakeAsServer)
      let req = client.recv(4096)
      if cfg.reqBuf != nil:
        cfg.reqBuf[i] = req
      # Only respond when a request actually arrived. When the client
      # rejects our certificate post-handshake (hostname mismatch tests),
      # recv yields 0 bytes — and SSL_write on that dead TLS socket can
      # block indefinitely inside OpenSSL rather than erroring out.
      if req.len > 0 and responses[i].len > 0:
        client.send(responses[i])
    except CatchableError:
      discard
    try:
      client.close()
    except CatchableError:
      # close() on an SSL socket sends close_notify, which can itself
      # fail when the peer is already gone; never let it kill the thread.
      discard
  server.close()
  ctx.destroyContext()

template withTlsServer(certF, keyF, response: string; testBody: untyped) =
  ## Single-connection TLS server around `testBody` (which sees `port`).
  let port {.inject.} = findFreePort()
  var thr: Thread[TlsServerConfig]
  createThread(thr, serveTls, TlsServerConfig(
    port: port, certFile: certF, keyFile: keyF, resp1: response, count: 1))
  sleep(100)
  testBody
  joinThread(thr)

type TlsTrickleConfig = object
  port: int
  certFile, keyFile: string
  head: string          ## Sent immediately after the request is read.
  pieces: seq[string]   ## Each sent after `delayMs` of silence.
  delayMs: int
  tailSilenceMs: int    ## Extra silence before closing (stall tests).

proc serveTlsTrickle(cfg: TlsTrickleConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  var ctx: SslContext
  try:
    ctx = newContext(verifyMode = CVerifyNone,
                     certFile = cfg.certFile, keyFile = cfg.keyFile)
  except CatchableError:
    server.close()
    return
  var client: Socket
  try:
    server.accept(client)
    ctx.wrapConnectedSocket(client, handshakeAsServer)
    discard client.recv(4096)
    client.send(cfg.head)
    for piece in cfg.pieces:
      sleep(cfg.delayMs)
      client.send(piece)
    if cfg.tailSilenceMs > 0:
      sleep(cfg.tailSilenceMs)
    client.close()
  except CatchableError:
    discard
  server.close()
  ctx.destroyContext()

# Plaintext single-shot server for the http → https redirect tests.
type PlainServerConfig = tuple[port: int, response: string]

proc servePlainOnce(cfg: PlainServerConfig) {.thread.} =
  var server = newSocket(buffered = false)
  server.setSockOpt(OptReuseAddr, true)
  server.bindAddr(Port(cfg.port), address = "127.0.0.1")
  server.listen()
  try:
    var client: Socket
    server.accept(client)
    discard client.recv(4096)
    client.send(cfg.response)
    client.close()
  except CatchableError:
    discard
  server.close()

# ---------------------------------------------------------------------------
# Client configurations
# ---------------------------------------------------------------------------

# verifyHostname defaults to true via the config field default.
const AsyncNoReconnect = AsyncSseClientConfig(
  autoReconnect: false,
  maxRedirects: 10,
  maxReconnectDelay: 60_000,
  stallTimeout: 0,
  recvSize: 4096,
)

const AsyncInsecure = AsyncSseClientConfig(
  autoReconnect: false,
  maxRedirects: 10,
  maxReconnectDelay: 60_000,
  stallTimeout: 0,
  recvSize: 4096,
  verifyHostname: false,
)

const SyncNoReconnect = SyncSseClientConfig(
  autoReconnect: false,
  maxRedirects: 10,
  maxReconnectDelay: 60_000,
  stallTimeout: 0,
  recvSize: 4096,
  pollInterval: 50,
  connectTimeout: 5_000,
)

const SyncInsecure = SyncSseClientConfig(
  autoReconnect: false,
  maxRedirects: 10,
  maxReconnectDelay: 60_000,
  stallTimeout: 0,
  recvSize: 4096,
  pollInterval: 50,
  connectTimeout: 5_000,
  verifyHostname: false,
)

# ===========================================================================
# 3. Async Client over TLS
# ===========================================================================

suite "Async Client over TLS":

  test "happy path: trusted cert, hostname verified, events delivered":
    withTlsServer(localhostCert, localhostKey,
                  sseResponse("data: secure hi\n\n")):
      proc run() {.async.} =
        var opened = false
        var events: seq[SseEvent] = @[]
        var errors: seq[string] = @[]
        let client = newAsyncSseClient(tlsUrl(port), config = AsyncNoReconnect)
        client.sslContext = trustedCtx(localhostCert)
        client.onOpen = proc() = opened = true
        client.onEvent = proc(e: SseEvent) = events.add(e)
        client.onError = proc(msg: string) = errors.add(msg)

        await client.connect()

        check opened
        check events.len == 1
        check events[0].data == "secure hi"
        check events[0].origin == "https://localhost:" & $port
        # The only error is the clean end-of-stream — no TLS errors.
        check errors == @["stream ended"]
      waitFor run()

  test "self-signed cert rejected by default context (fatal, no open)":
    withTlsServer(localhostCert, localhostKey,
                  sseResponse("data: nope\n\n")):
      proc run() {.async.} =
        var opened = false
        var errors: seq[string] = @[]
        # No sslContext set: library default = CVerifyPeer + system roots,
        # which cannot trust our self-signed test certificate.
        let client = newAsyncSseClient(tlsUrl(port), config = AsyncNoReconnect)
        client.onOpen = proc() = opened = true
        client.onError = proc(msg: string) = errors.add(msg)

        await client.connect()

        check not opened
        check client.readyState == Closed
        check errors.len == 1
        # Fatal TLS classification, not the retryable "connection failed".
        check "TLS" in errors[0]
      waitFor run()

  test "trusted chain but wrong hostname rejected (fatal)":
    withTlsServer(wronghostCert, wronghostKey,
                  sseResponse("data: mitm\n\n")):
      proc run() {.async.} =
        var opened = false
        var errors: seq[string] = @[]
        let client = newAsyncSseClient(tlsUrl(port), config = AsyncNoReconnect)
        # Chain verification passes (we trust the wronghost cert itself);
        # only the hostname check can reject it.
        client.sslContext = trustedCtx(wronghostCert)
        client.onOpen = proc() = opened = true
        client.onError = proc(msg: string) = errors.add(msg)

        await client.connect()

        check not opened
        check client.readyState == Closed
        check errors.len == 1
        check "not valid for hostname" in errors[0]
      waitFor run()

  test "CVerifyNone + verifyHostname=false accepts any certificate":
    withTlsServer(wronghostCert, wronghostKey,
                  sseResponse("data: dev mode\n\n")):
      proc run() {.async.} =
        var events: seq[SseEvent] = @[]
        let client = newAsyncSseClient(tlsUrl(port), config = AsyncInsecure)
        client.sslContext = insecureCtx()
        client.onEvent = proc(e: SseEvent) = events.add(e)

        await client.connect()

        check events.len == 1
        check events[0].data == "dev mode"
      waitFor run()

  test "hostname check independent of chain verification":
    withTlsServer(wronghostCert, wronghostKey,
                  sseResponse("data: nope\n\n")):
      proc run() {.async.} =
        var opened = false
        var errors: seq[string] = @[]
        # CVerifyNone skips chain verification, but verifyHostname is
        # still true (default) — the name mismatch must remain fatal.
        let client = newAsyncSseClient(tlsUrl(port), config = AsyncNoReconnect)
        client.sslContext = insecureCtx()
        client.onOpen = proc() = opened = true
        client.onError = proc(msg: string) = errors.add(msg)

        await client.connect()

        check not opened
        check client.readyState == Closed
        check errors.len == 1
        check "not valid for hostname" in errors[0]
      waitFor run()

  test "http → https permanent redirect switches to TLS mid-lifecycle":
    let tlsPort = findFreePort()
    var tlsThr: Thread[TlsServerConfig]
    createThread(tlsThr, serveTls, TlsServerConfig(
      port: tlsPort, certFile: localhostCert, keyFile: localhostKey,
      resp1: sseResponse("data: over tls\n\n"), count: 1))
    let plainPort = findFreePort()
    var plainThr: Thread[PlainServerConfig]
    createThread(plainThr, servePlainOnce, (plainPort,
      "HTTP/1.1 301 Moved Permanently\r\n" &
      "Location: https://localhost:" & $tlsPort & "/stream\r\n" &
      "Content-Length: 0\r\n\r\n"))
    sleep(100)

    proc run() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient("http://127.0.0.1:" & $plainPort & "/events",
                                     config = AsyncNoReconnect)
      client.sslContext = trustedCtx(localhostCert)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 1
      check events[0].data == "over tls"
      check events[0].origin == "https://localhost:" & $tlsPort
      # 301 is permanent: the reconnection URL must now be the https one.
      check client.url.scheme == "https"
      check client.url.port == tlsPort
    waitFor run()
    joinThread(plainThr)
    joinThread(tlsThr)

  test "Last-Event-ID sent on reconnection over TLS":
    var reqBuf: array[2, string]
    let port = findFreePort()
    var thr: Thread[TlsServerConfig]
    createThread(thr, serveTls, TlsServerConfig(
      port: port, certFile: localhostCert, keyFile: localhostKey,
      resp1: sseResponse("id: tls-42\ndata: first\n\n"),
      resp2: sseResponse("data: second\n\n"),
      count: 2, reqBuf: addr reqBuf))
    sleep(100)

    proc run() {.async.} =
      var openCount = 0
      let cfg = AsyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 500, stallTimeout: 0, recvSize: 4096)
      let client = newAsyncSseClient(tlsUrl(port), config = cfg)
      client.sslContext = trustedCtx(localhostCert)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()

      await client.connect()

      check openCount >= 2
    waitFor run()
    joinThread(thr)

    check "Last-Event-ID" notin reqBuf[0]
    check "Last-Event-ID: tls-42" in reqBuf[1]

  test "trickled TLS stream delivers events as records arrive":
    let port = findFreePort()
    var thr: Thread[TlsTrickleConfig]
    createThread(thr, serveTlsTrickle, TlsTrickleConfig(
      port: port, certFile: localhostCert, keyFile: localhostKey,
      head: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n",
      pieces: @["data: one\n\n", "data: two\n\n", "data: three\n\n"],
      delayMs: 150))
    sleep(100)

    proc run() {.async.} =
      var events: seq[SseEvent] = @[]
      let client = newAsyncSseClient(tlsUrl(port), config = AsyncNoReconnect)
      client.sslContext = trustedCtx(localhostCert)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 3
      check events[0].data == "one"
      check events[1].data == "two"
      check events[2].data == "three"
    waitFor run()
    joinThread(thr)

  test "stall timeout fires over TLS":
    let port = findFreePort()
    var thr: Thread[TlsTrickleConfig]
    createThread(thr, serveTlsTrickle, TlsTrickleConfig(
      port: port, certFile: localhostCert, keyFile: localhostKey,
      head: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
            "data: before stall\n\n",
      pieces: @[],
      delayMs: 0,
      tailSilenceMs: 2000))
    sleep(100)

    proc run() {.async.} =
      var events: seq[SseEvent] = @[]
      let cfg = AsyncSseClientConfig(
        autoReconnect: false, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 500, recvSize: 4096)
      let client = newAsyncSseClient(tlsUrl(port), config = cfg)
      client.sslContext = trustedCtx(localhostCert)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      await client.connect()

      check events.len == 1
      check events[0].data == "before stall"
      check client.readyState == Closed
    waitFor run()
    joinThread(thr)

# ===========================================================================
# 4. Sync Client over TLS
# ===========================================================================

suite "Sync Client over TLS":

  test "happy path: trusted cert, hostname verified, events delivered":
    withTlsServer(localhostCert, localhostKey,
                  sseResponse("data: secure hi\n\n")):
      proc run() =
        var opened = false
        var events: seq[SseEvent] = @[]
        var errors: seq[string] = @[]
        let client = newSyncSseClient(tlsUrl(port), config = SyncNoReconnect)
        client.sslContext = trustedCtx(localhostCert)
        client.onOpen = proc() = opened = true
        client.onEvent = proc(e: SseEvent) = events.add(e)
        client.onError = proc(msg: string) = errors.add(msg)

        client.connect()

        check opened
        check events.len == 1
        check events[0].data == "secure hi"
        check events[0].origin == "https://localhost:" & $port
        check errors == @["stream ended"]
      run()

  test "self-signed cert rejected by default context (fatal, no open)":
    withTlsServer(localhostCert, localhostKey,
                  sseResponse("data: nope\n\n")):
      proc run() =
        var opened = false
        var errors: seq[string] = @[]
        let client = newSyncSseClient(tlsUrl(port), config = SyncNoReconnect)
        client.onOpen = proc() = opened = true
        client.onError = proc(msg: string) = errors.add(msg)

        client.connect()

        check not opened
        check client.readyState == Closed
        check errors.len == 1
        check "TLS" in errors[0]
      run()

  test "trusted chain but wrong hostname rejected (fatal)":
    withTlsServer(wronghostCert, wronghostKey,
                  sseResponse("data: mitm\n\n")):
      proc run() =
        var opened = false
        var errors: seq[string] = @[]
        let client = newSyncSseClient(tlsUrl(port), config = SyncNoReconnect)
        client.sslContext = trustedCtx(wronghostCert)
        client.onOpen = proc() = opened = true
        client.onError = proc(msg: string) = errors.add(msg)

        client.connect()

        check not opened
        check client.readyState == Closed
        check errors.len == 1
        check "not valid for hostname" in errors[0]
      run()

  test "CVerifyNone + verifyHostname=false accepts any certificate":
    withTlsServer(wronghostCert, wronghostKey,
                  sseResponse("data: dev mode\n\n")):
      proc run() =
        var events: seq[SseEvent] = @[]
        let client = newSyncSseClient(tlsUrl(port), config = SyncInsecure)
        client.sslContext = insecureCtx()
        client.onEvent = proc(e: SseEvent) = events.add(e)

        client.connect()

        check events.len == 1
        check events[0].data == "dev mode"
      run()

  test "hostname check independent of chain verification":
    withTlsServer(wronghostCert, wronghostKey,
                  sseResponse("data: nope\n\n")):
      proc run() =
        var opened = false
        var errors: seq[string] = @[]
        let client = newSyncSseClient(tlsUrl(port), config = SyncNoReconnect)
        client.sslContext = insecureCtx()
        client.onOpen = proc() = opened = true
        client.onError = proc(msg: string) = errors.add(msg)

        client.connect()

        check not opened
        check client.readyState == Closed
        check errors.len == 1
        check "not valid for hostname" in errors[0]
      run()

  test "http → https permanent redirect switches to TLS mid-lifecycle":
    let tlsPort = findFreePort()
    var tlsThr: Thread[TlsServerConfig]
    createThread(tlsThr, serveTls, TlsServerConfig(
      port: tlsPort, certFile: localhostCert, keyFile: localhostKey,
      resp1: sseResponse("data: over tls\n\n"), count: 1))
    let plainPort = findFreePort()
    var plainThr: Thread[PlainServerConfig]
    createThread(plainThr, servePlainOnce, (plainPort,
      "HTTP/1.1 301 Moved Permanently\r\n" &
      "Location: https://localhost:" & $tlsPort & "/stream\r\n" &
      "Content-Length: 0\r\n\r\n"))
    sleep(100)

    proc run() =
      var events: seq[SseEvent] = @[]
      let client = newSyncSseClient("http://127.0.0.1:" & $plainPort & "/events",
                                    config = SyncNoReconnect)
      client.sslContext = trustedCtx(localhostCert)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      client.connect()

      check events.len == 1
      check events[0].data == "over tls"
      check events[0].origin == "https://localhost:" & $tlsPort
      check client.url.scheme == "https"
      check client.url.port == tlsPort
    run()
    joinThread(plainThr)
    joinThread(tlsThr)

  test "Last-Event-ID sent on reconnection over TLS":
    var reqBuf: array[2, string]
    let port = findFreePort()
    var thr: Thread[TlsServerConfig]
    createThread(thr, serveTls, TlsServerConfig(
      port: port, certFile: localhostCert, keyFile: localhostKey,
      resp1: sseResponse("id: tls-42\ndata: first\n\n"),
      resp2: sseResponse("data: second\n\n"),
      count: 2, reqBuf: addr reqBuf))
    sleep(100)

    proc run() =
      var openCount = 0
      let cfg = SyncSseClientConfig(
        autoReconnect: true, maxRedirects: 10,
        maxReconnectDelay: 500, stallTimeout: 0, recvSize: 4096,
        pollInterval: 50, connectTimeout: 5_000)
      let client = newSyncSseClient(tlsUrl(port), config = cfg)
      client.sslContext = trustedCtx(localhostCert)
      client.onOpen = proc() =
        inc openCount
        if openCount >= 2: client.close()

      client.connect()

      check openCount >= 2
    run()
    joinThread(thr)

    check "Last-Event-ID" notin reqBuf[0]
    check "Last-Event-ID: tls-42" in reqBuf[1]

  test "trickled TLS stream delivers events as records arrive":
    # The highest-risk sync path: pollRecv's recv(1, timeout) + buffered
    # drain must not lose or stall on data that sits in the TLS record
    # layer rather than the OS socket buffer.
    let port = findFreePort()
    var thr: Thread[TlsTrickleConfig]
    createThread(thr, serveTlsTrickle, TlsTrickleConfig(
      port: port, certFile: localhostCert, keyFile: localhostKey,
      head: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n",
      pieces: @["data: one\n\n", "data: two\n\n", "data: three\n\n"],
      delayMs: 150))
    sleep(100)

    proc run() =
      var events: seq[SseEvent] = @[]
      let client = newSyncSseClient(tlsUrl(port), config = SyncNoReconnect)
      client.sslContext = trustedCtx(localhostCert)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      client.connect()

      check events.len == 3
      check events[0].data == "one"
      check events[1].data == "two"
      check events[2].data == "three"
    run()
    joinThread(thr)

  test "stall timeout fires over TLS":
    let port = findFreePort()
    var thr: Thread[TlsTrickleConfig]
    createThread(thr, serveTlsTrickle, TlsTrickleConfig(
      port: port, certFile: localhostCert, keyFile: localhostKey,
      head: "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n" &
            "data: before stall\n\n",
      pieces: @[],
      delayMs: 0,
      tailSilenceMs: 2000))
    sleep(100)

    proc run() =
      var events: seq[SseEvent] = @[]
      let cfg = SyncSseClientConfig(
        autoReconnect: false, maxRedirects: 10,
        maxReconnectDelay: 60_000, stallTimeout: 500, recvSize: 4096,
        pollInterval: 50, connectTimeout: 5_000)
      let client = newSyncSseClient(tlsUrl(port), config = cfg)
      client.sslContext = trustedCtx(localhostCert)
      client.onEvent = proc(e: SseEvent) = events.add(e)

      client.connect()

      check events.len == 1
      check events[0].data == "before stall"
      check client.readyState == Closed
    run()
    joinThread(thr)

# ===========================================================================
# Cleanup
# ===========================================================================

removeDir(certDir)
