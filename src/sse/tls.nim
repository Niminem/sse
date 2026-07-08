## TLS helpers shared by the async and sync SSE clients (Phase 8).
##
## Only compiled when the project is built with `-d:ssl`; importing it
## from a non-SSL build is a compile-time error. Uses OpenSSL via
## `std/net` / `std/openssl`, loaded dynamically at runtime.
##
## Why this module exists: with `-d:ssl`, `std/net` verifies the
## certificate *chain* during the handshake (`CVerifyPeer`), but its
## hostname check (`checkCertName`) is compiled out on Windows, and
## `std/asyncnet` never performs one on any platform. Without a hostname
## check, a valid certificate for *any* domain is accepted for *every*
## host — a man-in-the-middle presenting a legitimate certificate for
## their own domain passes verification. `verifyPeerHostname` closes
## that gap portably (Windows, macOS, Linux) for both clients by calling
## `X509_check_host` directly.
##
## The sync handshake (`tlsHandshake`) is performed manually here rather
## than via `std/net.wrapConnectedSocket` because the latter bundles its
## own hostname check on POSIX (and silently omits it on Windows). Doing
## the handshake ourselves gives both clients identical, configurable
## verification behavior on every platform.

when not defined(ssl):
  {.error: "sse/tls requires building with -d:ssl".}

import std/net
import std/openssl

# Re-export the TLS surface clients and library users need, so neither
# has to import std/net's SSL symbols directly.
export SslContext, SslError, SslCVerifyMode, newContext

# ---------------------------------------------------------------------------
# Context Resolution
# ---------------------------------------------------------------------------

proc resolveTlsContext*(configured: SslContext;
                        cache: var SslContext): SslContext =
  ## Return the user-configured context if set, otherwise a
  ## lazily-created library default (`CVerifyPeer` + system CA roots),
  ## stored in `cache` so the CA store is scanned once per client rather
  ## than on every reconnect attempt.
  ##
  ## Raises `IOError`/`SslError` when OpenSSL or a CA bundle cannot be
  ## loaded. Callers treat this as fatal: a missing CA store never heals
  ## by retrying, and surfacing it once beats a silent backoff loop.
  if configured != nil:
    return configured
  if cache == nil:
    cache = newContext(verifyMode = CVerifyPeer)
  cache

# ---------------------------------------------------------------------------
# Hostname Verification
# ---------------------------------------------------------------------------

when defined(nimDisableCertificateValidation):
  proc verifyPeerHostname*(ssl: SslPtr; hostname: string) =
    ## No-op: certificate validation globally disabled at build time.
    discard

else:
  when defined(windows):
    # std/openssl declares the certificate-validation symbols only on
    # POSIX (the root cause of std/net's missing hostname check on
    # Windows). The DLLs do export them, so bind them here. Note the
    # X509_* procs live in libcrypto (DLLUtilName): unlike POSIX dlsym,
    # Windows GetProcAddress does not search dependent DLLs, so loading
    # them through libssl would fail at startup.
    when useOpenssl3:
      proc SSL_get1_peer_certificate(ssl: SslPtr): PX509
        {.cdecl, dynlib: DLLSSLName, importc.}
      proc SSL_get_peer_certificate(ssl: SslPtr): PX509 =
        SSL_get1_peer_certificate(ssl)
    else:
      # Removed in OpenSSL 3.0; only bound when loading 1.1 DLLs.
      proc SSL_get_peer_certificate(ssl: SslPtr): PX509
        {.cdecl, dynlib: DLLSSLName, importc.}

    proc X509_check_host(cert: PX509; name: cstring; namelen: cint;
                         flags: cuint; peername: cstring): cint
      {.cdecl, dynlib: DLLUtilName, importc.}

    proc X509_free(cert: PX509)
      {.cdecl, dynlib: DLLUtilName, importc.}

  proc verifyPeerHostname*(ssl: SslPtr; hostname: string) =
    ## Check that the peer certificate's Subject Alternative Name (or
    ## Subject CommonName) matches `hostname`, per RFC 6125. Wildcards
    ## match only in the left-most label. Must be called after a
    ## completed client handshake. Raises `SslError` on mismatch or when
    ## no certificate was presented.
    ##
    ## IP-literal targets are skipped: `X509_check_host` matches DNS
    ## names only (checking IPs needs `X509_check_ip_asc`, which the
    ## stdlib wrapper does not expose). This mirrors std/net's own
    ## behavior.
    if hostname.len == 0 or isIpAddress(hostname):
      return
    const AlwaysCheckSubject = 0x1.cuint  # X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT
    var cert: PX509
    try:
      cert = SSL_get_peer_certificate(ssl)
    except LibraryError:
      raise newException(SslError,
        "could not load certificate functions from OpenSSL")
    if cert.isNil:
      raise newException(SslError, "peer presented no certificate")
    let match = X509_check_host(cert, hostname.cstring, hostname.len.cint,
                                AlwaysCheckSubject, nil)
    X509_free(cert)
    if match != 1:
      raise newException(SslError,
        "certificate is not valid for hostname '" & hostname & "'")

# ---------------------------------------------------------------------------
# Sync Handshake
# ---------------------------------------------------------------------------

proc tlsHandshake*(socket: Socket; ctx: SslContext; hostname: string) =
  ## Perform a client-side TLS handshake on a connected blocking socket.
  ##
  ## Mirror of `std/net.wrapConnectedSocket(handshakeAsClient)` minus its
  ## built-in hostname check, which is inconsistent across platforms
  ## (compiled out on Windows). Callers invoke `verifyPeerHostname`
  ## explicitly afterwards, so verification is uniform on every platform
  ## and controlled by a single configuration knob.
  ##
  ## Raises `SslError` on handshake failure.
  wrapSocket(ctx, socket)
  if hostname.len > 0 and not isIpAddress(hostname):
    # SNI. Result discarded as in std/net: it only fails when the SSL
    # configuration predates TLSv1, in which case the handshake proceeds
    # without the extension.
    discard SSL_set_tlsext_host_name(socket.sslHandle, hostname)
  ErrClearError()
  let ret = SSL_connect(socket.sslHandle)
  socketError(socket, ret)
