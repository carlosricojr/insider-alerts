"""Real TLS with ephemeral synthetic keys and loopback-only client requests.

Fixture generation is an offline setup step using the CI image's OpenSSL. It
does not install a dependency, edit OS trust, or copy operational certificates.
"""

import hashlib
import ipaddress
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import urllib.request
from contextlib import contextmanager, nullcontext
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest.mock import patch

import pytest

from insider_alerts.backtest.prices import PriceDataError, StooqPriceClient
from insider_alerts.config import Settings
from insider_alerts.notify.ntfy import NtfyNotificationError, NtfyNotifier
from insider_alerts.review.market_context import DailyMarketDataClient, MarketContextError
from insider_alerts.sec.client import SecHttpClient, SecHttpError

CLIENTS = ("sec", "ntfy", "market_context", "prices")
CLIENT_ERRORS = (SecHttpError, NtfyNotificationError, MarketContextError, PriceDataError, OSError)


@pytest.fixture(scope="session")
def tls_material(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-isolation-tls")
    openssl = shutil.which("openssl")
    if openssl is None and os.name == "nt":
        candidate = (
            Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Git/usr/bin/openssl.exe"
        )
        if candidate.is_file():
            openssl = str(candidate)
    assert openssl is not None, (
        "Existing disposable CI OpenSSL is required; never install it on a host"
    )

    def command(*args):
        result = subprocess.run(
            [openssl, *map(str, args)],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        assert len(result.stdout) < 8192 and len(result.stderr) < 8192
        return result.stdout.strip()

    version = command("version")

    def key(name):
        path = root / f"{name}.key"
        command("genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", path)
        return path

    def request(name, private_key):
        path = root / f"{name}.csr"
        command("req", "-new", "-key", private_key, "-subj", f"/CN=synthetic-{name}", "-out", path)
        return path

    roots = {}
    for name in ("good", "bad-a", "bad-b"):
        private_key = key(name)
        cert = root / f"{name}.pem"
        command(
            "req",
            "-new",
            "-x509",
            "-sha256",
            "-key",
            private_key,
            "-days",
            "3650",
            "-subj",
            f"/CN=synthetic-{name}",
            "-out",
            cert,
            "-addext",
            "basicConstraints=critical,CA:TRUE,pathlen:1",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-addext",
            "subjectKeyIdentifier=hash",
        )
        roots[name] = (cert, private_key)

    def sign(name, csr, ca, private_key, intermediate=False):
        cert = root / f"{name}.pem"
        extensions = root / f"{name}.ext"
        usage = "keyCertSign,cRLSign" if intermediate else "digitalSignature,keyEncipherment"
        constraints = "CA:TRUE,pathlen:0" if intermediate else "CA:FALSE"
        text = (
            f"basicConstraints=critical,{constraints}\nkeyUsage=critical,{usage}\n"
            "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n"
        )
        if not intermediate:
            text += "extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n"
        extensions.write_text(text, encoding="ascii")
        command(
            "x509",
            "-req",
            "-sha256",
            "-in",
            csr,
            "-CA",
            ca,
            "-CAkey",
            private_key,
            "-set_serial",
            str(100 + len(list(root.glob("*.pem")))),
            "-days",
            "3650",
            "-extfile",
            extensions,
            "-out",
            cert,
        )
        return cert

    good_key = key("good-leaf")
    good = sign("good-leaf", request("good-leaf", good_key), *roots["good"])
    intermediate_key = key("synthetic-compromised-intermediate")
    intermediate_csr = request("synthetic-compromised-intermediate", intermediate_key)
    intermediate_a = sign("intermediate-a", intermediate_csr, *roots["bad-a"], intermediate=True)
    intermediate_b = sign("intermediate-b", intermediate_csr, *roots["bad-b"], intermediate=True)
    bad_key = key("bad-leaf")
    bad = sign("bad-leaf", request("bad-leaf", bad_key), intermediate_a, intermediate_key)
    chains = {}
    for name, intermediate in (("a", intermediate_a), ("b", intermediate_b)):
        chain = root / f"bad-chain-{name}.pem"
        chain.write_bytes(bad.read_bytes() + intermediate.read_bytes())
        chains[name] = chain
    command("verify", "-x509_strict", "-CAfile", roots["good"][0], good)
    for name, intermediate in (("a", intermediate_a), ("b", intermediate_b)):
        command(
            "verify",
            "-x509_strict",
            "-CAfile",
            roots[f"bad-{name}"][0],
            "-untrusted",
            intermediate,
            bad,
        )
    empty = root / "empty-ca-directory"
    empty.mkdir()
    bad_directory = root / "additional-bad-ca-directory"
    bad_directory.mkdir()
    ca_hash = command("x509", "-in", roots["bad-b"][0], "-subject_hash", "-noout")
    assert len(ca_hash) == 8 and all(c in "0123456789abcdef" for c in ca_hash)
    (bad_directory / f"{ca_hash}.0").write_bytes(roots["bad-b"][0].read_bytes())
    # No CA/intermediate signing key remains when any client test starts.
    for _, private_key in roots.values():
        private_key.unlink()
    intermediate_key.unlink()
    provenance = {
        "generator": version,
        "python": sys.version,
        "python_openssl": ssl.OPENSSL_VERSION,
        "generator_source": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "fixture_type": "ephemeral synthetic; no operational identities or CA keys committed",
        "pem_lf_sha256": {
            p.name: hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
            for p in sorted(root.glob("*.pem"))
        },
    }
    if os.environ.get("RUNNER_TEMP"):
        (Path(os.environ["RUNNER_TEMP"]) / "insider-offline-tls-provenance.json").write_text(
            json.dumps(provenance), encoding="utf-8"
        )
    return {
        "good": good,
        "good_key": good_key,
        "bad_key": bad_key,
        "chains": chains,
        "roots": {name: pair[0] for name, pair in roots.items()},
        "empty": empty,
        "bad_directory": bad_directory,
        "directory": root,
    }


@pytest.fixture
def isolated_tls(monkeypatch, tls_material):
    # Environment changes apply only to this disposable test process.
    for name in tuple(os.environ):
        if name.lower().endswith("_proxy") or name in (
            "SSLKEYLOGFILE",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
        ):
            monkeypatch.delenv(name)
    monkeypatch.setenv("NO_PROXY", "*")  # Also bypass Windows registry proxies.
    monkeypatch.setenv("SSL_CERT_DIR", str(tls_material["empty"]))
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["good"]))
    monkeypatch.setattr(urllib.request, "_opener", None)
    monkeypatch.setattr(socket, "getfqdn", lambda name="": "synthetic-loopback")
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_dns = socket.getaddrinfo

    def guard_address(address):
        assert isinstance(address, tuple) and ipaddress.ip_address(address[0]).is_loopback

    def connect(sock, address):
        guard_address(address)
        return original_connect(sock, address)

    def connect_ex(sock, address):
        guard_address(address)
        return original_connect_ex(sock, address)

    def dns(host, *args, **kwargs):
        assert host in ("127.0.0.1", "localhost", "::1", b"127.0.0.1", b"localhost", b"::1")
        return original_dns(host, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", dns)
    failures = []
    original_wrap = ssl.SSLContext.wrap_socket

    def observe_handshake(context, *args, **kwargs):
        try:
            return original_wrap(context, *args, **kwargs)
        except ssl.SSLCertVerificationError as exc:
            failures.append(exc.verify_code)
            raise  # Observation only: no verification result is substituted.

    monkeypatch.setattr(ssl.SSLContext, "wrap_socket", observe_handshake)
    return failures


@contextmanager
def https_server(chain, private_key):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(chain, private_key)

    class Handler(BaseHTTPRequestHandler):
        def reply(self):
            self.rfile.read(min(int(self.headers.get("Content-Length", "0")), 1024))
            body = b'{"id":"synthetic-fixture","time":1,"message":"synthetic"}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = reply
        do_POST = reply

        def log_message(self, *args):
            pass

    class Server(HTTPServer):
        attempts = 0

        def get_request(self):
            raw, address = super().get_request()
            self.attempts += 1  # Count even when client rejects the handshake.
            raw.settimeout(2)
            try:
                return context.wrap_socket(raw, server_side=True), address
            except BaseException:
                raw.close()
                raise

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
    )
    thread.start()
    try:
        yield server, f"https://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive(), "owned loopback server must stop"


def forbidden_sleep(seconds):
    raise AssertionError(f"synthetic client unexpectedly requested a sleep: {seconds}")


def request_client(kind, url, *, fresh_opener=True):
    """Profile tests isolate urllib initialization; retained-context tests opt out.

    This resets test-local library state with restoration, not verification or
    production client behavior. It does not prove that a running writer reloads
    trust when its environment changes.
    """
    settings = Settings(
        _env_file=None,
        SEC_USER_AGENT="synthetic offline test contact@example.invalid",
        SEC_RETRY_ATTEMPTS=1,
        SEC_TIMEOUT_SECONDS=2,
        NTFY_BASE_URL=url,
        NTFY_TOPIC="synthetic-fixture",
        NTFY_TOKEN=None,
        NTFY_RETRY_ATTEMPTS=1,
        NTFY_TIMEOUT_SECONDS=2,
    )
    if kind == "sec":
        return SecHttpClient(settings, now_fn=lambda: 100, sleep_fn=forbidden_sleep).get_text(url)
    if kind == "ntfy":
        assert (
            NtfyNotifier(settings).send("synthetic", "synthetic", markdown=False).http_status == 200
        )
        return "synthetic"
    options = {
        "user_agent": "synthetic",
        "timeout_seconds": 2,
        "rate_limit_per_second": 1,
        "retry_attempts": 1,
        "retry_min_seconds": 0,
        "retry_max_seconds": 0,
        "now_fn": lambda: 100,
        "sleep_fn": forbidden_sleep,
    }
    # urlopen caches its HTTPSHandler/SSLContext globally. Each trust profile
    # needs a newly initialized opener; restore the prior one after the request.
    scope = patch.object(urllib.request, "_opener", None) if fresh_opener else nullcontext()
    with scope:
        if kind == "market_context":
            return DailyMarketDataClient(**options)._download_text(url, symbol="SYNTHETIC")
        assert kind == "prices"
        return StooqPriceClient(**options)._download_text(url=url, symbol="SYNTHETIC")


@pytest.mark.parametrize("kind", ("market_context", "prices"))
def test_retained_urllib_context_does_not_reload_changed_environment(
    kind, isolated_tls, tls_material, monkeypatch
):
    with https_server(tls_material["chains"]["b"], tls_material["bad_key"]) as (server, url):
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["bad-b"]))
        assert "synthetic" in request_client(kind, url, fresh_opener=False)
        retained = urllib.request._opener
        assert retained is not None
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["good"]))
        before = server.attempts
        # Deliberate negative feasibility result: a live cached context retains
        # the old anchor. Environment mutation is not an admitted trust update.
        assert "synthetic" in request_client(kind, url, fresh_opener=False)
        assert server.attempts > before and urllib.request._opener is retained
        before = server.attempts
        with pytest.raises(CLIENT_ERRORS):
            request_client(kind, url)
        assert server.attempts > before and set(isolated_tls) & {20, 21}
        assert urllib.request._opener is retained, "fresh-profile state must be restored"


@pytest.mark.parametrize("kind", CLIENTS)
def test_good_chain_then_wrong_hostname(kind, isolated_tls, tls_material):
    with https_server(tls_material["good"], tls_material["good_key"]) as (server, url):
        assert "synthetic" in request_client(kind, url)
        before = server.attempts
        with pytest.raises(CLIENT_ERRORS):
            request_client(kind, url.replace("127.0.0.1", "localhost"))
        assert server.attempts > before
        assert set(isolated_tls) & {62, 64}, "actual hostname verification failure required"


@pytest.mark.parametrize("kind", CLIENTS)
@pytest.mark.parametrize("chain", ("a", "b"))
def test_each_cross_signed_chain_has_a_positive_and_unknown_issuer_control(
    kind, chain, isolated_tls, tls_material, monkeypatch
):
    with https_server(tls_material["chains"][chain], tls_material["bad_key"]) as (server, url):
        # Chain B still works without A: removing one anchor cannot distrust the
        # synthetic intermediate identity that is cross-signed by both roots.
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"][f"bad-{chain}"]))
        assert "synthetic" in request_client(kind, url)
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["good"]))
        before = server.attempts
        with pytest.raises(CLIENT_ERRORS):
            request_client(kind, url)
        assert server.attempts > before
        assert set(isolated_tls) & {20, 21}, "actual issuer verification failure required"


@pytest.mark.parametrize("kind", CLIENTS)
def test_environment_only_variant_does_not_narrow_urllib_trust(
    kind, isolated_tls, tls_material, monkeypatch
):
    with https_server(tls_material["chains"]["b"], tls_material["bad_key"]) as (_, url):
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["bad-b"]))
        assert "synthetic" in request_client(kind, url)
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["good"]))
        monkeypatch.setenv("SSL_CERT_DIR", str(tls_material["bad_directory"]))
        if kind in ("market_context", "prices"):
            # Deliberate negative feasibility result, not operational admission.
            assert "synthetic" in request_client(kind, url)
        else:
            with pytest.raises(CLIENT_ERRORS):
                request_client(kind, url)
            assert set(isolated_tls) & {20, 21}


@pytest.mark.parametrize("kind", CLIENTS)
@pytest.mark.parametrize("invalid", ("missing", "malformed"))
def test_invalid_bundle_requires_real_configuration_or_issuer_failure(
    kind, invalid, isolated_tls, tls_material, monkeypatch
):
    with https_server(tls_material["good"], tls_material["good_key"]) as (server, url):
        assert "synthetic" in request_client(kind, url)
        invalid_file = tls_material["directory"] / f"{invalid}.pem"
        if invalid == "malformed":
            invalid_file.write_bytes(b"synthetic invalid PEM\n")
        monkeypatch.setenv("SSL_CERT_FILE", str(invalid_file))
        failures = []
        original = ssl.create_default_context

        def observe_context(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except OSError as exc:
                failures.append(exc)
                raise

        monkeypatch.setattr(ssl, "create_default_context", observe_context)
        monkeypatch.setattr(ssl, "_create_default_https_context", observe_context)
        before = server.attempts
        with pytest.raises(CLIENT_ERRORS):
            request_client(kind, url)
        if kind in ("sec", "ntfy"):
            assert failures and server.attempts == before
            if invalid == "missing":
                assert any(isinstance(exc, FileNotFoundError) for exc in failures)
            else:
                assert any(isinstance(exc, ssl.SSLError) for exc in failures)
        else:
            # Default-path loading can ignore an unavailable bundle and retain
            # other trust sources. That is not explicit-cafile fail-closedness.
            assert failures or (server.attempts > before and set(isolated_tls) & {20, 21})


@pytest.mark.parametrize("kind", CLIENTS)
@pytest.mark.parametrize("empty", (False, True))
def test_missing_or_empty_environment_is_not_a_controlled_bundle(
    kind, empty, isolated_tls, tls_material, monkeypatch
):
    with https_server(tls_material["good"], tls_material["good_key"]) as (server, url):
        assert "synthetic" in request_client(kind, url)
        if empty:
            monkeypatch.setenv("SSL_CERT_FILE", "")
        else:
            monkeypatch.delenv("SSL_CERT_FILE")
        before = server.attempts
        with pytest.raises(CLIENT_ERRORS):
            request_client(kind, url)
        assert server.attempts > before and set(isolated_tls) & {20, 21}


@pytest.mark.skipif(os.name != "nt", reason="labelled Windows store-enumeration model")
@pytest.mark.parametrize("kind", CLIENTS)
def test_windows_store_enumeration_model_is_additive(kind, isolated_tls, tls_material, monkeypatch):
    # Model only: no real OS store is modified or production inventory inferred.
    der = ssl.PEM_cert_to_DER_cert(tls_material["roots"]["bad-b"].read_text())
    stores = []

    def synthetic_store(name):
        stores.append(name)
        return [(der, "x509_asn", True)]

    monkeypatch.setattr(ssl, "enum_certificates", synthetic_store)
    with https_server(tls_material["chains"]["b"], tls_material["bad_key"]) as (_, url):
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["bad-b"]))
        assert "synthetic" in request_client(kind, url)
        stores.clear()
        monkeypatch.setenv("SSL_CERT_FILE", str(tls_material["roots"]["good"]))
        if kind in ("market_context", "prices"):
            assert "synthetic" in request_client(kind, url)
            assert {"CA", "ROOT"}.issubset(stores)
        else:
            with pytest.raises(CLIENT_ERRORS):
                request_client(kind, url)
            assert not stores and set(isolated_tls) & {20, 21}
