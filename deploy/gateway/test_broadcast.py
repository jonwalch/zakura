#!/usr/bin/env python3
"""Tests for the Zakura public submit gateway."""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any


SCRIPT_PATH = Path(__file__).with_name("broadcast.py")
SPEC = importlib.util.spec_from_file_location("zakura_broadcast_gateway", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
gateway = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gateway
SPEC.loader.exec_module(gateway)


def make_fake_backend_handler(
    responses: dict[str, tuple[int, dict[str, Any]]],
) -> type[BaseHTTPRequestHandler]:
    class FakeBackendHandler(BaseHTTPRequestHandler):
        forwarded_for_values: list[str | None] = []
        post_count = 0

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            if self.path != "/healthz":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "3")
            self.end_headers()
            self.wfile.write(b"ok\n")

        def do_POST(self) -> None:
            type(self).post_count += 1
            type(self).forwarded_for_values.append(self.headers.get("X-Forwarded-For"))
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            payload = json.loads(raw.decode())
            method = payload["method"]
            status, body = responses.get(
                method,
                (
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "error": {"code": -32601, "message": "unknown"},
                    },
                ),
            )
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return FakeBackendHandler


def start_fake_backend(
    responses: dict[str, tuple[int, dict[str, Any]]],
) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_fake_backend_handler(responses))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}/"


class LoadBackendsTest(unittest.TestCase):
    def test_loads_backends_toml(self) -> None:
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "backends.toml"
            path.write_text(
                '[[backends]]\nname = "a"\nurl = "http://127.0.0.1:8237/"\n',
                encoding="utf-8",
            )
            backends = gateway.load_backends(path)
            self.assertEqual(
                backends,
                [gateway.Backend(name="a", url="http://127.0.0.1:8237/")],
            )


class RateLimiterTest(unittest.TestCase):
    def test_limits_per_client(self) -> None:
        limiter = gateway.RateLimiter(limit=2, window=60.0)
        self.assertTrue(limiter.allow("1.1.1.1", now=100.0))
        self.assertTrue(limiter.allow("1.1.1.1", now=100.1))
        self.assertFalse(limiter.allow("1.1.1.1", now=100.2))
        self.assertTrue(limiter.allow("2.2.2.2", now=100.2))


class BackendPoolTest(unittest.TestCase):
    def test_does_not_retry_an_ambiguous_backend_failure(self) -> None:
        good_payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "abcd" * 16,
        }
        server_good, url_good = start_fake_backend({"sendrawtransaction": (200, good_payload)})
        try:
            pool = gateway.BackendPool(
                [
                    # Reserved port with nothing listening.
                    gateway.Backend("bad", "http://127.0.0.1:1/"),
                    gateway.Backend("good", url_good),
                ],
                timeout=1.0,
                health_interval=60.0,
            )
            # Pretend both are healthy so the unreachable backend is selected.
            with pool.lock:
                pool.healthy = {"bad": True, "good": True}
                pool.next_index = 0
            status, body, name = pool.forward(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "sendrawtransaction",
                    "params": ["00"],
                },
                "203.0.113.9",
            )
            self.assertEqual(status, 502)
            self.assertEqual(name, "")
            self.assertEqual(json.loads(body)["error"]["code"], -32000)
            self.assertEqual(server_good.RequestHandlerClass.post_count, 0)
        finally:
            server_good.shutdown()
            server_good.server_close()

    def test_forwards_the_sanitized_client_ip(self) -> None:
        submit_ok = {"jsonrpc": "2.0", "id": 1, "result": "ab" * 32}
        server, url = start_fake_backend({"sendrawtransaction": (200, submit_ok)})
        try:
            pool = gateway.BackendPool(
                [gateway.Backend("local", url)],
                timeout=1.0,
                health_interval=60.0,
            )
            status, _, name = pool.forward(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "sendrawtransaction",
                    "params": ["00"],
                },
                "203.0.113.9",
            )
            self.assertEqual(status, 200)
            self.assertEqual(name, "local")
            self.assertEqual(
                server.RequestHandlerClass.forwarded_for_values,
                ["203.0.113.9"],
            )
        finally:
            server.shutdown()
            server.server_close()


class HandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        submit_ok = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": "ab" * 32,
        }
        self.backend, backend_url = start_fake_backend(
            {"sendrawtransaction": (200, submit_ok)}
        )
        pool = gateway.BackendPool(
            [gateway.Backend("local", backend_url)],
            timeout=2.0,
            health_interval=60.0,
        )
        pool.refresh_health()
        gateway.GATEWAY = pool
        gateway.RATE_LIMITER = gateway.RateLimiter(limit=100, window=60.0)
        gateway.MAX_BODY_BYTES = 1024
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), gateway.SubmitHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.backend.shutdown()
        self.backend.server_close()
        gateway.GATEWAY = None

    def _post(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            self.base + "/",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                return int(resp.status), json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return int(exc.code), json.loads(exc.read().decode())

    def test_allowlists_sendrawtransaction(self) -> None:
        status, body = self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": gateway.ALLOWED_METHOD,
                "params": ["00"],
            }
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["result"], "ab" * 32)

    def test_rejects_other_methods(self) -> None:
        status, body = self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getblockchaininfo",
                "params": [],
            }
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["error"]["code"], -32601)

    def test_healthz(self) -> None:
        with urllib.request.urlopen(self.base + "/healthz", timeout=3) as resp:
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.read(), b"ok\n")


if __name__ == "__main__":
    unittest.main()
