from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import pytest
import yaml

SPEC_DIR = Path(__file__).parent / "spec"
SPEC: dict[str, Any] = yaml.safe_load((SPEC_DIR / "openapi.yml").read_text())
SCHEMAS: dict[str, Any] = SPEC["components"]["schemas"]
CALLS: list[dict[str, Any]] = json.loads((SPEC_DIR / "sdk-calls.json").read_text())
CONTRACT: dict[str, Any] = json.loads((SPEC_DIR / "sdk-contract.json").read_text())


@dataclass
class Recorded:
    method: str
    path: str
    query: list[tuple[str, str]]
    headers: dict[str, str]
    body: bytes


@dataclass
class Reply:
    status: int = 200
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    raw: bytes | None = None
    delay: float = 0.0


Handler = Callable[[Recorded, int], Reply]


class Fake:
    def __init__(self, handler: Handler) -> None:
        self.handler = handler
        self.requests: list[Recorded] = []
        self.lock = threading.Lock()
        self.stop = threading.Event()
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def _serve(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                body = self.rfile.read(length) if length else b""
                parts = urlsplit(self.path)
                rec = Recorded(self.command, parts.path, parse_qsl(parts.query, keep_blank_values=True), {k.lower(): v for k, v in self.headers.items()}, body)
                with fake.lock:
                    fake.requests.append(rec)
                    n = len(fake.requests)
                reply = fake.handler(rec, n)
                if reply.delay and fake.stop.wait(reply.delay):
                    return
                data = reply.raw if reply.raw is not None else json.dumps(reply.body).encode()
                try:
                    self.send_response(reply.status)
                    headers = {"content-type": "application/json", **reply.headers}
                    for k, v in headers.items():
                        self.send_header(k, v)
                    self.send_header("content-length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = _serve
            do_POST = _serve

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}/api/v1"

    def close(self) -> None:
        self.stop.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


@pytest.fixture
def serve() -> Iterator[Callable[[Handler], Fake]]:
    fakes: list[Fake] = []

    def make(handler: Handler) -> Fake:
        f = Fake(handler)
        fakes.append(f)
        return f

    yield make
    for f in fakes:
        f.close()


@pytest.fixture(autouse=True)
def no_leaked_threads() -> Iterator[None]:
    before = {t.ident for t in threading.enumerate()}
    yield
    import time

    deadline = time.time() + 3
    while time.time() < deadline:
        extra = [t for t in threading.enumerate() if t.ident not in before and t.is_alive() and not t.daemon]
        if not extra:
            return
        time.sleep(0.02)
    raise AssertionError(f"threads leaked: {extra}")


def resolve(node: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in node:
        _, _, section, name = node["$ref"].split("/", 3)
        node = SPEC["components"][section][name]
    return node


def instance(node: dict[str, Any], variant: str) -> Any:
    n = resolve(node)
    if "enum" in n:
        return "x_future_value" if variant == "unknown-values" else n["enum"][0]
    if "oneOf" in n:
        return instance(n["oneOf"][0], variant)
    t = n.get("type")
    if t == "object":
        props = n.get("properties") or {}
        required = set(n.get("required") or [])
        out: dict[str, Any] = {"x_future_field": {"nested": [1]}} if variant != "required" and props else {}
        for k, v in props.items():
            if variant != "required" or k in required:
                out[k] = instance(v, variant)
        ap = n.get("additionalProperties")
        if not props and isinstance(ap, dict) and variant != "required":
            out["k1"] = instance(ap, variant)
        return out
    if t == "array":
        return [instance(n["items"], variant)]
    if t == "string":
        return "2026-09-28T00:00:00Z" if n.get("format") == "date-time" else "1"
    if t == "integer":
        return 1
    if t == "number":
        return 1.5
    if t == "boolean":
        return True
    return "1"


def problem(status: int, code: str, detail: str, headers: dict[str, str] | None = None) -> Reply:
    body = {"type": f"https://docs.avee.tech/errors/{code}", "title": code, "status": status, "code": code, "detail": detail, "request_id": "req-1"}
    return Reply(status, body, {"content-type": "application/problem+json", "x-request-id": "req-1", **(headers or {})})
