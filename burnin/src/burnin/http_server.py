"""Threading HTTP server with all burn-in REST API endpoints (spec §3–§7)."""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Protocol

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

logger = logging.getLogger("burnin")

_deprecation_warned: set[str] = set()


class EngineAPI(Protocol):
    """Interface the HTTP server uses to call into the engine/app controller."""

    def get_state_name(self) -> str: ...
    def get_info(self) -> dict[str, Any]: ...
    def get_broker_status(self) -> dict[str, Any]: ...
    def handle_run_start(self, body: dict[str, Any]) -> tuple[int, dict[str, Any]]: ...
    def handle_run_stop(self) -> tuple[int, dict[str, Any]]: ...
    def get_run(self) -> dict[str, Any]: ...
    def get_run_status(self) -> dict[str, Any]: ...
    def get_run_config(self) -> tuple[int, dict[str, Any]]: ...
    def get_run_report(self) -> tuple[int, dict[str, Any]]: ...
    def handle_cleanup(self) -> tuple[int, dict[str, Any]]: ...
    def get_cors_origins(self) -> str: ...


class _Handler(BaseHTTPRequestHandler):
    """HTTP request handler for all burn-in API endpoints."""

    server: _BurninServer  # type: ignore[assignment]

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._add_cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?")[0]
        api = self.server.engine_api

        if path == "/health":
            self._json_ok(200, {"status": "alive"})

        elif path == "/ready":
            state = api.get_state_name() if api else "idle"
            not_ready = state in ("starting", "stopping")
            code = 503 if not_ready else 200
            status = "not_ready" if not_ready else "ready"
            self._json_ok(code, {"status": status, "state": state})

        elif path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self._add_cors_headers()
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(output)))
            self.end_headers()
            self.wfile.write(output)

        elif path == "/info":
            self._json_ok(200, api.get_info() if api else {})

        elif path == "/broker/status":
            self._json_ok(200, api.get_broker_status() if api else {})

        elif path == "/run":
            self._json_ok(200, api.get_run() if api else {"run_id": None, "state": "idle"})

        elif path in ("/run/status", "/status"):
            if path == "/status":
                self._log_deprecation("/status", "/run/status")
            self._json_ok(200, api.get_run_status() if api else {"run_id": None, "state": "idle"})

        elif path == "/run/config":
            if api:
                code, data = api.get_run_config()
            else:
                code, data = 404, {"message": "No run configuration available"}
            self._json_ok(code, data)

        elif path in ("/run/report", "/summary"):
            if path == "/summary":
                self._log_deprecation("/summary", "/run/report")
            if api:
                code, data = api.get_run_report()
            else:
                code, data = 404, {"message": "No completed run report available"}
            self._json_ok(code, data)

        else:
            self._json_ok(404, {"message": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?")[0]
        api = self.server.engine_api

        if path == "/run/start":
            body = self._read_json_body()
            if body is None:
                return
            if api:
                code, data = api.handle_run_start(body)
            else:
                code, data = 500, {"message": "Engine not initialized"}
            self._json_ok(code, data)

        elif path == "/run/stop":
            if api:
                code, data = api.handle_run_stop()
            else:
                code, data = 409, {"message": "No active run to stop", "state": "idle"}
            self._json_ok(code, data)

        elif path == "/cleanup":
            if api:
                code, data = api.handle_cleanup()
            else:
                code, data = 500, {"message": "Engine not initialized"}
            self._json_ok(code, data)

        else:
            self._json_ok(404, {"message": "not found"})

    # --- Helpers ---

    def _read_json_body(self) -> dict[str, Any] | None:
        """Read and parse JSON POST body. Returns None on error (response already sent)."""
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            return {}
        try:
            raw = self.rfile.read(content_length)
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            self._json_ok(400, {"message": f"Invalid JSON: {e}"})
            return None

    def _json_ok(self, code: int, data: Any) -> None:
        body = json.dumps(data, default=str).encode()
        self.send_response(code)
        self._add_cors_headers()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _add_cors_headers(self) -> None:
        origins = "*"
        if self.server.engine_api:
            origins = self.server.engine_api.get_cors_origins()
        self.send_header("Access-Control-Allow-Origin", origins)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    @staticmethod
    def _log_deprecation(old: str, new: str) -> None:
        if old not in _deprecation_warned:
            _deprecation_warned.add(old)
            logger.warning("deprecated endpoint %s used — use %s instead", old, new)

    def log_message(self, format: str, *args: Any) -> None:
        pass


class _BurninServer(ThreadingHTTPServer):
    """ThreadingHTTPServer subclass with engine API reference."""

    engine_api: EngineAPI | None = None
    daemon_threads = True


class BurninHTTPServer:
    """Burn-in HTTP server running in a daemon thread."""

    def __init__(
        self,
        port: int,
        engine_api: EngineAPI | None = None,
        summary_fn: Callable[[], Any] | None = None,
        status_fn: Callable[[], Any] | None = None,
    ) -> None:
        self._port = port
        self._server = _BurninServer(("0.0.0.0", port), _Handler)
        self._server.engine_api = engine_api
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="http-server",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()

    def set_engine_api(self, api: EngineAPI) -> None:
        self._server.engine_api = api

    def set_ready(self, ready: bool) -> None:
        pass
