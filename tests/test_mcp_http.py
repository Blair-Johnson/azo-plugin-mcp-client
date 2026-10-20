"""Streamable HTTP MCP transport and config regression tests."""
from __future__ import annotations

import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import os
import threading
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    "mcp_http_under_test", Path(__file__).resolve().parents[1] / "src/mcp_client.py"
)
mcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mcp)


class FakeHTTPMCPHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    requests: list[dict] = []

    def log_message(self, format, *args):
        return

    @classmethod
    def reset(cls):
        cls.requests = []

    def _record(self, message):
        self.__class__.requests.append({
            "message": message,
            "authorization": self.headers.get("Authorization"),
            "accept": self.headers.get("Accept"),
            "content_type": self.headers.get("Content-Type"),
            "method_header": self.headers.get("Mcp-Method"),
            "protocol_version": self.headers.get("MCP-Protocol-Version"),
            "session_id": self.headers.get("Mcp-Session-Id"),
        })

    def _write_json(self, status, message, *, session_id=None):
        body = json.dumps(message).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if session_id is not None:
            self.send_header("Mcp-Session-Id", session_id)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_sse(self, message):
        payload = json.dumps(message, indent=2)
        body = "".join(f"data: {line}\n" for line in payload.splitlines()) + "\n"
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _write_empty(self, status):
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        message = json.loads(self.rfile.read(length))
        self._record(message)
        method = message.get("method")
        if method == "server/discover":
            self._write_json(400, {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32601, "message": "legacy initialize required"},
            })
        elif method == "initialize":
            self._write_json(200, {
                "jsonrpc": "2.0",
                "id": message["id"],
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {"listChanged": True}},
                    "serverInfo": {"name": "fake-http", "version": "1.0"},
                },
            }, session_id="session-1")
        elif method == "notifications/initialized":
            self._write_empty(202)
        elif method == "tools/list":
            self._write_sse({
                "jsonrpc": "2.0",
                "id": message["id"],
                "result": {"tools": [
                    {"name": "first", "description": "HTTP first", "inputSchema": {"type": "object"}},
                    {"name": "second", "description": "HTTP second", "inputSchema": {"type": "object"}},
                ]},
            })
        elif method == "tools/call":
            self._write_json(200, {
                "jsonrpc": "2.0",
                "id": message["id"],
                "result": {
                    "content": [{"type": "text", "text": "HTTP success"}],
                    "structuredContent": message.get("params", {}).get("arguments", {}),
                },
            })
        else:
            self._write_json(400, {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32601, "message": f"unknown method {method}"},
            })


class HTTPServerMixin:
    @classmethod
    def setUpClass(cls):
        FakeHTTPMCPHandler.reset()
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), FakeHTTPMCPHandler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.httpd.server_address[1]}/mcp"

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=2)


class HTTPConfigTests(unittest.TestCase):
    def test_http_and_stdio_configs_normalize_without_cross_contamination(self):
        with patch.dict(os.environ, {"MCP_TEST_TOKEN": "secret-token"}, clear=False):
            configs = mcp._normalize_config({"mcpServers": {
                "fetch": {
                    "type": "http",
                    "url": "https://example.test/mcp",
                    "headers": {"Authorization": "Bearer $MCP_TEST_TOKEN"},
                },
                "local": {"command": "python", "args": ["-m", "server"]},
            }})
        self.assertEqual(configs["fetch"].transport, "http")
        self.assertEqual(configs["fetch"].url, "https://example.test/mcp")
        self.assertEqual(configs["fetch"].headers["Authorization"], "Bearer secret-token")
        self.assertEqual(configs["local"].transport, "stdio")
        self.assertEqual(configs["local"].command, "python")
        self.assertEqual(configs["local"].args, ("-m", "server"))

    def test_tls_options_are_explicit_and_secure_by_default(self):
        default = mcp._normalize_config({"mcpServers": {
            "default": {"type": "http", "url": "https://example.test/mcp"},
        }})["default"]
        self.assertTrue(default.verify_ssl)
        self.assertIsNone(default.ca_file)

        configured = mcp._normalize_config({"mcpServers": {
            "internal": {
                "type": "http",
                "url": "https://internal.test/mcp",
                "ca_file": "~/certs/internal.pem",
            },
        }})["internal"]
        self.assertTrue(configured.verify_ssl)
        self.assertTrue(configured.ca_file.endswith("/certs/internal.pem"))

        insecure = mcp._normalize_config({"mcpServers": {
            "insecure": {
                "type": "http", "url": "https://example.test/mcp", "verify_ssl": False,
            },
        }})["insecure"]
        self.assertFalse(insecure.verify_ssl)
        self.assertIsNone(insecure.ca_file)

        conflict = mcp._normalize_config({"mcpServers": {
            "conflict": {
                "type": "http", "url": "https://example.test/mcp",
                "verify_ssl": False, "ca_file": "/tmp/ca.pem",
            },
        }})
        self.assertNotIn("conflict", conflict)

    def test_http_config_identity_includes_url_and_headers(self):
        first = mcp.MCPServerConfig(
            name="fetch", transport="http", url="https://one.test/mcp", headers={"X-Key": "one"}
        )
        second = mcp.MCPServerConfig(
            name="fetch", transport="http", url="https://two.test/mcp", headers={"X-Key": "two"}
        )
        self.assertNotEqual(first, second)

    def test_legacy_sse_is_rejected_without_affecting_other_servers(self):
        configs = mcp._normalize_config({"mcpServers": {
            "legacy": {"type": "sse", "url": "https://example.test/events"},
            "local": {"command": "python"},
        }})
        self.assertNotIn("legacy", configs)
        self.assertIn("local", configs)


class HTTPClientTests(HTTPServerMixin, unittest.TestCase):
    def setUp(self):
        FakeHTTPMCPHandler.reset()
        self.client = mcp.StreamableHTTPMCPClient(mcp.MCPServerConfig(
            name="fetch",
            transport="http",
            url=self.url,
            headers={"Authorization": "Bearer test-token"},
            timeout_seconds=2,
        ))
        self.addCleanup(self.client.close)

    def test_discovery_session_headers_sse_listing_and_json_call(self):
        tools = self.client.list_tools()
        self.assertEqual([tool["name"] for tool in tools], ["first", "second"])
        result = self.client.call_tool("first", {"value": 4})
        self.assertEqual(result["structuredContent"], {"value": 4})
        self.assertEqual(self.client.protocol_era, "legacy")
        self.assertEqual(self.client.protocol_version, "2025-03-26")
        self.assertEqual(self.client.status()["transport"], "http")
        self.assertTrue(self.client.status()["session_bound"])

        methods = [item["message"].get("method") for item in FakeHTTPMCPHandler.requests]
        self.assertEqual(methods, [
            "server/discover", "initialize", "notifications/initialized", "tools/list", "tools/call",
        ])
        discover, initialize, initialized, listing, call = FakeHTTPMCPHandler.requests
        self.assertEqual(discover["authorization"], "Bearer test-token")
        self.assertEqual(discover["accept"], "application/json, text/event-stream")
        self.assertEqual(discover["content_type"], "application/json")
        self.assertEqual(discover["method_header"], "server/discover")
        self.assertEqual(initialize["method_header"], "initialize")
        self.assertEqual(initialized["method_header"], "notifications/initialized")
        self.assertEqual(listing["method_header"], "tools/list")
        self.assertEqual(call["method_header"], "tools/call")
        self.assertEqual(discover["protocol_version"], "2026-07-28")
        self.assertIsNone(discover["session_id"])
        self.assertIsNone(initialize["protocol_version"])
        self.assertIsNone(initialize["session_id"])
        self.assertEqual(initialized["protocol_version"], "2025-03-26")
        self.assertEqual(initialized["session_id"], "session-1")
        self.assertEqual(listing["protocol_version"], "2025-03-26")
        self.assertEqual(listing["session_id"], "session-1")
        self.assertEqual(call["session_id"], "session-1")


class HTTPRegistryTests(HTTPServerMixin, unittest.TestCase):
    def test_registry_factory_discovers_and_routes_http_endpoint(self):
        registry = mcp.MCPClientRegistry()
        registry._load_config = Mock()
        registry._servers = {"fetch": mcp.MCPServerConfig(
            name="fetch", transport="http", url=self.url, timeout_seconds=2,
        )}
        self.addCleanup(registry.close_all)
        status = registry.refresh(server="fetch", force=True, wait_seconds=3)
        self.assertIn("fetch: connected", status)
        self.assertIn("mcp_fetch_first", registry.virtual_tools)
        result = registry.call_virtual_tool("mcp_fetch_first", {"query": "test"})
        self.assertIn("MCP TOOL RESULT: fetch/first", result)
        self.assertIn("HTTP success", result)


if __name__ == "__main__":
    unittest.main()
