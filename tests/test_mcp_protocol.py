"""Offline stdio protocol tests; servers are disposable Python subprocesses."""
from __future__ import annotations

import concurrent.futures
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("mcp_protocol_under_test", Path(__file__).resolve().parents[1] / "src/mcp_client.py")
mcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mcp)

CURRENT = "2026-07-28"
INFO = {"name": "fake-server", "version": "1.0"}
META_VERSION = "io.modelcontextprotocol/protocolVersion"
META_CAPS = "io.modelcontextprotocol/clientCapabilities"
META_INFO = "io.modelcontextprotocol/clientInfo"

# The fake checks the wire rather than mocking the client's request builder.
FAKE_SERVER = r'''
import json, sys
options = json.loads(sys.argv[1])
log = open(sys.argv[2], "a", encoding="utf-8", buffering=1)
info = {"name": "fake-server", "version": "1.0"}
modern = options.get("era", "modern") == "modern"
discover_count = 0

def emit(message):
    print(json.dumps(message), flush=True)

def reply(request, result=None, error=None):
    emit({"jsonrpc": "2.0", "id": request["id"], **({"error": error} if error is not None else {"result": result})})

for line in sys.stdin:
    message = json.loads(line)
    log.write(json.dumps(message) + "\n")
    method = message.get("method")
    if method is None or "id" not in message:
        continue
    if method == "server/discover":
        discover_count += 1
        if not modern:
            if options.get("silent_probe"):
                continue
            reply(message, error={"code": options.get("probe_error", -32601), "message": "not initialized"})
            continue
    if modern:
        meta = message.get("params", {}).get("_meta", {})
        if "io.modelcontextprotocol/protocolVersion" not in meta or "io.modelcontextprotocol/clientCapabilities" not in meta:
            reply(message, error={"code": -32602, "message": "missing required metadata"})
            continue
    if method == "server/discover":
        if discover_count > 1 and options.get("discover_after_error"):
            reply(message, error=options["discover_after_error"])
            continue
        if "discover_result" in options:
            reply(message, options["discover_result"])
            continue
        action = options.get("discover_action")
        if action == "exit":
            sys.exit(0)
        if action == "invalid_json":
            print("not-json", flush=True)
            continue
        if options.get("discover_error") and (not options.get("error_once") or discover_count == 1):
            reply(message, error=options["discover_error"])
            continue
        result = {"resultType": "complete", "supportedVersions": ["2026-07-28"],
                  "capabilities": {"tools": {}}, "instructions": "Fake server instructions",
                  "_meta": {"io.modelcontextprotocol/serverInfo": info}, "ttlMs": 0, "cacheScope": "private"}
        result.update(options.get("discover_update", {}))
        for key in options.get("discover_remove", []):
            result.pop(key, None)
        reply(message, result)
    elif method == "initialize":
        if options.get("silent_init"):
            continue
        if options.get("init_error"):
            reply(message, error={"code": -32603, "message": "failed handshake"})
            continue
        if options.get("init_ping"):
            emit({"jsonrpc": "2.0", "id": "server-ping", "method": "ping"})
            incoming = json.loads(sys.stdin.readline())
            log.write(json.dumps(incoming) + "\n")
            if incoming != {"jsonrpc": "2.0", "id": "server-ping", "result": {}}:
                sys.exit(3)
        result = {"protocolVersion": options.get("selected_version", "2025-11-25"),
                  "capabilities": options.get("capabilities", {"tools": {"listChanged": True}}),
                  "serverInfo": info, "instructions": "Legacy instructions"}
        for key in options.get("init_remove", []):
            result.pop(key, None)
        reply(message, result)
    elif method == "tools/list":
        cursor = message.get("params", {}).get("cursor")
        result = {"tools": [{"name": "second" if cursor else "first", "inputSchema": {"type": "object"}}]}
        if not cursor or options.get("repeat_cursor"):
            result["nextCursor"] = "page-two"
        reply(message, result)
    elif method == "tools/call":
        reply(message, options.get("call_result", {"content": [{"type": "text", "text": "fake only"}],
                                                  "structuredContent": message["params"].get("arguments", {}), "isError": True}))
    elif method == "ping":
        reply(message, {})
    elif method == "hang":
        continue
    elif method == "exit":
        sys.exit(0)
    elif method == "notify":
        emit({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
        reply(message, {})
    elif method == "wrong-ids":
        emit({"jsonrpc": "2.0", "id": str(message["id"]), "result": {"wrong": True}})
        emit({"jsonrpc": "2.0", "id": 1000000, "result": {"wrong": True}})
        reply(message, {"correct": True})
    elif method == "bad-envelope":
        emit({"jsonrpc": "1.0", "id": message["id"], "result": {}})
    else:
        reply(message, error={"code": -32601, "message": "unknown method", "data": {"method": method}})
'''


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.clients = []

    def client(self, **options):
        timeout = options.pop("timeout", 1.0)
        transcript = Path(self.temp.name) / f"wire-{len(self.clients)}.jsonl"
        config = mcp.MCPServerConfig(name="fake", command=sys.executable,
                                     args=("-u", "-c", FAKE_SERVER, json.dumps(options), str(transcript)),
                                     timeout_seconds=timeout)
        client = mcp.StdioMCPClient(config)
        self.clients.append(client)
        self.addCleanup(client.close)
        client.transcript = transcript
        return client

    def wire(self, client):
        if not client.transcript.exists():
            return []
        return [json.loads(line) for line in client.transcript.read_text().splitlines()]

    def methods(self, client):
        return [message.get("method") for message in self.wire(client)]

    def test_modern_discovery_metadata_pagination_and_fake_tool_result(self):
        client = self.client()
        self.assertEqual([tool["name"] for tool in client.list_tools()], ["first", "second"])
        result = client.call_tool("fake", {"value": 4})
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"], {"value": 4})
        self.assertEqual(client.protocol_version, CURRENT)
        self.assertEqual(client.server_info, INFO)
        self.assertEqual(client.server_instructions, "Fake server instructions")
        self.assertEqual(client.server_capabilities, {"tools": {}})
        self.assertEqual(client.discovery_result["ttlMs"], 0)
        self.assertEqual(client.status()["supported_versions"], [CURRENT])
        self.assertFalse(client.tools_dirty)
        for message in self.wire(client):
            if "id" not in message:
                continue
            meta = message["params"]["_meta"]
            self.assertEqual(meta[META_VERSION], CURRENT)
            self.assertEqual(meta[META_CAPS], {})
            self.assertEqual(meta[META_INFO]["name"], "agent-zoo-userspace-mcp-client")
        self.assertNotIn("initialize", self.methods(client))
        self.assertNotIn("notifications/initialized", self.methods(client))

    def test_metadata_merges_without_mutation_and_overrides_reserved_fields(self):
        client = self.client()
        client.start()
        params = {"_meta": {"example.com/trace": "test", META_VERSION: "bad", META_CAPS: {"sampling": {}}}}
        client._request("ping", params)
        self.assertEqual(params["_meta"][META_VERSION], "bad")
        sent = self.wire(client)[-1]["params"]["_meta"]
        self.assertEqual(sent[META_VERSION], CURRENT)
        self.assertEqual(sent[META_CAPS], {})
        self.assertEqual(sent["example.com/trace"], "test")

    def test_missing_optional_server_metadata_and_result_type_is_accepted(self):
        client = self.client(discover_remove=["_meta", "resultType", "instructions"])
        client.start()
        self.assertEqual(client.server_info, {})
        self.assertIsNone(client.server_instructions)

    def test_missing_required_discovery_fields_fail_without_fallback(self):
        for field in ("supportedVersions", "capabilities"):
            with self.subTest(field=field):
                client = self.client(discover_remove=[field])
                with self.assertRaises(mcp.MCPError):
                    client.start()
                self.assertNotIn("initialize", self.methods(client))
                self.assertIsNone(client.process)
                self.assertIsNotNone(client.last_error)

    def test_modern_unsupported_version_negotiation(self):
        # Simulate a future client preference to exercise a real retry to current.
        future = "2099-01-01"
        client = self.client(discover_error={"code": -32022, "message": "version mismatch",
                                            "data": {"supported": [CURRENT], "requested": future}}, error_once=True)
        with patch.object(mcp, "_MODERN_PROTOCOL_VERSIONS", (future, CURRENT)):
            client.start()
        versions = [m["params"]["_meta"][META_VERSION] for m in self.wire(client)]
        self.assertEqual(versions, [future, CURRENT])
        self.assertEqual(client.protocol_version, CURRENT)
        self.assertNotIn("initialize", self.methods(client))

    def test_modern_error_never_downgrades_even_when_only_legacy_is_offered(self):
        for code, data in ((-32020, None), (-32021, {"requiredCapabilities": {"elicitation": {}}}),
                           (-32022, {"supported": ["2025-11-25"], "requested": CURRENT}),
                           (-32022, {"supported": [CURRENT], "requested": CURRENT}),
                           (-32022, {})):
            with self.subTest(code=code, data=data):
                client = self.client(discover_error={"code": code, "message": "modern rejection", "data": data})
                with self.assertRaises(mcp.MCPError):
                    client.start()
                self.assertEqual(self.methods(client), ["server/discover"])
                self.assertIsNone(client.process)

    def test_successful_modern_probe_with_no_supported_version_does_not_fallback(self):
        for versions in (["2099-01-01"], ["2025-11-25"], [], "2026-07-28", [None]):
            with self.subTest(versions=versions):
                client = self.client(discover_update={"supportedVersions": versions})
                with self.assertRaises(mcp.MCPError):
                    client.start()
                self.assertNotIn("initialize", self.methods(client))

    def test_legacy_fallback_is_not_keyed_to_one_error(self):
        for code in (-32601, -32602, -32000, -32603, -32042):
            with self.subTest(code=code):
                client = self.client(era="legacy", probe_error=code)
                self.assertEqual(len(client.list_tools()), 2)
                self.assertEqual(client.protocol_era, "legacy")
                self.assertEqual(client.server_info, INFO)
                self.assertEqual(client.server_instructions, "Legacy instructions")
                methods = self.methods(client)
                self.assertEqual(methods[:3], ["server/discover", "initialize", "notifications/initialized"])
                init = self.wire(client)[1]
                self.assertEqual(init["params"]["protocolVersion"], "2025-11-25")
                self.assertEqual(init["params"]["capabilities"], {})
                for message in self.wire(client)[1:]:
                    self.assertNotIn("_meta", message.get("params", {}))

    def test_probe_timeout_falls_back_and_cancels_probe(self):
        client = self.client(era="legacy", silent_probe=True, timeout=0.15)
        client.start()
        client._request("ping")  # Ensure prior notification has been consumed.
        wire = self.wire(client)
        self.assertEqual(wire[1]["method"], "notifications/cancelled")
        self.assertEqual(wire[1]["params"]["requestId"], wire[0]["id"])
        self.assertIn("initialize", self.methods(client))
        self.assertEqual(client.protocol_era, "legacy")

    def test_legacy_server_selected_supported_versions(self):
        for version in mcp._LEGACY_PROTOCOL_VERSIONS:
            with self.subTest(version=version):
                client = self.client(era="legacy", selected_version=version)
                client.start()
                self.assertEqual(client.protocol_version, version)

    def test_legacy_unsupported_or_missing_version_disconnects(self):
        for version in ("2099-01-01", CURRENT, "", None):
            with self.subTest(version=version):
                client = self.client(era="legacy", selected_version=version)
                with self.assertRaisesRegex(mcp.MCPError, "unsupported legacy version"):
                    client.start()
                self.assertNotIn("notifications/initialized", self.methods(client))
                self.assertEqual(self.methods(client).count("initialize"), 1)
                self.assertIsNone(client.process)
                self.assertIsNone(client.protocol_version)

    def test_failed_handshake_is_not_retried_and_process_is_reaped(self):
        client = self.client(era="legacy", init_error=True)
        with self.assertRaises(mcp.MCPRPCError) as error:
            client.start()
        self.assertEqual(error.exception.code, -32603)
        self.assertIn("failed handshake", client.last_error)
        self.assertEqual(self.methods(client), ["server/discover", "initialize"])
        self.assertIsNone(client.process)

    def test_missing_legacy_metadata_fails(self):
        for field in ("serverInfo", "capabilities", "protocolVersion"):
            with self.subTest(field=field):
                client = self.client(era="legacy", init_remove=[field])
                with self.assertRaises(mcp.MCPError):
                    client.start()
                self.assertNotIn("notifications/initialized", self.methods(client))

    def test_legacy_ping_during_initialize_and_no_optional_capabilities(self):
        client = self.client(era="legacy", init_ping=True)
        client.start()
        self.assertEqual(client._request("ping"), {})
        replies = [m for m in self.wire(client) if m.get("id") == "server-ping"]
        self.assertEqual(replies, [{"jsonrpc": "2.0", "id": "server-ping", "result": {}}])
        with patch.object(client, "_send_raw") as send:
            client._handle_message({"jsonrpc": "2.0", "id": "sampling", "method": "sampling/createMessage"})
        self.assertEqual(send.call_args.args[0]["error"]["code"], -32601)

    def test_modern_does_not_reply_to_forbidden_server_request(self):
        client = self.client()
        client.start()
        with patch.object(client, "_send_raw") as send:
            client._handle_message({"jsonrpc": "2.0", "id": 2, "method": "ping"})
        send.assert_not_called()

    def test_optional_tools_not_advertised_legacy(self):
        client = self.client(era="legacy", capabilities={})
        self.assertEqual(client.list_tools(), [])
        with self.assertRaisesRegex(mcp.MCPError, "does not advertise tools"):
            client.call_tool("not-real", {})
        self.assertNotIn("tools/list", self.methods(client))
        self.assertNotIn("tools/call", self.methods(client))

    def test_legacy_list_change_notification(self):
        client = self.client(era="legacy")
        client.list_tools()
        self.assertFalse(client.tools_dirty)
        client._request("notify")
        self.assertTrue(client.tools_dirty)

    def test_modern_unknown_or_input_required_results_fail_explicitly(self):
        for result_type in ("input_required", "extension-unknown", None):
            with self.subTest(result_type=result_type):
                client = self.client(call_result={"resultType": result_type})
                with self.assertRaisesRegex(mcp.MCPError, "unsupported resultType"):
                    client.call_tool("fake", {})

    def test_timeout_cancellation_and_late_unknown_response_cleanup(self):
        client = self.client()
        client.start()
        with self.assertRaises(mcp.MCPTimeoutError):
            client._request("hang", timeout=0.04)
        client._request("ping")
        wire = self.wire(client)
        hang = next(m for m in wire if m.get("method") == "hang")
        cancel = next(m for m in wire if m.get("method") == "notifications/cancelled")
        self.assertEqual(cancel["params"]["requestId"], hang["id"])
        client._handle_message({"jsonrpc": "2.0", "id": hang["id"], "result": {}})
        self.assertEqual(client._pending, set())
        self.assertEqual(client._responses, {})

    def test_process_exit_fails_promptly_instead_of_waiting_full_timeout(self):
        client = self.client(timeout=3)
        client.start()
        start = time.monotonic()
        with self.assertRaisesRegex(mcp.MCPError, "closed"):
            client._request("exit")
        self.assertLess(time.monotonic() - start, 1.5)

    def test_failed_probe_transport_does_not_trigger_legacy(self):
        client = self.client(discover_action="exit")
        with self.assertRaises(mcp.MCPError):
            client.start()
        self.assertEqual(self.methods(client), ["server/discover"])
        self.assertIsNone(client.process)

    def test_close_wakes_inflight_request_and_clears_metadata(self):
        client = self.client(timeout=3)
        client.start()
        proc = client.process
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(client._request, "hang")
            deadline = time.monotonic() + 1
            while "hang" not in self.methods(client) and time.monotonic() < deadline:
                time.sleep(0.01)
            client.close()
            with self.assertRaises(mcp.MCPError):
                future.result(timeout=1)
        self.assertIsNotNone(proc.poll())
        self.assertEqual(client._responses, {})
        self.assertEqual(client.server_info, {})
        self.assertIsNone(client.protocol_version)

    def test_concurrent_start_spawns_once_and_restart_reprobes(self):
        client = self.client()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: client.start(), range(4)))
        self.assertEqual(self.methods(client), ["server/discover"])
        first = client.process
        client.close()
        client.start()
        self.assertIsNot(client.process, first)
        self.assertEqual(self.methods(client), ["server/discover", "server/discover"])
        client._handle_message({"jsonrpc": "2.0", "id": 1, "result": {}}, first)
        self.assertEqual(client._responses, {})

    def test_response_ids_are_exact_and_unsolicited_responses_do_not_accumulate(self):
        client = self.client()
        client.start()
        self.assertEqual(client._request("wrong-ids"), {"correct": True})
        self.assertEqual(client._responses, {})
        with self.assertRaisesRegex(mcp.MCPError, "Malformed JSON-RPC response"):
            client._request("bad-envelope")

    def test_rpc_error_fields_preserved(self):
        client = self.client()
        client.start()
        with self.assertRaises(mcp.MCPRPCError) as error:
            client._request("unknown-test")
        self.assertEqual(error.exception.code, -32601)
        self.assertEqual(error.exception.data, {"method": "unknown-test"})
        self.assertEqual(error.exception.message, "unknown method")

    def test_repeated_pagination_cursor_fails_boundedly(self):
        client = self.client(repeat_cursor=True)
        with self.assertRaisesRegex(mcp.MCPError, "repeated cursor"):
            client.list_tools()
        self.assertEqual(self.methods(client).count("tools/list"), 2)

    def test_identified_modern_server_does_not_downgrade_after_retry_error(self):
        future = "2099-01-01"
        client = self.client(discover_error={"code": -32022, "message": "version mismatch",
                                            "data": {"supported": [CURRENT], "requested": future}}, error_once=True,
                             discover_after_error={"code": -32602, "message": "modern invalid params"})
        with patch.object(mcp, "_MODERN_PROTOCOL_VERSIONS", (future, CURRENT)):
            with self.assertRaises(mcp.MCPRPCError):
                client.start()
        self.assertEqual(self.methods(client), ["server/discover", "server/discover"])

    def test_malformed_discovery_success_or_error_does_not_trigger_fallback(self):
        for options in ({"discover_result": []}, {"discover_result": {}},
                        {"discover_update": {"_meta": []}},
                        {"discover_error": {"code": -32021}}):
            with self.subTest(options=options):
                client = self.client(**options)
                with self.assertRaises(mcp.MCPError):
                    client.start()
                self.assertEqual(self.methods(client), ["server/discover"])

    def test_legacy_initialize_timeout_is_not_cancelled_or_retried(self):
        client = self.client(era="legacy", silent_init=True, timeout=0.15)
        with self.assertRaises(mcp.MCPTimeoutError):
            client.start()
        self.assertEqual(self.methods(client), ["server/discover", "initialize"])
        self.assertIsNone(client.process)

    def test_missing_required_request_metadata_is_rejected_by_fake_server(self):
        client = self.client()
        client.start()
        # Deliberately bypass modern metadata to test the independent wire oracle.
        client.protocol_era = "legacy"
        with self.assertRaises(mcp.MCPRPCError) as error:
            client._request("ping")
        self.assertEqual(error.exception.code, -32602)

    def test_command_start_failure_is_actionable(self):
        client = mcp.StdioMCPClient(mcp.MCPServerConfig(name="missing", command="/definitely/missing/mcp-server"))
        with self.assertRaisesRegex(mcp.MCPError, "failed to start MCP server missing"):
            client.start()
        self.assertIsNone(client.process)
        self.assertIsNotNone(client.last_error)


if __name__ == "__main__":
    unittest.main()
