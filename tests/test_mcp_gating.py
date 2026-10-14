"""Catalog gating regressions; no external servers or provider calls."""
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("gating_mcp_client", Path(__file__).resolve().parents[1] / "src/mcp_client.py")
mcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mcp)


def state():
    return SimpleNamespace(tool_schemas={}, pending_tool_calls=[], mcp_enabled_tools=[])


def call(router, session, name, **args):
    tc = SimpleNamespace(name=name, parsed_args=args, result=None, error=False, started_at=None, finished_at=None)
    session.pending_tool_calls = [tc]
    router(session)
    session.pending_tool_calls = []
    return tc


def tool(name, description="Inspect an artifact"):
    return {"name": name, "description": description, "inputSchema": {
        "type": "object", "properties": {"path": {"type": "string", "description": "SCHEMA_SENTINEL"}},
    }}


class GatingTests(unittest.TestCase):
    def setUp(self):
        self.registry = mcp.MCPClientRegistry()
        self.registry._load_config = Mock()
        self.registry._servers = {
            name: mcp.MCPServerConfig(name=name, command="unused") for name in ("rea", "other")
        }
        self.registry._clients = {name: Mock(tools_dirty=False) for name in self.registry._servers}
        for client in self.registry._clients.values():
            client.is_running.return_value = True
            client.protocol_version = "2026-07-28"
            client.last_error = None
        self.registry._schedule_discovery = Mock()
        self.registry._wait_for_discovery = Mock()
        self.registry._install_virtual_tools("rea", [tool("inspect"), tool("analyze", "Analyze assembly")])
        self.registry._install_virtual_tools("other", [tool("inspect")])
        self.router = mcp.MCPVirtualToolRouter(self.registry)
        self.session = state()
        self.addCleanup(self.registry.close_all)

    def test_default_is_only_management_tools(self):
        self.router(self.session)
        self.assertEqual(set(self.session.tool_schemas), {"mcp_status", "mcp_refresh", "search_mcp", "enable_mcp_tool"})
        self.assertNotIn("SCHEMA_SENTINEL", json.dumps(self.session.tool_schemas))

    def test_search_returns_names_not_schemas_or_enables(self):
        result = call(self.router, self.session, "search_mcp", query="REA inspect")
        self.assertFalse(result.error, result.result)
        self.assertEqual(result.result, "mcp_rea_inspect — Inspect an artifact")
        self.assertNotIn("SCHEMA_SENTINEL", result.result)
        self.assertEqual(self.session.mcp_enabled_tools, [])
        self.registry._schedule_discovery.assert_not_called()

    def test_search_text_only_adds_relevant_notices(self):
        paged = call(self.router, self.session, "search_mcp", limit=1)
        self.assertIn("More matches: use offset=1", paged.result)
        self.assertIn("3 total", paged.result)
        for internal in ('"matches"', '"server"', '"tool"', '"config_error"', "null", "idle"):
            self.assertNotIn(internal, paged.result)
        self.registry._install_virtual_tools("rea", [tool("inspect", "Line one.\n\n" + "word " * 100)])
        short = call(self.router, self.session, "search_mcp", server="rea")
        self.assertEqual(len(short.result.splitlines()), 1)
        self.assertTrue(short.result.endswith("…"))
        self.assertLessEqual(len(short.result), len("mcp_rea_inspect — ") + 200)
        self.registry._discovery_status["rea"] = "running"
        pending = call(self.router, self.session, "search_mcp", query="missing", server="rea")
        self.assertIn("No matching MCP tools", pending.result)
        self.assertIn("rea: discovery pending; retry search shortly.", pending.result)
        self.registry._discovery_status["rea"] = "error: timed out"
        self.registry._config_error = "invalid config"
        failed = call(self.router, self.session, "search_mcp", server="rea")
        self.assertIn("rea: discovery failed — timed out. See mcp_status.", failed.result)
        self.assertIn("Config error: invalid config", failed.result)
        self.registry._servers.clear()
        self.registry._config_error = None
        self.assertEqual(call(self.router, self.session, "search_mcp").result, "No MCP servers configured.")

    def test_status_is_a_server_summary_not_a_metadata_dump(self):
        call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        result = call(self.router, self.session, "mcp_status")
        self.assertFalse(result.error, result.result)
        self.assertEqual(result.result, "other: connected (MCP 2026-07-28); 1 tool; 0 enabled\nrea: connected (MCP 2026-07-28); 2 tools; 1 enabled")
        for internal in ("mcp_rea_inspect", "command", "cwd", "pid", "stderr", "instructions", "config_path"):
            self.assertNotIn(internal, result.result)
        self.registry._clients["rea"].status.assert_not_called()

    def test_status_retains_actionable_exception_details(self):
        self.registry._remove_server_virtuals("rea")
        self.registry._clients["rea"].is_running.return_value = False
        self.registry._discovery_status["rea"] = "error: timed out"
        self.registry._next_retry_at["rea"] = mcp.time.monotonic() + 30
        self.registry._config_error = "invalid config"
        result = call(self.router, self.session, "mcp_status")
        self.assertIn("rea: not connected; catalog not loaded; 0 enabled; error: timed out; retry in", result.result)
        self.assertIn("Config error: invalid config", result.result)
        self.registry._servers["other"].enabled = False
        self.assertIn("other: disabled", self.registry.status_text())

    def test_search_pagination_and_description_bound(self):
        self.registry._install_virtual_tools("rea", [tool(f"tool_{i:03}", "x" * 2000) for i in range(65)])
        first = self.registry.search_tools(server="rea", limit=50)
        self.assertEqual(len(first["matches"]), 50)
        self.assertEqual(first["next_offset"], 50)
        self.assertEqual(first["total"], 65)
        self.assertTrue(all(len(item["description"]) == 320 for item in first["matches"]))
        second = self.registry.search_tools(server="rea", offset=50)
        self.assertEqual(len(second["matches"]), 15)
        self.assertIsNone(second["next_offset"])
        self.assertEqual(self.registry.search_tools(server="rea", offset=100)["matches"], [])

    def test_search_discovers_missing_or_dirty_catalogs_only(self):
        self.registry._clients["rea"].tools_dirty = True
        self.registry.search_tools()
        self.registry._schedule_discovery.assert_called_once_with("rea")
        self.registry._wait_for_discovery.assert_called_with(["rea"], 1.0)
        self.registry._schedule_discovery.reset_mock()
        self.registry._clients["rea"].tools_dirty = False
        self.registry._remove_server_virtuals("rea")
        self.registry.search_tools(server="rea")
        self.registry._schedule_discovery.assert_called_once_with("rea")

    def test_invalid_search_arguments_are_errors(self):
        for args in ({"query": 3}, {"limit": 0}, {"limit": 51}, {"limit": True}, {"offset": -1}, {"server": "missing"}):
            with self.subTest(args=args):
                result = call(self.router, self.session, "search_mcp", **args)
                self.assertTrue(result.error)
        self.assertEqual(self.session.mcp_enabled_tools, [])

    def test_enable_is_additive_idempotent_and_schema_exact(self):
        result = call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        self.assertFalse(result.error, result.result)
        self.assertEqual(self.session.mcp_enabled_tools, ["mcp_rea_inspect"])
        self.assertEqual(self.session.tool_schemas["mcp_rea_inspect"]["schema"], self.registry.virtual_tools["mcp_rea_inspect"].schema)
        self.assertNotIn("mcp_rea_analyze", self.session.tool_schemas)
        call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect", "mcp_rea_inspect", "mcp_rea_analyze"])
        self.assertEqual(self.session.mcp_enabled_tools, ["mcp_rea_analyze", "mcp_rea_inspect"])
        result = call(self.router, self.session, "search_mcp", query="inspect", server="rea")
        self.assertEqual(result.result, "mcp_rea_inspect [enabled] — Inspect an artifact")

    def test_unknown_batch_is_atomic(self):
        result = call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect", "typo"])
        self.assertTrue(result.error)
        self.assertEqual(self.session.mcp_enabled_tools, [])
        self.assertNotIn("mcp_rea_inspect", self.session.tool_schemas)

    def test_invalid_enable_arguments_and_empty_noop(self):
        for names in (None, "mcp_rea_inspect", [42], [""]):
            with self.subTest(names=names):
                self.assertTrue(call(self.router, self.session, "enable_mcp_tool", tool=names).error)
        self.assertFalse(call(self.router, self.session, "enable_mcp_tool", tool=[]).error)
        self.assertEqual(self.session.mcp_enabled_tools, [])

    def test_direct_disabled_call_does_not_execute(self):
        result = call(self.router, self.session, "mcp_rea_inspect", path="example")
        self.assertTrue(result.error)
        self.assertIn("enable_mcp_tool", result.result)
        self.registry._clients["rea"].call_tool.assert_not_called()

    def test_enabled_call_routes_and_preserves_tool_error(self):
        client = self.registry._clients["rea"]
        client.call_tool.return_value = {"isError": True, "structuredContent": {"reason": "missing"}}
        call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        result = call(self.router, self.session, "mcp_rea_inspect", path="example")
        self.assertTrue(result.error)
        self.assertIn("missing", result.result)
        client.call_tool.assert_called_once_with("inspect", {"path": "example"})

    def test_sessions_do_not_share_selection(self):
        call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        second = state()
        self.router(second)
        self.assertNotIn("mcp_rea_inspect", second.tool_schemas)
        self.assertEqual(second.mcp_enabled_tools, [])

    def test_legacy_eager_schemas_removed_without_enabling(self):
        self.session.tool_schemas = {name: {"schema": item.schema, "available": True} for name, item in self.registry.virtual_tools.items()}
        self.session.tool_schemas["unrelated"] = {"schema": {"function": {"name": "unrelated"}}, "available": True}
        self.router(self.session)
        self.assertNotIn("mcp_rea_inspect", self.session.tool_schemas)
        self.assertIn("unrelated", self.session.tool_schemas)
        self.assertEqual(self.session.mcp_enabled_tools, [])

    def test_refresh_never_enables_new_tools_and_removes_old_schemas(self):
        call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        def refresh(**kwargs):
            self.registry._install_virtual_tools("rea", [tool("new_endpoint")])
            return "MCP STATUS"
        self.registry.refresh = refresh
        self.assertFalse(call(self.router, self.session, "mcp_refresh").error)
        self.assertNotIn("mcp_rea_inspect", self.session.tool_schemas)
        self.assertNotIn("mcp_rea_new_endpoint", self.session.tool_schemas)

    def test_foreign_tool_collision_rejects_enable(self):
        entry = {"schema": {"function": {"name": "mcp_rea_inspect", "description": "Another plugin"}}, "available": True}
        self.session.tool_schemas["mcp_rea_inspect"] = entry
        result = call(self.router, self.session, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        self.assertTrue(result.error)
        self.assertEqual(self.session.mcp_enabled_tools, [])
        self.assertIs(self.session.tool_schemas["mcp_rea_inspect"], entry)

    def test_control_names_cannot_be_shadowed(self):
        self.registry._servers["rea"].tool_prefix = ""
        self.registry._install_virtual_tools("rea", [tool("search_mcp"), tool("enable_mcp_tool")])
        self.assertNotIn("search_mcp", self.registry.virtual_tools)
        self.assertNotIn("enable_mcp_tool", self.registry.virtual_tools)

    def test_publish_never_discovers_or_starts_servers(self):
        self.registry._remove_server_virtuals("rea")
        self.router(self.session)
        self.registry._schedule_discovery.assert_not_called()
        self.registry._clients["rea"].start.assert_not_called()

    def test_stale_discovery_cannot_repopulate_removed_server(self):
        client = self.registry._clients["rea"]
        def removed():
            self.registry._clients.pop("rea")
            self.registry._servers.pop("rea")
            self.registry._remove_server_virtuals("rea")
            return [tool("obsolete")]
        client.list_tools.side_effect = removed
        self.registry._discover_server("rea", force=True)
        self.assertFalse(any(item.server_name == "rea" for item in self.registry.virtual_tools.values()))

    def test_stale_error_does_not_poison_replacement(self):
        client = self.registry._clients["rea"]
        def replaced():
            self.registry._clients["rea"] = Mock(tools_dirty=False)
            raise RuntimeError("old process failed")
        client.list_tools.side_effect = replaced
        self.registry._discover_server("rea", force=True)
        self.assertNotIn("rea", self.registry._next_retry_at)

    def test_foreign_tool_collision_leaves_call_for_its_handler(self):
        self.session.tool_schemas["mcp_rea_inspect"] = {
            "schema": {"function": {"name": "mcp_rea_inspect", "description": "Another plugin"}},
            "available": True,
        }
        for selected in ([], ["mcp_rea_inspect"]):
            self.session.mcp_enabled_tools = selected
            result = call(self.router, self.session, "mcp_rea_inspect", path="example")
            self.assertIsNone(result.result)
            self.assertFalse(result.error)
        self.registry._clients["rea"].call_tool.assert_not_called()

    def test_installed_extended_timeout_is_preserved(self):
        configs = mcp._normalize_config({"mcpServers": {"rea": {"command": "unused", "timeout_seconds": 120}}})
        self.assertEqual(configs["rea"].timeout_seconds, 120)

    def test_config_uses_state_home_not_install_root(self):
        with patch.dict(os.environ, {"AGENT_ZOO_HOME": "/state", "AGENT_ZOO_INSTALL_ROOT": "/code"}, clear=True):
            self.assertEqual(mcp._user_config_dir(), Path("/state/plugin-configs/azo-plugin-mcp-client"))
        with patch.dict(os.environ, {"AGENT_ZOO_STATE_ROOT": "/state2"}, clear=True):
            self.assertEqual(mcp._agent_zoo_data_root(), Path("/state2"))

    def test_canonical_session_restore_preserves_only_explicit_selection(self):
        from agent_utils.session import Session
        from agent_utils.serialization import capture_session_document
        original = Session()
        original.pipeline = [self.router]
        original.ensure_initialized()
        call(self.router, original.state, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        document = json.loads(json.dumps(capture_session_document(original.state, original.pipeline)))
        restored = Session()
        restored_router = mcp.MCPVirtualToolRouter(self.registry)
        restored.pipeline = [restored_router]
        self.registry._remove_server_virtuals("rea")
        restored.load_document(document)
        restored.ensure_initialized()
        restored_router(restored.state)
        self.assertEqual(restored.state.mcp_enabled_tools, ["mcp_rea_inspect"])
        self.assertNotIn("mcp_rea_inspect", restored.state.tool_schemas)
        self.registry._install_virtual_tools("rea", [tool("inspect"), tool("analyze")])
        restored_router(restored.state)
        self.assertIn("mcp_rea_inspect", restored.state.tool_schemas)
        self.assertNotIn("mcp_rea_analyze", restored.state.tool_schemas)
        self.assertEqual(len(restored.tool_schemas), 5)


if __name__ == "__main__":
    unittest.main()
