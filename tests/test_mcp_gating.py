"""Catalog gating regressions for the Sans-I/O event-driven MCP plugin."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from agent_utils.core.events import (
    InputReceived,
    IoInterrupted,
    RenderRequested,
    RunToolCall,
    SessionRestored,
    TurnSettled,
)
from agent_utils.core.pipeline import Feature, PipelineBuilder
from agent_utils.core.session import Session
from agent_utils.core.state import State, ToolCall
from agent_utils.core.tool import Tool
from agent_utils.features.core import core_feature
from agent_utils.runtime.loop import Runtime
from agent_utils.runtime.simulation import SimulationRuntime
from agent_utils.storage.serialization import capture_session_document, restore_session_document
from agent_zoo.testing.sans_io_guard import assert_pipeline_sans_io, audit_module_sans_io

SPEC = importlib.util.spec_from_file_location(
    "gating_mcp_client", Path(__file__).resolve().parents[1] / "src/mcp_client.py"
)
mcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mcp)


def build_mcp_session(
    registry: mcp.MCPClientRegistry | None = None,
    *,
    catalog: mcp.MCPCatalog | None = None,
    extra_features: list[Feature] | None = None,
    batch_deadline_s: float | None = None,
) -> tuple[Session, mcp.MCPCatalog, mcp.MCPClientDriver | None]:
    shared_catalog = catalog if catalog is not None else mcp.MCPCatalog(
        registry.virtual_tools if registry is not None else None
    )
    builder = (
        PipelineBuilder()
        .add(core_feature(batch_deadline_s=batch_deadline_s))
        .add(
            mcp.mcp_client_feature(
                catalog=shared_catalog,
                registry=registry,
                include_driver=registry is not None,
            )
        )
    )
    for feat in extra_features or ():
        builder.add(feat)
    pipeline = builder.build()
    session = Session(pipeline=pipeline)
    session.ensure_initialized()
    session.render()
    drivers = builder.build_drivers()
    driver = next((d for d in drivers if isinstance(d, mcp.MCPClientDriver)), None)
    return session, shared_catalog, driver


def run_tool_call(
    session: Session,
    driver: mcp.MCPClientDriver | None,
    name: str,
    **args,
) -> ToolCall:
    """Execute one tool call through the pure pipeline + driver (via SimulationRuntime)."""
    tc = ToolCall(
        id=f"call_{name}",
        name=name,
        raw_args=json.dumps(args),
        parsed_args=dict(args),
    )
    msg = {
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(args)},
            }
        ],
    }
    sim = SimulationRuntime(session)
    if driver is not None:
        sim.register_handler(
            mcp.MCPRequest,
            lambda _op_id, req, _sim: driver._dispatch_request(req),
        )
    sim.inject(InputReceived(message=msg))
    sim.run_until_idle()
    if session.state.last_tool_calls:
        return session.state.last_tool_calls[-1]
    return tc


def tool(name, description="Inspect an artifact"):
    return {
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "SCHEMA_SENTINEL"}},
        },
    }


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
        self.session, self.catalog, self.driver = build_mcp_session(self.registry)
        self.addCleanup(self.registry.close_all)

    def _call(self, name: str, **args) -> ToolCall:
        return run_tool_call(self.session, self.driver, name, **args)

    def _enabled(self, session: Session | None = None) -> list[str]:
        target = session if session is not None else self.session
        return target.state.durable("mcp_client", mcp.MCPClientState).enabled_tools

    def test_default_is_only_management_tools(self):
        self.session.render()
        self.assertEqual(
            set(self.session.state.tool_schemas),
            {"mcp_status", "mcp_refresh", "search_mcp", "enable_mcp_tool"},
        )
        self.assertNotIn("SCHEMA_SENTINEL", json.dumps(self.session.state.tool_schemas))

    def test_search_returns_names_not_schemas_or_enables(self):
        result = self._call("search_mcp", query="REA inspect")
        self.assertFalse(result.error, result.result)
        self.assertEqual(result.result, "mcp_rea_inspect — Inspect an artifact")
        self.assertNotIn("SCHEMA_SENTINEL", result.result)
        self.assertEqual(self._enabled(), [])
        self.registry._schedule_discovery.assert_not_called()

    def test_search_text_only_adds_relevant_notices(self):
        paged = self._call("search_mcp", limit=1)
        self.assertIn("More matches: use offset=1", paged.result)
        self.assertIn("3 total", paged.result)
        for internal in ('"matches"', '"server"', '"tool"', '"config_error"', "null", "idle"):
            self.assertNotIn(internal, paged.result)
        self.registry._install_virtual_tools("rea", [tool("inspect", "Line one.\n\n" + "word " * 100)])
        short = self._call("search_mcp", server="rea")
        self.assertEqual(len(short.result.splitlines()), 1)
        self.assertTrue(short.result.endswith("…"))
        self.assertLessEqual(len(short.result), len("mcp_rea_inspect — ") + 200)
        self.registry._discovery_status["rea"] = "running"
        pending = self._call("search_mcp", query="missing", server="rea")
        self.assertIn("No matching MCP tools", pending.result)
        self.assertIn("rea: discovery pending; retry search shortly.", pending.result)
        self.registry._discovery_status["rea"] = "error: timed out"
        self.registry._config_error = "invalid config"
        failed = self._call("search_mcp", server="rea")
        self.assertIn("rea: discovery failed — timed out. See mcp_status.", failed.result)
        self.assertIn("Config error: invalid config", failed.result)
        self.registry._servers.clear()
        self.registry._config_error = None
        self.assertEqual(self._call("search_mcp").result, "No MCP servers configured.")

    def test_status_is_a_server_summary_not_a_metadata_dump(self):
        self._call("enable_mcp_tool", tool=["mcp_rea_inspect"])
        result = self._call("mcp_status")
        self.assertFalse(result.error, result.result)
        self.assertEqual(
            result.result,
            "other: connected (MCP 2026-07-28); 1 tool; 0 enabled\n"
            "rea: connected (MCP 2026-07-28); 2 tools; 1 enabled",
        )
        for internal in ("mcp_rea_inspect", "command", "cwd", "pid", "stderr", "instructions", "config_path"):
            self.assertNotIn(internal, result.result)
        self.registry._clients["rea"].status.assert_not_called()

    def test_status_retains_actionable_exception_details(self):
        self.registry._remove_server_virtuals("rea")
        self.registry._clients["rea"].is_running.return_value = False
        self.registry._discovery_status["rea"] = "error: timed out"
        self.registry._next_retry_at["rea"] = mcp.time.monotonic() + 30
        self.registry._config_error = "invalid config"
        result = self._call("mcp_status")
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
                result = self._call("search_mcp", **args)
                self.assertTrue(result.error)
        self.assertEqual(self._enabled(), [])

    def test_enable_is_additive_idempotent_and_schema_exact(self):
        result = self._call("enable_mcp_tool", tool=["mcp_rea_inspect"])
        self.assertFalse(result.error, result.result)
        self.assertEqual(self._enabled(), ["mcp_rea_inspect"])
        self.assertEqual(
            self.session.state.tool_schemas["mcp_rea_inspect"]["schema"],
            self.registry.virtual_tools["mcp_rea_inspect"].schema,
        )
        self.assertNotIn("mcp_rea_analyze", self.session.state.tool_schemas)
        self._call("enable_mcp_tool", tool=["mcp_rea_inspect", "mcp_rea_inspect", "mcp_rea_analyze"])
        self.assertEqual(self._enabled(), ["mcp_rea_analyze", "mcp_rea_inspect"])
        result = self._call("search_mcp", query="inspect", server="rea")
        self.assertEqual(result.result, "mcp_rea_inspect [enabled] — Inspect an artifact")

    def test_unknown_batch_is_atomic(self):
        result = self._call("enable_mcp_tool", tool=["mcp_rea_inspect", "typo"])
        self.assertTrue(result.error)
        self.assertEqual(self._enabled(), [])
        self.assertNotIn("mcp_rea_inspect", self.session.state.tool_schemas)

    def test_invalid_enable_arguments_and_empty_noop(self):
        for names in (None, "mcp_rea_inspect", [42], [""]):
            with self.subTest(names=names):
                self.assertTrue(self._call("enable_mcp_tool", tool=names).error)
        self.assertFalse(self._call("enable_mcp_tool", tool=[]).error)
        self.assertEqual(self._enabled(), [])

    def test_direct_disabled_call_does_not_execute(self):
        result = self._call("mcp_rea_inspect", path="example")
        self.assertTrue(result.error)
        self.assertIn("enable_mcp_tool", result.result)
        self.registry._clients["rea"].call_tool.assert_not_called()

    def test_enabled_call_routes_and_preserves_tool_error(self):
        client = self.registry._clients["rea"]
        client.call_tool.return_value = {"isError": True, "structuredContent": {"reason": "missing"}}
        self._call("enable_mcp_tool", tool=["mcp_rea_inspect"])
        result = self._call("mcp_rea_inspect", path="example")
        self.assertTrue(result.error)
        self.assertIn("missing", result.result)
        client.call_tool.assert_called_once_with("inspect", {"path": "example"})

    def test_sessions_do_not_share_selection(self):
        self._call("enable_mcp_tool", tool=["mcp_rea_inspect"])
        second, _, _ = build_mcp_session(self.registry, catalog=self.catalog)
        second.render()
        self.assertNotIn("mcp_rea_inspect", second.state.tool_schemas)
        self.assertEqual(self._enabled(second), [])

    def test_legacy_eager_schemas_removed_without_enabling(self):
        publisher = mcp.MCPToolSchemaPublisher(self.catalog)
        state = State()
        state.durable(mcp.MCP_CLIENT_DURABLE_SPEC).enabled_tools = []
        state.tool_schemas = {
            name: {"schema": item.schema, "available": True}
            for name, item in self.registry.virtual_tools.items()
        }
        state.tool_schemas["unrelated"] = {"schema": {"function": {"name": "unrelated"}}, "available": True}
        publisher.handle(RenderRequested(), state, lambda _ev: None)
        self.assertNotIn("mcp_rea_inspect", state.tool_schemas)
        self.assertIn("unrelated", state.tool_schemas)
        self.assertEqual(state.durable("mcp_client", mcp.MCPClientState).enabled_tools, [])

    def test_refresh_never_enables_new_tools_and_removes_old_schemas(self):
        self._call("enable_mcp_tool", tool=["mcp_rea_inspect"])

        def refresh(**kwargs):
            self.registry._install_virtual_tools("rea", [tool("new_endpoint")])
            return "MCP STATUS"

        self.registry.refresh = refresh
        self.assertFalse(self._call("mcp_refresh").error)
        self.assertNotIn("mcp_rea_inspect", self.session.state.tool_schemas)
        self.assertNotIn("mcp_rea_new_endpoint", self.session.state.tool_schemas)

    def test_foreign_tool_collision_rejects_enable(self):
        class ForeignTool(Tool):
            name = "mcp_rea_inspect"
            description = "Another plugin"

            def call(self, state: State, path: str = "") -> str:
                return f"foreign:{path}"

        session, _, driver = build_mcp_session(
            self.registry,
            extra_features=[Feature(name="foreign_plugin", components=[ForeignTool()])],
        )
        entry = session.state.tool_schemas["mcp_rea_inspect"]
        result = run_tool_call(session, driver, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        self.assertTrue(result.error)
        self.assertEqual(self._enabled(session), [])
        self.assertEqual(session.state.tool_schemas["mcp_rea_inspect"], entry)

    def test_control_names_cannot_be_shadowed(self):
        self.registry._servers["rea"].tool_prefix = ""
        self.registry._install_virtual_tools("rea", [tool("search_mcp"), tool("enable_mcp_tool")])
        self.assertNotIn("search_mcp", self.registry.virtual_tools)
        self.assertNotIn("enable_mcp_tool", self.registry.virtual_tools)

    def test_publish_never_discovers_or_starts_servers(self):
        self.registry._remove_server_virtuals("rea")
        self.session.step(mcp.MCPCatalogUpdated(catalog=self.registry.virtual_tools))
        self.session.render()
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
        class ForeignTool(Tool):
            name = "mcp_rea_inspect"
            description = "Another plugin"

            def call(self, state: State, path: str = "") -> str:
                return f"foreign:{path}"

        session, _, driver = build_mcp_session(
            self.registry,
            extra_features=[Feature(name="foreign_plugin", components=[ForeignTool()])],
        )
        for selected in ([], ["mcp_rea_inspect"]):
            session.state.durable("mcp_client", mcp.MCPClientState).enabled_tools = list(selected)
            result = run_tool_call(session, driver, "mcp_rea_inspect", path="example")
            self.assertFalse(result.error)
            self.assertEqual(result.result, "foreign:example")
        self.registry._clients["rea"].call_tool.assert_not_called()

        # Also verify router skips a foreign schema even when invoked directly.
        router = mcp.MCPVirtualToolRouter(self.catalog)
        raw_state = State()
        raw_state.tool_schemas["mcp_rea_inspect"] = {
            "schema": {"function": {"name": "mcp_rea_inspect", "description": "Another plugin"}},
            "available": True,
        }
        raw_state.durable("mcp_client", mcp.MCPClientState).enabled_tools = ["mcp_rea_inspect"]
        tc = ToolCall(id="c1", name="mcp_rea_inspect", raw_args="{}", parsed_args={"path": "x"})
        emitted = []
        router.handle(RunToolCall(tc), raw_state, emitted.append)
        self.assertIsNone(tc.result)
        self.assertFalse(tc.error)
        self.assertEqual(emitted, [])

    def test_installed_extended_timeout_is_preserved(self):
        configs = mcp._normalize_config({"mcpServers": {"rea": {"command": "unused", "timeout_seconds": 120}}})
        self.assertEqual(configs["rea"].timeout_seconds, 120)

    def test_config_uses_state_home_not_install_root(self):
        with patch.dict(os.environ, {"AGENT_ZOO_HOME": "/state", "AGENT_ZOO_INSTALL_ROOT": "/code"}, clear=True):
            self.assertEqual(mcp._user_config_dir(), Path("/state/plugin-configs/azo-plugin-mcp-client"))
        with patch.dict(os.environ, {"AGENT_ZOO_STATE_ROOT": "/state2"}, clear=True):
            self.assertEqual(mcp._agent_zoo_data_root(), Path("/state2"))

    def test_canonical_session_restore_preserves_only_explicit_selection(self):
        original, _, driver_orig = build_mcp_session(self.registry)
        res = run_tool_call(original, driver_orig, "enable_mcp_tool", tool=["mcp_rea_inspect"])
        self.assertFalse(res.error, res.result)
        document = json.loads(json.dumps(capture_session_document(original.state, original.pipeline)))

        restored_catalog = mcp.MCPCatalog()
        restored, _, _ = build_mcp_session(catalog=restored_catalog)
        restore_session_document(document, restored.pipeline, state=restored.state)
        restored.step(SessionRestored())
        restored.render()
        self.assertEqual(self._enabled(restored), ["mcp_rea_inspect"])
        self.assertNotIn("mcp_rea_inspect", restored.state.tool_schemas)
        self.assertEqual(len(restored.tool_schemas), 4)

        restored.step(mcp.MCPCatalogUpdated(catalog=self.registry.virtual_tools))
        restored.render()
        self.assertIn("mcp_rea_inspect", restored.state.tool_schemas)
        self.assertNotIn("mcp_rea_analyze", restored.state.tool_schemas)
        self.assertEqual(len(restored.tool_schemas), 5)

    def test_driver_quiesce_and_router_io_interrupted_and_sans_io_guard(self):
        import threading

        # Verify explicit durability and Sans-I/O cleanliness.
        self.assertEqual(mcp.MCPClientDriver.durability, "process_bound")
        self.assertEqual(
            audit_module_sans_io(Path(__file__).resolve().parents[1] / "src/mcp_client.py"),
            [],
        )
        assert_pipeline_sans_io(
            self.session.pipeline,
            session=self.session,
            events=[RenderRequested(), SessionRestored(), TurnSettled()],
        )

        # Verify quiesce cancels in-flight tool calls and emits IoInterrupted.
        entered = threading.Event()
        release = threading.Event()

        def slow_call(tool_name, args):
            entered.set()
            release.wait(2.0)
            return {"content": [{"type": "text", "text": "late"}]}

        self.registry._clients["rea"].call_tool.side_effect = slow_call
        self._call("enable_mcp_tool", tool=["mcp_rea_inspect"])
        drv = mcp.MCPClientDriver(registry=self.registry)
        emitted = []
        drv.start(emitted.append)
        try:
            effects = self.session.step(InputReceived(message={
                "role": "assistant",
                "content": "",
                "tool_calls": [{
                    "id": "tc_int",
                    "type": "function",
                    "function": {"name": "mcp_rea_inspect", "arguments": "{}"},
                }],
            }))
            submit_eff = next(e for e in effects if isinstance(getattr(e, "request", None), mcp.MCPCallToolRequest))
            drv.submit(submit_eff.op_id, submit_eff.request)
            self.assertTrue(entered.wait(1.0))
            interrupted = drv.quiesce(policy="terminate")
            self.assertEqual(len(interrupted), 1)
            self.assertIsInstance(interrupted[0], IoInterrupted)
            self.assertEqual(interrupted[0].op_id, submit_eff.op_id)
            self.session.step(interrupted[0])
            tc = self.session.state.last_tool_calls[-1]
            self.assertTrue(tc.error)
            self.assertIn("interrupted", tc.result.lower())
            self.assertIsNone(tc.waiting_op)
            self.assertIsNone(tc._owner)

            # Also verify MCPVirtualToolRouter handles IoInterrupted directly.
            router = mcp.MCPVirtualToolRouter(self.catalog)
            raw_state = State()
            tc2 = ToolCall(id="tc_int2", name="mcp_rea_inspect", raw_args="{}", parsed_args={})
            tc2._owner = router
            tc2.waiting_op = "op_direct"
            raw_state.pending_tool_calls = [tc2]
            direct_emitted = []
            router.handle(IoInterrupted(op_id="op_direct", reason="quiesce:terminate"), raw_state, direct_emitted.append)
            self.assertTrue(tc2.error)
            self.assertIn("interrupted", tc2.result.lower())
            self.assertIsNone(tc2.waiting_op)
            self.assertIsNone(tc2._owner)
            self.assertEqual(len(direct_emitted), 1)
        finally:
            release.set()
            drv.close()
        self.assertTrue(self.registry._closed)

    def test_pure_simulation_and_driver_unsolicited_catalog_update(self):
        # Pure Sans-I/O test with SimulationRuntime: zero real registry or threads.
        catalog = mcp.MCPCatalog()
        session, _, _ = build_mcp_session(catalog=catalog)
        sim = SimulationRuntime(session)
        fake_vtool = mcp.MCPVirtualTool(
            virtual_name="mcp_sim_ping",
            server_name="sim",
            tool_name="ping",
            schema={
                "type": "function",
                "function": {
                    "name": "mcp_sim_ping",
                    "description": "[MCP:sim/ping] Ping tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            description="Ping tool",
        )
        seen_requests = []

        def handle_mcp(_op_id, req, _sim):
            seen_requests.append(type(req).__name__)
            if isinstance(req, mcp.MCPSearchRequest):
                return mcp.MCPSearchResult(
                    result={
                        "matches": [{"name": "mcp_sim_ping", "server": "sim", "tool": "ping", "description": "Ping tool"}],
                        "total": 1,
                        "next_offset": None,
                        "discovery": {"sim": "ok: 1 tool(s)"},
                        "config_error": None,
                    },
                    catalog={"mcp_sim_ping": fake_vtool},
                )
            if isinstance(req, mcp.MCPCallToolRequest):
                return mcp.MCPCallResult(text="MCP TOOL RESULT: sim/ping\npong")
            raise AssertionError(f"Unexpected request {req!r}")

        sim.register_handler(mcp.MCPRequest, handle_mcp)

        def inject_call(name, args):
            sim.inject(InputReceived(message={
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": f"tc_{name}", "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}],
            }))
            sim.run_until_idle()
            return session.state.last_tool_calls[-1]

        found = inject_call("search_mcp", {"query": "ping"})
        self.assertFalse(found.error, found.result)
        self.assertIn("mcp_sim_ping", found.result)
        enabled = inject_call("enable_mcp_tool", {"tool": ["mcp_sim_ping"]})
        self.assertFalse(enabled.error, enabled.result)
        called = inject_call("mcp_sim_ping", {})
        self.assertFalse(called.error, called.result)
        self.assertIn("pong", called.result)
        self.assertEqual(seen_requests, ["MCPSearchRequest", "MCPCallToolRequest"])

    def test_mcp_client_driver_runtime_lifecycle(self):
        catalog = mcp.MCPCatalog()
        session, _, driver = build_mcp_session(self.registry, catalog=catalog)
        assert driver is not None
        self.assertNotIn("mcp_rea_inspect", catalog)
        with Runtime(session, drivers=[driver]) as rt:
            # Driver pushes initial catalog snapshot on start.
            self.assertTrue(rt.step_once(timeout=1.0))
            self.assertIn("mcp_rea_inspect", catalog)
            # Trigger status request through real Runtime loop.
            rt.post(InputReceived(message={
                "role": "assistant",
                "content": "",
                "tool_calls": [{"id": "tc_st", "type": "function", "function": {"name": "mcp_status", "arguments": "{}"}}],
            }))
            self.assertTrue(rt.step_once(timeout=1.0))  # InputReceived -> submits MCPStatusRequest
            self.assertTrue(rt.step_once(timeout=2.0))  # IoCompleted -> finishes tool call & settles
            self.assertEqual(len(session.state.last_tool_calls), 1)
            self.assertFalse(session.state.last_tool_calls[0].error)
            self.assertIn("rea: connected", session.state.last_tool_calls[0].result)

    def test_register_features_composes_onto_default_builder(self):
        from agent_zoo.pipelines.default import build_default_builder

        session = Session()
        builder = build_default_builder(
            session,
            config={},
            include_plugins=False,
            include_llm_drivers=False,
        )
        mcp.register_features(builder, session=session, config={})
        pipeline = builder.build()
        drivers = builder.build_drivers(config={})
        session.pipeline = pipeline
        session.ensure_initialized()
        session.render()
        for tool_name in ("mcp_status", "mcp_refresh", "search_mcp", "enable_mcp_tool"):
            self.assertIn(tool_name, session.state.tool_schemas)
            self.assertTrue(session.state.tool_schemas[tool_name]["available"])
        self.assertTrue(any(isinstance(d, mcp.MCPClientDriver) for d in drivers))
        for d in drivers:
            if isinstance(d, mcp.MCPClientDriver):
                d.close()


if __name__ == "__main__":
    unittest.main()
