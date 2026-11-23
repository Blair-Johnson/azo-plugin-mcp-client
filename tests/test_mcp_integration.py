"""Protocol and schema gate together, using disposable stdio servers through Runtime + MCPClientDriver."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

from agent_utils.core.events import InputReceived
from agent_utils.runtime.loop import Runtime

from test_mcp_gating import build_mcp_session, mcp
from test_mcp_protocol import FAKE_SERVER


def _runtime_call(rt: Runtime, session, name: str, **args):
    step_before = session.state.step
    rt.post(InputReceived(message={
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": f"tc_{name}_{step_before}",
                "type": "function",
                "function": {"name": name, "arguments": json.dumps(args)},
            }
        ],
    }))
    while session.state.step == step_before:
        if not rt.step_once(timeout=10.0):
            raise TimeoutError(f"Timed out waiting for tool call {name}")
    return session.state.last_tool_calls[-1]


class IntegrationTests(unittest.TestCase):
    def test_discover_search_enable_call_in_both_protocol_eras(self):
        for era in ("modern", "legacy"):
            with self.subTest(era=era), tempfile.TemporaryDirectory() as directory:
                wire_path = Path(directory) / "wire.jsonl"
                options = {"era": era, "call_result": {"content": [{"type": "text", "text": "fake success"}]}}
                registry = mcp.MCPClientRegistry()
                registry._load_config = Mock()
                registry._servers = {
                    "fake": mcp.MCPServerConfig(
                        name="fake",
                        command=sys.executable,
                        args=("-u", "-c", FAKE_SERVER, json.dumps(options), str(wire_path)),
                        timeout_seconds=2,
                    )
                }
                session, _, driver = build_mcp_session(registry, batch_deadline_s=None)
                assert driver is not None
                with Runtime(session, drivers=[driver]) as rt:
                    session.render()
                    self.assertEqual(len(session.state.tool_schemas), 4)

                    refreshed = _runtime_call(rt, session, "mcp_refresh", wait_seconds=5)
                    self.assertFalse(refreshed.error, refreshed.result)
                    self.assertEqual(registry._clients["fake"].protocol_era, era)
                    self.assertEqual(len(registry.virtual_tools), 2)
                    self.assertEqual(len(session.state.tool_schemas), 4)

                    found = _runtime_call(rt, session, "search_mcp", query="first")
                    self.assertFalse(found.error, found.result)
                    self.assertEqual(found.result, "mcp_fake_first — MCP tool first from server fake")
                    self.assertEqual(len(session.state.tool_schemas), 4)

                    disabled_call = _runtime_call(rt, session, "mcp_fake_first")
                    self.assertTrue(disabled_call.error)

                    enabled = _runtime_call(rt, session, "enable_mcp_tool", tool=["mcp_fake_first"])
                    self.assertFalse(enabled.error, enabled.result)
                    self.assertEqual(len(session.state.tool_schemas), 5)
                    self.assertNotIn("mcp_fake_second", session.state.tool_schemas)

                    result = _runtime_call(rt, session, "mcp_fake_first", example=42)
                    self.assertFalse(result.error, result.result)
                    self.assertIn("fake success", result.result)

                    wire = [json.loads(line) for line in wire_path.read_text().splitlines()]
                    endpoint_calls = [item for item in wire if item.get("method") == "tools/call"]
                    self.assertEqual(len(endpoint_calls), 1)
                    self.assertEqual(endpoint_calls[0]["params"]["name"], "first")
                    self.assertEqual(endpoint_calls[0]["params"]["arguments"], {"example": 42})


if __name__ == "__main__":
    unittest.main()
