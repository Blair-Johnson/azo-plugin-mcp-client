"""Protocol and schema gate together, using disposable stdio servers only."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock

from test_mcp_gating import call, mcp, state
from test_mcp_protocol import FAKE_SERVER


class IntegrationTests(unittest.TestCase):
    def test_discover_search_enable_call_in_both_protocol_eras(self):
        for era in ("modern", "legacy"):
            with self.subTest(era=era), tempfile.TemporaryDirectory() as directory:
                wire_path = Path(directory) / "wire.jsonl"
                options = {"era": era, "call_result": {"content": [{"type": "text", "text": "fake success"}]}}
                registry = mcp.MCPClientRegistry()
                registry._load_config = Mock()
                registry._servers = {"fake": mcp.MCPServerConfig(
                    name="fake", command=sys.executable,
                    args=("-u", "-c", FAKE_SERVER, json.dumps(options), str(wire_path)),
                    timeout_seconds=2,
                )}
                try:
                    router = mcp.MCPVirtualToolRouter(registry)
                    session = state()
                    router(session)
                    self.assertEqual(len(session.tool_schemas), 4)
                    refreshed = call(router, session, "mcp_refresh", wait_seconds=5)
                    self.assertFalse(refreshed.error, refreshed.result)
                    self.assertEqual(registry._clients["fake"].protocol_era, era)
                    self.assertEqual(len(registry.virtual_tools), 2)
                    self.assertEqual(len(session.tool_schemas), 4)
                    found = call(router, session, "search_mcp", query="first")
                    self.assertFalse(found.error, found.result)
                    self.assertEqual(found.result, "mcp_fake_first — MCP tool first from server fake")
                    self.assertEqual(len(session.tool_schemas), 4)
                    self.assertTrue(call(router, session, "mcp_fake_first").error)
                    enabled = call(router, session, "enable_mcp_tool", tool=["mcp_fake_first"])
                    self.assertFalse(enabled.error, enabled.result)
                    self.assertEqual(len(session.tool_schemas), 5)
                    self.assertNotIn("mcp_fake_second", session.tool_schemas)
                    result = call(router, session, "mcp_fake_first", example=42)
                    self.assertFalse(result.error, result.result)
                    self.assertIn("fake success", result.result)
                    wire = [json.loads(line) for line in wire_path.read_text().splitlines()]
                    endpoint_calls = [item for item in wire if item.get("method") == "tools/call"]
                    self.assertEqual(len(endpoint_calls), 1)
                    self.assertEqual(endpoint_calls[0]["params"]["name"], "first")
                    self.assertEqual(endpoint_calls[0]["params"]["arguments"], {"example": 42})
                finally:
                    registry.close_all()


if __name__ == "__main__":
    unittest.main()
