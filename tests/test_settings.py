"""Offline /config registration and safe plugin-owned persistence."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from agent_utils import Feature, PipelineBuilder, Session
from agent_utils.builder import flatten
from agent_zoo.runtime.settings import Setting, SettingsContext, SettingsRegistry
import mcp_client as mcp


def registry():
    session = Session()
    builder = PipelineBuilder(session)
    builder.add(Feature("test_tool_slot", order=[mcp.Tool]))
    with patch.object(mcp.StdioMCPClient, "start", side_effect=AssertionError("no server")):
        mcp.register_features(builder, session=session, config={})
        session.pipeline = builder.build()
    rows = [c for c in flatten(session.pipeline) if isinstance(c, Setting)]
    return SettingsRegistry(rows), SettingsContext(session)


def request(reg, ctx, action="list", **kw):
    return reg.handle({"type": "plugin_settings", "request_id": "test", "action": action, **kw}, ctx)


def edit(reg, ctx, suffix, value):
    key = next(key for key in reg.settings if key.endswith("." + suffix))
    return request(reg, ctx, "set", binding=reg.binding, key=key, value=value)


class SettingsTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.path = self.root / "config.json"
        self.enterContext(patch.dict(os.environ, {"AZO_MCP_CONFIG": str(self.path)}))

    def test_persist_preserves_private_fields_and_aliases(self):
        import yaml
        for suffix, server_key, timeout_key in (
            (".json", "mcpServers", "timeout_seconds"), (".yaml", "servers", "timeout"),
        ):
            with self.subTest(suffix=suffix):
                path = self.root / ("servers" + suffix)
                original = {server_key: {"disabled server!": {
                    "enabled": False, timeout_key: 1.125, "command": "secret-command",
                    "args": ["private"], "env": {"TOKEN": "secret-value"},
                    "url": "https://private.invalid", "headers": {"Authorization": "secret"},
                    "custom": {"prompt": "leave untouched"},
                }}, "unknown": [1, "instruction"]}
                path.write_text(json.dumps(original) if suffix == ".json" else yaml.safe_dump(original))
                path.chmod(0o640)
                os.environ["AZO_MCP_CONFIG"] = str(path)
                reg, ctx = registry()
                result = request(reg, ctx)
                self.assertTrue(result["ok"])
                self.assertEqual(len(result["settings"]), 2)
                self.assertEqual({row["kind"] for row in result["settings"]}, {"bool", "int"})
                self.assertTrue(all(row["apply"] == "live" for row in result["settings"]))
                self.assertNotIn("secret", json.dumps(result))
                self.assertEqual({row["value"] for row in result["settings"]}, {False, 1125})
                self.assertTrue(edit(reg, ctx, "enabled", True)["ok"])
                self.assertTrue(edit(reg, ctx, "timeout_ms", 2251)["ok"])
                original[server_key]["disabled server!"]["enabled"] = True
                original[server_key]["disabled server!"][timeout_key] = 2.251
                self.assertEqual(yaml.safe_load(path.read_text()), original)
                self.assertEqual(path.stat().st_mode & 0o777, 0o640)
                reg, ctx = registry()  # fresh pipeline reads persisted values
                self.assertEqual({r["value"] for r in request(reg, ctx)["settings"]}, {True, 2251})
                self.assertFalse(edit(reg, ctx, "enabled", 1)["ok"])
                self.assertFalse(edit(reg, ctx, "timeout_ms", 1.2)["ok"])
                self.assertFalse(edit(reg, ctx, "timeout_ms", 999)["ok"])

    def test_defaults_and_current_override_path(self):
        self.path.write_text('{"mcpServers":{"test":{"command":"unused"}}}')
        second = self.root / "second.json"
        second.write_text(self.path.read_text())
        reg, ctx = registry()
        self.assertEqual({r["value"] for r in request(reg, ctx)["settings"]}, {True, 15000})
        os.environ["AZO_MCP_CONFIG"] = str(second)
        self.assertTrue(edit(reg, ctx, "enabled", False)["ok"])
        self.assertNotIn("enabled", json.loads(self.path.read_text())["mcpServers"]["test"])
        self.assertEqual(json.loads(second.read_text()),
                         {"mcpServers": {"test": {"command": "unused", "enabled": False}}})

    def test_malformed_never_overwritten(self):
        for bad in ('{"TOKEN":"secret",', '[]', '', '{"mcpServers":[]}'):
            with self.subTest(bad=bad):
                self.path.write_text('{"mcpServers":{"test":{"command":"unused"}}}')
                reg, ctx = registry()
                self.path.write_text(bad)
                result = edit(reg, ctx, "enabled", False)
                self.assertFalse(result["ok"])
                self.assertEqual(self.path.read_text(), bad)
                self.assertTrue(all(row["error"] for row in result["settings"]))
                self.assertNotIn("secret", json.dumps(result))
                reg, ctx = registry()
                result = request(reg, ctx)
                self.assertTrue(result["settings"][0]["error"])
                self.assertFalse(result["settings"][0]["writable"])

    def test_fractional_milliseconds_and_write_failure(self):
        self.path.write_text('{"mcpServers":{"test":{"command":"unused","timeout":1.0001}}}')
        reg, ctx = registry()
        row = next(r for r in request(reg, ctx)["settings"] if r["kind"] == "int")
        self.assertIsNone(row["value"])
        self.assertTrue(row["error"])
        before = self.path.read_bytes()
        with patch.object(mcp.os, "replace", side_effect=PermissionError("secret")):
            result = edit(reg, ctx, "enabled", False)
        self.assertFalse(result["ok"])
        self.assertNotIn("secret", json.dumps(result))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertFalse(list(self.root.glob(".mcp-settings-*")))

    def test_unreadable_file_safe_error(self):
        self.path.mkdir()
        reg, ctx = registry()
        self.assertTrue(request(reg, ctx)["settings"][0]["error"])

    def test_symlink_and_long_unicode_server_label(self):
        target = self.root / "target.json"
        name = "server-" + "\u00e9" * 200
        target.write_text(json.dumps({"mcpServers": {name: {"command": "unused"}}}))
        self.path.symlink_to(target)
        reg, ctx = registry()
        self.assertTrue(request(reg, ctx)["ok"])
        self.assertTrue(edit(reg, ctx, "enabled", False)["ok"])
        self.assertTrue(self.path.is_symlink())
        self.assertFalse(json.loads(target.read_text())["mcpServers"][name]["enabled"])


if __name__ == "__main__":
    unittest.main()
