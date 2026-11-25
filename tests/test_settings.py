"""Offline /config registration and pure Sans-I/O plugin-owned persistence via StoreUpdate."""
import json
import os
from pathlib import Path
import queue
import tempfile
import unittest
from unittest.mock import patch

from agent_utils.core.events import IoCompleted, IoFailed
from agent_utils.core.io import SubmitOp
from agent_utils.core.pipeline import PipelineBuilder
from agent_utils.core.session import Session
from agent_utils.features.core import core_feature
from agent_zoo.durability import PluginStorage, PluginStoreDriver, StoreUpdate
from agent_zoo.events import ReplyFrontendRequest, SettingsRequest
from agent_zoo.features.settings import (
    Setting,
    SettingsContext,
    SettingsDispatcher,
    SettingsRegistry,
    settings_feature,
)
from agent_zoo.testing.sans_io_guard import assert_pipeline_sans_io
import mcp_client as mcp


def build_settings_session():
    session = Session()
    builder = PipelineBuilder().add(core_feature()).add(settings_feature())
    with patch.object(mcp.StdioMCPClient, "start", side_effect=AssertionError("no server")):
        mcp.register_features(builder, config={})
        session.pipeline = builder.build()
    session.ensure_initialized()
    assert_pipeline_sans_io(session.pipeline)
    dispatcher = next(c for c in session.pipeline.components if isinstance(c, SettingsDispatcher))
    return session, dispatcher


def dispatch_settings(session, *, action="list", store_driver=None, fail_persist=None, **kw):
    """Step a SettingsRequest through the pipeline and run any StoreUpdate via PluginStoreDriver."""
    ev = SettingsRequest(request_id="test", action=action, **kw)
    effects = session.step(ev)
    reply = None

    def _collect(effs):
        nonlocal reply
        next_events = []
        for eff in effs:
            if not isinstance(eff, SubmitOp):
                continue
            req = eff.request
            if isinstance(req, ReplyFrontendRequest):
                reply = req.payload
            elif isinstance(req, StoreUpdate):
                if fail_persist is not None:
                    next_events.append(IoFailed(op_id=eff.op_id, error=fail_persist))
                else:
                    drv = store_driver or PluginStoreDriver()
                    inbox: queue.Queue = queue.Queue()
                    drv.start(inbox.put)
                    try:
                        drv.submit(eff.op_id, req)
                        io_ev = inbox.get(timeout=5.0)
                    finally:
                        drv.close()
                    next_events.append(io_ev)
        return next_events

    pending_events = _collect(effects)
    while pending_events:
        ev2 = pending_events.pop(0)
        pending_events.extend(_collect(session.step(ev2)))
    assert reply is not None, "SettingsDispatcher did not emit ReplyFrontendRequest"
    return reply


def edit(session, listing, suffix, value, **kwargs):
    key = next(row["key"] for row in listing["settings"] if row["key"].endswith("." + suffix))
    return dispatch_settings(
        session,
        action="set",
        binding=listing["binding"],
        key=key,
        value=value,
        **kwargs,
    )


class SettingsTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.config_dir = self.root / "plugin-configs" / "azo-plugin-mcp-client"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.config_dir / "mcp_servers.json"
        self.enterContext(
            patch.dict(
                os.environ,
                {"AGENT_ZOO_HOME": str(self.root)},
                clear=False,
            )
        )
        os.environ.pop("AZO_MCP_CONFIG", None)

    def test_persist_preserves_private_fields_and_aliases(self):
        import yaml

        for suffix, server_key, timeout_key in (
            (".json", "mcpServers", "timeout_seconds"),
            (".yaml", "servers", "timeout"),
        ):
            with self.subTest(suffix=suffix):
                for existing in self.config_dir.glob("mcp_servers.*"):
                    existing.unlink()
                path = self.config_dir / ("mcp_servers" + suffix)
                original = {
                    server_key: {
                        "disabled server!": {
                            "enabled": False,
                            timeout_key: 1.125,
                            "command": "secret-command",
                            "args": ["private"],
                            "env": {"TOKEN": "secret-value"},
                            "url": "https://private.invalid",
                            "headers": {"Authorization": "secret"},
                            "custom": {"prompt": "leave untouched"},
                        }
                    },
                    "unknown": [1, "instruction"],
                }
                path.write_text(
                    json.dumps(original) if suffix == ".json" else yaml.safe_dump(original),
                    encoding="utf-8",
                )
                session, _ = build_settings_session()
                listing = dispatch_settings(session, action="list")
                self.assertTrue(listing["ok"])
                mcp_rows = [r for r in listing["settings"] if r["key"].startswith("mcp.")]
                self.assertEqual(len(mcp_rows), 2)
                self.assertEqual({row["kind"] for row in mcp_rows}, {"bool", "int"})
                self.assertTrue(all(row["apply"] == "live" for row in mcp_rows))
                self.assertNotIn("secret", json.dumps(listing))
                self.assertEqual({row["value"] for row in mcp_rows}, {False, 1125})

                res1 = edit(session, listing, "enabled", True)
                self.assertTrue(res1["ok"], res1)
                res2 = edit(session, res1, "timeout_ms", 2251)
                self.assertTrue(res2["ok"], res2)

                original[server_key]["disabled server!"]["enabled"] = True
                original[server_key]["disabled server!"][timeout_key] = 2.251
                self.assertEqual(yaml.safe_load(path.read_text(encoding="utf-8")), original)

                # Fresh pipeline reads persisted values cleanly.
                session2, _ = build_settings_session()
                listing2 = dispatch_settings(session2, action="list")
                mcp_rows2 = [r for r in listing2["settings"] if r["key"].startswith("mcp.")]
                self.assertEqual({r["value"] for r in mcp_rows2}, {True, 2251})

                # Invalid values are rejected without emitting StoreUpdate.
                self.assertFalse(edit(session2, listing2, "enabled", 1)["ok"])
                self.assertFalse(edit(session2, listing2, "timeout_ms", 1.2)["ok"])
                self.assertFalse(edit(session2, listing2, "timeout_ms", 999)["ok"])

    def test_defaults_and_store_update_preserves_config_filename(self):
        self.path.write_text('{"mcpServers":{"test":{"command":"unused"}}}', encoding="utf-8")
        session, _ = build_settings_session()
        listing = dispatch_settings(session, action="list")
        mcp_rows = [r for r in listing["settings"] if r["key"].startswith("mcp.")]
        self.assertEqual({r["value"] for r in mcp_rows}, {True, 15000})

        res = edit(session, listing, "enabled", False)
        self.assertTrue(res["ok"])
        self.assertEqual(
            json.loads(self.path.read_text(encoding="utf-8")),
            {"mcpServers": {"test": {"command": "unused", "enabled": False}}},
        )

    def test_malformed_never_overwritten(self):
        for bad in ('{"TOKEN":"secret",', "[]", "", '{"mcpServers":[]}'):
            with self.subTest(bad=bad):
                self.path.write_text(bad, encoding="utf-8")
                session, _ = build_settings_session()
                listing = dispatch_settings(session, action="list")
                mcp_rows = [r for r in listing["settings"] if r["key"].startswith("mcp.")]
                self.assertTrue(mcp_rows[0]["error"])
                self.assertFalse(mcp_rows[0]["writable"])
                self.assertNotIn("secret", json.dumps(listing))
                self.assertEqual(self.path.read_text(encoding="utf-8"), bad)

    def test_fractional_milliseconds_and_persist_failure_rollback(self):
        self.path.write_text(
            '{"mcpServers":{"test":{"command":"unused","timeout":1.0001}}}',
            encoding="utf-8",
        )
        session, _ = build_settings_session()
        listing = dispatch_settings(session, action="list")
        mcp_rows = [r for r in listing["settings"] if r["key"].startswith("mcp.")]
        row = next(r for r in mcp_rows if r["kind"] == "int")
        self.assertIsNone(row["value"])
        self.assertTrue(row["error"])

        before = self.path.read_bytes()
        failed = edit(
            session,
            listing,
            "enabled",
            False,
            fail_persist=PermissionError("secret"),
        )
        self.assertFalse(failed["ok"])
        self.assertNotIn("secret", json.dumps(failed))
        self.assertEqual(self.path.read_bytes(), before)
        # In-memory _SettingsBox rolled back to True after IoFailed!
        enabled_row = next(
            r for r in failed["settings"] if r["key"].startswith("mcp.") and r["kind"] == "bool"
        )
        self.assertTrue(enabled_row["value"])

    def test_unreadable_file_safe_error(self):
        self.path.mkdir()
        session, _ = build_settings_session()
        listing = dispatch_settings(session, action="list")
        mcp_rows = [r for r in listing["settings"] if r["key"].startswith("mcp.")]
        self.assertTrue(mcp_rows[0]["error"])

    def test_long_unicode_server_label(self):
        name = "server-" + "\u00e9" * 200
        self.path.write_text(
            json.dumps({"mcpServers": {name: {"command": "unused"}}}),
            encoding="utf-8",
        )
        session, _ = build_settings_session()
        listing = dispatch_settings(session, action="list")
        self.assertTrue(listing["ok"])
        res = edit(session, listing, "enabled", False)
        self.assertTrue(res["ok"])
        self.assertFalse(
            json.loads(self.path.read_text(encoding="utf-8"))["mcpServers"][name]["enabled"]
        )

    def test_persist_preserves_concurrent_sibling_server_edit_on_disk(self):
        self.path.write_text(
            json.dumps({
                "mcpServers": {
                    "alpha": {"command": "unused-a", "enabled": True},
                    "beta": {"command": "unused-b", "enabled": True},
                }
            }),
            encoding="utf-8",
        )
        session, _ = build_settings_session()
        listing = dispatch_settings(session, action="list")
        # Another process updates beta on disk after our PluginConfigBox loaded.
        disk_doc = json.loads(self.path.read_text(encoding="utf-8"))
        disk_doc["mcpServers"]["beta"]["enabled"] = False
        self.path.write_text(json.dumps(disk_doc), encoding="utf-8")

        alpha_key = next(
            row["key"]
            for row in listing["settings"]
            if row["label"].startswith("alpha:") and row["key"].endswith(".enabled")
        )
        res = dispatch_settings(
            session,
            action="set",
            binding=listing["binding"],
            key=alpha_key,
            value=False,
        )
        self.assertTrue(res["ok"], res)
        persisted = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertFalse(persisted["mcpServers"]["alpha"]["enabled"])
        self.assertFalse(persisted["mcpServers"]["beta"]["enabled"])


if __name__ == "__main__":
    unittest.main()
