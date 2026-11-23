"""Dry-build host pipelines against this checkout, without live installation."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import agent_zoo
from agent_utils.core.session import Session
from agent_zoo.pipelines.default import build_default_builder
from agent_zoo.pipelines.rlm import build_default_rlm_builder, build_readonly_rlm_builder
import mcp_client as mcp

host = Path(agent_zoo.__file__).resolve().parents[1]
source = Path(__file__).resolve().parents[1] / "src/mcp_client.py"
static_script = host / "skills/agent-zoo-userspace-plugins/scripts/static_inspect_plugin.py"
if static_script.exists():
    subprocess.run([sys.executable, str(static_script), str(source)], check=True)

with tempfile.TemporaryDirectory(prefix="mcp-plugin-check-") as temporary:
    root = Path(temporary)
    config_file = root / "mcp.json"
    config_file.write_text(json.dumps({"mcpServers": {"offline": {"command": "unused", "enabled": False}}}))
    old_env = os.environ.get("AZO_MCP_CONFIG")
    os.environ["AZO_MCP_CONFIG"] = str(config_file)
    try:
        for name, make_builder in (
            ("default", build_default_builder),
            ("rlm", build_default_rlm_builder),
            ("rlm_readonly", build_readonly_rlm_builder),
        ):
            session = Session()
            builder = make_builder(
                session,
                config={},
                include_plugins=False,
                include_llm_drivers=False,
            )
            mcp.register_features(builder, session=session, config={})
            assert builder.has("mcp_client_addon"), name
            assert builder.has("mcp_settings"), name
            pipeline = builder.build()
            drivers = builder.build_drivers(config={})
            try:
                assert any(isinstance(d, mcp.MCPClientDriver) for d in drivers), (name, drivers)
                session.pipeline = pipeline
                session.ensure_initialized()
                session.render()
                assert hasattr(session.state, "mcp_enabled_tools"), name
                for tool_name in ("mcp_status", "mcp_refresh", "search_mcp", "enable_mcp_tool"):
                    assert tool_name in session.state.tool_schemas, (name, tool_name)
                print(f"{name}: MCP plugin registered; session selection state initialized")
            finally:
                for d in drivers:
                    if isinstance(d, mcp.MCPClientDriver):
                        d.close()
    finally:
        if old_env is None:
            os.environ.pop("AZO_MCP_CONFIG", None)
        else:
            os.environ["AZO_MCP_CONFIG"] = old_env
