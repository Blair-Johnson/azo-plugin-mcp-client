"""Read-only configured-server probe: discover and enable a schema, never tools/call."""
import argparse
import importlib.util
import json
import os
from pathlib import Path

from agent_utils.core.events import InputReceived
from agent_utils.core.pipeline import PipelineBuilder
from agent_utils.core.session import Session
from agent_utils.features.core import core_feature
from agent_utils.runtime.simulation import SimulationRuntime

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--config", required=True)
parser.add_argument("--server", required=True)
args = parser.parse_args()
os.environ["AZO_MCP_CONFIG"] = str(Path(args.config).expanduser())
spec = importlib.util.spec_from_file_location("mcp_catalog_probe", Path(__file__).resolve().parents[1] / "src/mcp_client.py")
mcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcp)
registry = mcp.MCPClientRegistry()
try:
    registry._load_config(force=True)
    config = registry._servers.get(args.server)
    if config is None or not config.enabled:
        raise RuntimeError("Requested server is not configured and enabled")
    # Bound this independent diagnostic client; never change the config file.
    config.timeout_seconds = min(config.timeout_seconds, 10)
    registry._discover_server(args.server, force=True)
    catalog = {name: tool for name, tool in registry.virtual_tools.items() if tool.server_name == args.server}
    if not catalog:
        raise RuntimeError(f"No catalog: {registry._discovery_status.get(args.server)}")
    shared_catalog = mcp.MCPCatalog(registry.virtual_tools)
    pipeline = (
        PipelineBuilder()
        .add(core_feature(batch_deadline_s=None))
        .add(mcp.mcp_client_feature(catalog=shared_catalog, include_driver=False))
        .build()
    )
    session = Session(pipeline=pipeline)
    session.ensure_initialized()
    session.render()
    before = len(session.state.tool_schemas)
    assert before == 4, session.state.tool_schemas.keys()
    found = registry.search_tools(server=args.server, limit=3)
    assert found["matches"]
    selected = found["matches"][0]["name"]
    sim = SimulationRuntime(session)
    sim.inject(InputReceived(message={
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": "probe_enable",
                "type": "function",
                "function": {"name": "enable_mcp_tool", "arguments": json.dumps({"tool": [selected]})},
            }
        ],
    }))
    sim.run_until_idle()
    call = session.state.last_tool_calls[-1]
    assert not call.error, call.result
    after = len(session.state.tool_schemas)
    assert after == 5 and selected in session.state.tool_schemas
    assert set(catalog) & session.state.tool_schemas.keys() == {selected}
    client = registry._clients[args.server]
    print(json.dumps({
        "kind": "final",
        "server": args.server,
        "protocol_version": client.protocol_version,
        "protocol_era": client.protocol_era,
        "catalog_tools": len(catalog),
        "catalog_schema_bytes": len(json.dumps([item.schema for item in catalog.values()]).encode()),
        "visible_before": before,
        "visible_after": after,
        "selected": selected,
        "endpoint_calls": 0,
    }))
finally:
    registry.close_all()
