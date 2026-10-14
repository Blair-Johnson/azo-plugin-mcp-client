"""Read-only configured-server probe: discover and enable a schema, never tools/call."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

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
    session = SimpleNamespace(tool_schemas={}, pending_tool_calls=[], mcp_enabled_tools=[])
    router = mcp.MCPVirtualToolRouter(registry)
    router(session)
    before = len(session.tool_schemas)
    assert before == 4, session.tool_schemas.keys()
    found = registry.search_tools(server=args.server, limit=3)
    assert found["matches"]
    selected = found["matches"][0]["name"]
    call = SimpleNamespace(
        name="enable_mcp_tool", parsed_args={"tool": [selected]},
        result=None, error=False, started_at=None, finished_at=None,
    )
    session.pending_tool_calls = [call]
    router(session)
    assert not call.error, call.result
    after = len(session.tool_schemas)
    assert after == 5 and selected in session.tool_schemas
    assert set(catalog) & session.tool_schemas.keys() == {selected}
    client = registry._clients[args.server]
    print(json.dumps({
        "kind": "final", "server": args.server, "protocol_version": client.protocol_version,
        "protocol_era": client.protocol_era, "catalog_tools": len(catalog),
        "catalog_schema_bytes": len(json.dumps([item.schema for item in catalog.values()]).encode()),
        "visible_before": before, "visible_after": after, "selected": selected,
        "endpoint_calls": 0,
    }))
finally:
    registry.close_all()
