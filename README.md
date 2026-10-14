# azo-plugin-mcp-client

Userspace Agent Zoo plugin for configured stdio MCP servers. Discovery fills a local catalog; endpoint schemas remain hidden until explicitly enabled for the current session. No native provider tool-search feature is required.

## Install and configure

```bash
azo-plugin install /path/to/azo-plugin-mcp-client
```

Installation initializes `<state-home>/plugin-configs/azo-plugin-mcp-client/mcp_servers.json` only if missing. The default state home is `~/.local/share/agent-zoo`; `AGENT_ZOO_HOME` and then `AGENT_ZOO_STATE_ROOT` override it. `AGENT_ZOO_INSTALL_ROOT` is not a state/config root. Set `AZO_MCP_CONFIG` to select a specific config file. Both `mcpServers` and `servers` config keys are supported. Normal uninstall preserves config; `--purge-config` removes it. Run `/reload` after installation to rebuild the plugin pipeline.

## Search, enable, call

```python
search_mcp(query="inspect", server="rea")
enable_mcp_tool(tool=["mcp_rea_inspect_binary"])
# Then call the exact endpoint using its newly exposed schema.
```

The endpoint name above is illustrative: use exact names returned by `search_mcp`. A fresh session exposes only `search_mcp`, `enable_mcp_tool`, `mcp_status`, and `mcp_refresh` from this plugin. Search matches case-insensitive words across server names, endpoint names, and descriptions; it returns short descriptions, not input schemas. Empty `query` lists the catalog. Results default to 20 per page, with `limit` capped at 50 and a next-page offset shown only when more matches remain. Search output is one callable name and concise description per line; already enabled endpoints are marked. Status summarizes each server's availability, protocol, catalog count, and enabled count, adding discovery/error/backoff details only when relevant. The enablement acknowledgment remains a small structured object.

Search schedules discovery for missing or dirty catalogs and waits at most one second. A slow server may still be pending; retry search or call `mcp_refresh(server="rea", wait_seconds=10)`. Refresh does not enable anything and still returns immediately by default. Normal schema publication does not start servers or perform MCP requests.

`enable_mcp_tool(tool: list[str])` is additive and idempotent. Unknown names reject the whole batch. Enabling never executes endpoints. Guessed calls to disabled endpoints are rejected. Enabled names are session state, not a global setting; they survive save/load and become available after rediscovery. New sessions start empty. Refresh removes vanished schemas, but retains explicit selections for endpoints that return. There is deliberately no automatic enable-all behavior.

Upgrading an old session removes its previously eager MCP schema advertisements; they do not count as explicit selections. Unrelated tools are preserved. Config changes discard stale in-flight discovery results rather than installing tools from a replaced server.

## Protocol support

The stdio client supports MCP `2026-07-28` with per-request metadata and `server/discover`, plus validated legacy initialization for older servers. It retains server metadata, handles cancellation and process cleanup, and checks pagination cursors. See `docs/protocol.md` for the verified compatibility rules and intentionally unsupported optional features. HTTP/auth transports are not part of this stdio upgrade.
## Development checks

Tests use standard-library `unittest`, fake clients/servers, and the host's `agent_utils`. Run them from an Agent Zoo Pixi environment that provides the plugin runtime:

```bash
pixi run --manifest-path /path/to/agent-zoo/pixi.toml -e agent-zoo-plugins python -m unittest discover -s tests -v
```

The plugin does not implement a general-purpose tool-search system or rely on embeddings. Explicitly enabling a large set can still produce a large model request; enable only the endpoints needed for the task.
