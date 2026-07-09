# azo-plugin-mcp-client

Userspace agent-zoo plugin that publishes MCP server tools as virtual agent-zoo tools and routes calls over MCP.

Install with:

```bash
azo-plugin install /path/to/azo-plugin-mcp-client
```

Install initializes `~/.local/share/agent-zoo/plugin-configs/azo-plugin-mcp-client/mcp_servers.json` from the default `mcp_servers.json` if it is missing. Edit that installed config, or set `AZO_MCP_CONFIG`, to configure MCP servers. Normal uninstall keeps the config; use `azo-plugin uninstall azo-plugin-mcp-client --purge-config` for full removal.
