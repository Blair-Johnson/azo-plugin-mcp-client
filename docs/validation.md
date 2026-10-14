# MCP upgrade and endpoint-gate validation

Validated October 14, 2026 in the `mcp-tool-gating` worktree on branch `feature/mcp-tool-gating`. The protocol implementation was delegated to an RLM in its own `mcp-current-protocol` worktree and cherry-picked after review. Neither the main checkout nor the installed plugin/configuration was changed.

## Combined offline verification

Rx run `r6`, command `c6`, independent integration root, tags `mcp,integration`, succeeded with exit 0 in 3.91 seconds. The installed `agent-zoo-plugins` Pixi Python environment ran 54 tests: 22 gate tests, 31 protocol tests, and one end-to-end test exercising both modern and legacy disposable stdio servers. The static plugin inspector reported no warnings; isolated default and RLM pipeline builds both registered the plugin and initialized selection state.

The checks cover default-hidden schemas, lightweight bounded search, explicit atomic/idempotent enablement, disabled-call rejection, foreign-tool name collisions, legacy eager-schema cleanup, session isolation and canonical capture/restore, refreshed schema removal, stale discovery rejection, modern metadata/version errors, legacy negotiation, cancellation, shutdown/restart/concurrency, exact response IDs, malformed responses, and pagination. The end-to-end test verifies that only an enabled endpoint reaches `tools/call`.

Run `r3` / `c2` was the earlier 20-test gate pass, a child of `r2`; `r2` failed solely because its persistence test called the host's `tool_schemas` property as a method. Protocol-worker runs `r1` and `r4` independently passed 27 and 31 tests. Run `r5` / `c5` independently checked both pipelines before integration. The authoritative merged offline result is `r6`.

## Read-only real REA verification

Rx run `r7`, command `c7`, independent root, tags `mcp,rea,catalog`, succeeded with exit 0 in 2.10 seconds. `tests/probe_catalog.py` used a separate bounded client with the installed REA configuration. It initialized/discovered the server and listed tools, then enabled a schema only in transient local session state. No REA endpoint or model request was executed, and the server configuration file was not modified.

REA selected modern protocol `2026-07-28`. The catalog contained 207 tools whose model-facing schemas serialized to 897,528 bytes. Before enablement, the model-visible registry contained four management schemas and zero endpoint schemas. Enabling `mcp_rea_address_name` made exactly one endpoint visible, for five schemas total. All other endpoint schemas remained absent. The independent client was closed at probe completion.

This establishes live modern REA discovery and gate behavior. It does not establish that every REA endpoint/schema is compatible with every model provider, nor does it repair the separate core Responses-adapter bug that accepts failed/incomplete turns as successful answers.

## Reproduction

From an Agent Zoo Pixi environment with `agent_utils`, run `python -m unittest discover -s tests -v` and `python tests/check_pipeline.py`. The latter uses a temporary plugin/state root and does not install anything. The explicit live discovery probe is `python tests/probe_catalog.py --config /path/to/mcp_servers.json --server rea`; it is not part of unittest discovery.

Rx retains immutable source snapshots and command logs. Use `rx show r6` and `rx show r7` from this repository to locate them. The runtime environment is external to those snapshots.
