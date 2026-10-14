# Stdio protocol support

Verified against the official current MCP revision, **2026-07-28**, on **October 14, 2026**. The plugin supports that modern revision plus legacy `2025-11-25`, `2025-06-18`, `2025-03-26`, and `2024-11-05`. Runtime implementation remains in `src/mcp_client.py` and uses the standard library for transport.

## Wire behavior

- Each new subprocess is probed with `server/discover`. Modern requests carry `io.modelcontextprotocol/protocolVersion`, `io.modelcontextprotocol/clientInfo`, and an empty `io.modelcontextprotocol/clientCapabilities` object in **`params._meta`**, including methods with no other parameters.
- Successful discovery selects a mutually supported modern version from `supportedVersions`. The discovery result and server capabilities/instructions are retained as diagnostic snapshots. Optional server identity comes from `result._meta["io.modelcontextprotocol/serverInfo"]`; its absence is valid. Identity is not trusted for authorization or behavior selection.
- Modern errors `-32020`, `-32021`, and `-32022` establish the modern era. `-32022` uses `error.data.supported` and `error.data.requested`; retries are bounded by distinct supported modern versions and a shared request deadline. No mutually supported modern version is an error, not permission to initialize a legacy session.
- A non-modern JSON-RPC error **of any code**, or an unanswered initial discovery probe, falls back to legacy `initialize`. Once modern behavior is identified, later errors cannot trigger that downgrade. Transport failure, malformed success, or malformed error also fail explicitly rather than silently downgrading.
- Legacy initialization offers the latest supported legacy version once. A server-selected supported legacy version is accepted; missing or unsupported selections are rejected. Only successful initialization sends `notifications/initialized`. Legacy metadata is not replaced with modern per-request fields.
- Both eras retain server information, capabilities, and optional instructions. Legacy base `ping` is answered even during initialization, without advertising optional capabilities. Modern stdio forbids server requests and client responses; unsolicited requests are ignored, not answered using the legacy protocol.
- Unknown or unsupported modern `resultType` values fail explicitly. An absent discriminator is treated as `complete` for compatibility. Multi-round-trip input handling is not advertised or implemented.

## Lifecycle and scope

Startup is serialized. Failed handshakes close the subprocess; readers and pending replies are isolated by process generation. EOF/exit wakes waiting requests, late or unsolicited responses do not accumulate, request IDs are matched exactly, and timeouts send cancellation notifications (except legacy initialization, which cannot be cancelled). Shutdown closes stdin, waits, escalates to termination/kill if necessary, and reaps the process. Tool pagination remains supported and rejects repeated cursors.

The plugin does not implement HTTP/authentication, sampling, elicitation, roots, subscriptions, task extensions, or discovery-result reuse governed by TTL/cache scope. It does not claim those optional client capabilities. Modern discovery metadata is a snapshot, not a cross-request authorization cache. Server instructions are available for diagnostics but are not automatically injected into model prompts. Registry refresh/publication and provider behavior are separate concerns.

## Offline verification

`tests/test_mcp_protocol.py` uses disposable Python stdio fake servers and stdlib `unittest`. It covers modern metadata/discovery, version rejection/retry, error classification, legacy fallback (including timeout), selected-version validation, missing metadata, legacy ping, malformed results, cancellation, process exit/close/restart, concurrent startup, pagination, and preservation of fake structured/error tool results. It executes no real MCP tools and needs no credentials. A live REA interoperability test is not part of this validation.

## Authoritative references

- Current revision: https://modelcontextprotocol.io/specification/versioning
- Versioning and compatibility: https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning
- Stdio binding: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio
- Base metadata, errors, and result types: https://modelcontextprotocol.io/specification/2026-07-28/basic
- Discovery: https://modelcontextprotocol.io/specification/2026-07-28/server/discover
- Schema source: https://raw.githubusercontent.com/modelcontextprotocol/modelcontextprotocol/main/schema/2026-07-28/schema.ts
- Cancellation: https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/cancellation
- Legacy lifecycle: https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle
- Legacy ping: https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/ping

The formerly referenced modern `basic/lifecycle` URL redirects to `basic/versioning`; modern lifecycle details are now split across versioning and the transport binding. Sources were fetched directly with `urllib.request`, including the site's Markdown endpoints and the official schema, because the web tool supplied no usable body.
