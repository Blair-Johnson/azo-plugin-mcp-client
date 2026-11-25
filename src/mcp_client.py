"""Userspace MCP client addon for agent-zoo (Sans-I/O event-driven architecture).

This plugin discovers MCP tools into a searchable catalog, publishes only
session-enabled endpoints, and routes their calls over MCP ``tools/call``.
Discovery is explicit or background-only so unreliable servers cannot stall turns.

Architecture:
- Pure Sans-I/O pipeline layer:
  * Typed ``MCP*Request`` IO requests (subclasses of ``IoRequest``) and
    ``MCPCatalogUpdated`` event (subclass of ``Event``).
  * Four management ``Tool`` subclasses (``mcp_status``, ``mcp_refresh``,
    ``search_mcp``, ``enable_mcp_tool``) that yield ``MCP*Request``s or mutate
    session state purely without performing OS/network/subprocess IO.
  * ``MCPVirtualToolRouter`` component (ordered after ``Tool``, before
    ``UnknownToolFallback``) routing enabled virtual ``mcp_*`` endpoint calls
    via ``MCPCallToolRequest``.
  * ``MCPToolSchemaPublisher`` component (ordered after ``SchemaPublisher``)
    publishing only explicitly enabled virtual MCP schemas onto
    ``state.tool_schemas`` and stripping legacy eager MCP schemas.
- External IO runtime layer:
  * ``MCPClientDriver`` (subclass of ``agent_zoo.durability.ThreadPoolDriver``),
    contributed via ``Feature(..., drivers=[...])``, owning ``MCPClientRegistry``,
    ``StdioMCPClient``, ``StreamableHTTPMCPClient``, config loading, background
    discovery threads, and clean shutdown on ``close()``.

Configuration defaults to the installed user config at
``plugin-configs/azo-plugin-mcp-client/mcp_servers.json`` with the plugin source
config as a development fallback, and can be overridden with ``AZO_MCP_CONFIG``.
Both the common MCP ``mcpServers`` shape and a ``servers`` alias are accepted:

{
  "mcpServers": {
    "filesystem": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]
    }
  }
}

The implementation intentionally uses only the Python standard library at
runtime. YAML configs are supported opportunistically when PyYAML is already
installed by agent-zoo, but JSON configs require no optional imports.
"""

from collections import deque
from collections.abc import Callable, Iterable
import copy
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import shlex
import subprocess
import threading
import time
from typing import Any

from agent_utils.core.events import (
    Event,
    IoCompleted,
    IoFailed,
    IoInterrupted,
    RenderRequested,
    RunToolCall,
    SessionRestored,
    ToolCallFinished,
    TurnSettled,
)
from agent_utils.core.io import IoRequest
from agent_utils.core.pipeline import EmitFn, Feature
from agent_utils.core.state import DurableSpec, State, ToolCall
from agent_utils.core.tool import Tool
from agent_utils.features.core import SchemaPublisher, UnknownToolFallback
from agent_zoo.durability import PluginStorage, StoreUpdate, ThreadPoolDriver
from agent_zoo.features.settings import Setting, SettingError

log = logging.getLogger("agent_zoo_user_plugin.mcp_client")

_PLUGIN_NAME = "azo-plugin-mcp-client"
_PLUGIN_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_ENV = "AZO_MCP_CONFIG"
_MODERN_PROTOCOL_VERSIONS = ("2026-07-28",)
_LEGACY_PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")
_DEFAULT_PROTOCOL_VERSIONS = _MODERN_PROTOCOL_VERSIONS + _LEGACY_PROTOCOL_VERSIONS
_PROTOCOL_VERSION_META = "io.modelcontextprotocol/protocolVersion"
_CLIENT_INFO_META = "io.modelcontextprotocol/clientInfo"
_CLIENT_CAPABILITIES_META = "io.modelcontextprotocol/clientCapabilities"
_SERVER_INFO_META = "io.modelcontextprotocol/serverInfo"
_CLIENT_INFO = {"name": "agent-zoo-userspace-mcp-client", "version": "0.2.0"}
_UNSUPPORTED_PROTOCOL_VERSION = -32022
_MODERN_ERROR_CODES = frozenset({-32020, -32021, _UNSUPPORTED_PROTOCOL_VERSION})
_DEFAULT_TIMEOUT_SECONDS = 15.0
_MAX_SERVER_TIMEOUT_SECONDS = 600.0
_HTTP_TRANSPORT_NAMES = frozenset({"http", "streamable-http", "streamable_http"})
_STDIO_TRANSPORT_NAMES = frozenset({"stdio", ""})
_CONFIG_POLL_SECONDS = 2.0
_DISCOVERY_RETRY_SECONDS = 60.0
_DEFAULT_REFRESH_WAIT_SECONDS = 0.0
_MAX_FUNCTION_NAME = 64
_RESERVED_CONTROL_NAMES = frozenset({
    "mcp_status",
    "mcp_refresh",
    "search_mcp",
    "enable_mcp_tool",
    "mcp_virtual_tool_router",
})


class MCPError(RuntimeError):
    """Raised when an MCP server returns an error or violates the protocol."""


class MCPTimeoutError(MCPError):
    """A request expired without a response (distinct from transport failure)."""


class MCPRPCError(MCPError):
    """A well-formed peer error, preserving fields for version negotiation."""

    def __init__(self, method: str, server: str, code: int, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        suffix = f" data={_json_dump(data)}" if data is not None else ""
        super().__init__(f"{method} failed on {server}: [{code}] {message}{suffix}")


class MCPServerConfig:
    """Normalized configuration for a stdio or Streamable HTTP MCP server."""

    def __init__(
        self,
        *,
        name: str,
        transport: str = "stdio",
        command: str | None = None,
        args: tuple[str, ...] = (),
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        url: str | None = None,
        headers: dict[str, str] | None = None,
        verify_ssl: bool = True,
        ca_file: str | None = None,
        enabled: bool = True,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        tool_prefix: str | None = None,
    ):
        self.name = name
        self.transport = transport
        self.command = command
        self.args = tuple(args)
        self.env = dict(env or {})
        self.cwd = cwd
        self.url = url
        self.headers = dict(headers or {})
        self.verify_ssl = bool(verify_ssl)
        self.ca_file = ca_file
        self.enabled = bool(enabled)
        self.timeout_seconds = float(timeout_seconds)
        self.tool_prefix = tool_prefix

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MCPServerConfig):
            return False
        return self.as_tuple() == other.as_tuple()

    def as_tuple(self):
        return (
            self.name,
            self.transport,
            self.command,
            self.args,
            tuple(sorted(self.env.items())),
            self.cwd,
            self.url,
            tuple(sorted(self.headers.items())),
            self.verify_ssl,
            self.ca_file,
            self.enabled,
            self.timeout_seconds,
            self.tool_prefix,
        )


class MCPVirtualTool:
    """Mapping from a published virtual tool name to a real MCP tool."""

    def __init__(
        self,
        *,
        virtual_name: str,
        server_name: str,
        tool_name: str,
        schema: dict[str, Any],
        description: str,
    ):
        self.virtual_name = virtual_name
        self.server_name = server_name
        self.tool_name = tool_name
        self.schema = schema
        self.description = description


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _strip_json_line_comments(text: str) -> str:
    """Remove // comments outside JSON strings, allowing JSONC-style config."""
    lines: list[str] = []
    for line in text.splitlines():
        in_string = False
        escaped = False
        cut_at: int | None = None
        for idx, char in enumerate(line):
            if escaped:
                escaped = False
                continue
            if char == "\\" and in_string:
                escaped = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            if not in_string and char == "/" and idx + 1 < len(line) and line[idx + 1] == "/":
                cut_at = idx
                break
        lines.append(line[:cut_at].rstrip() if cut_at is not None else line)
    return "\n".join(lines)


def _clean_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value or "").strip())
    token = re.sub(r"_+", "_", token).strip("_").lower()
    if not token:
        token = "tool"
    if token[0].isdigit():
        token = f"_{token}"
    return token


def _shorten_tool_name(name: str) -> str:
    if len(name) <= _MAX_FUNCTION_NAME:
        return name
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    keep = _MAX_FUNCTION_NAME - len(digest) - 1
    return f"{name[:keep].rstrip('_-')}_{digest}"


def _agent_zoo_data_root() -> Path:
    return PluginStorage(_PLUGIN_NAME).state_root


def _user_config_dir() -> Path:
    return PluginStorage(_PLUGIN_NAME).config_dir


def _default_config_path() -> Path:
    env_path = os.environ.get(_CONFIG_ENV, "").strip()
    if env_path:
        return Path(env_path).expanduser()
    for root in (_user_config_dir(), _PLUGIN_ROOT):
        for name in ("mcp_servers.json", "mcp_servers.yaml", "mcp_servers.yml"):
            candidate = root / name
            if candidate.exists():
                return candidate
    return _user_config_dir() / "mcp_servers.json"


def _load_config_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as exc:  # pragma: no cover - depends on local env
            raise MCPError(
                f"YAML config {path} requires PyYAML; use JSON or install PyYAML."
            ) from exc
        loaded = yaml.safe_load(text) or {}
    else:
        loaded = json.loads(_strip_json_line_comments(text) or "{}")
    if not isinstance(loaded, dict):
        raise MCPError(f"MCP config {path} must contain a JSON/YAML object.")
    return loaded


def _expand_env_map(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    env: dict[str, str] = {}
    for key, value in raw.items():
        if value is None:
            continue
        env[str(key)] = os.path.expandvars(str(value))
    return env


def _expand_header_map(raw: Any) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise MCPError("server headers must be an object")
    headers: dict[str, str] = {}
    for raw_name, raw_value in raw.items():
        name = str(raw_name).strip()
        if not name or "\r" in name or "\n" in name:
            raise MCPError("server header names must be non-empty and cannot contain newlines")
        if raw_value is None:
            continue
        value = os.path.expandvars(str(raw_value))
        if "\r" in value or "\n" in value:
            raise MCPError(f"server header {name!r} cannot contain newlines")
        headers[name] = value
    return headers


def _normalize_http_url(raw_url: Any) -> str:
    from urllib.parse import urlparse

    url = os.path.expandvars(str(raw_url or "").strip())
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise MCPError("HTTP MCP server url must be an absolute http(s) URL")
    return url


def _normalize_command(raw_command: Any, raw_args: Any) -> tuple[str, tuple[str, ...]]:
    if isinstance(raw_command, (list, tuple)):
        parts = [str(part) for part in raw_command if str(part)]
        if not parts:
            raise MCPError("server command list is empty")
        command, embedded_args = parts[0], parts[1:]
    else:
        command_text = str(raw_command or "").strip()
        if not command_text:
            raise MCPError("server command is required")
        split = shlex.split(command_text)
        command, embedded_args = (split[0], split[1:]) if split else (command_text, [])

    args: list[str] = list(embedded_args)
    if isinstance(raw_args, (list, tuple)):
        args.extend(str(arg) for arg in raw_args)
    elif isinstance(raw_args, str) and raw_args.strip():
        args.extend(shlex.split(raw_args))
    elif raw_args not in (None, ""):
        raise MCPError("server args must be a list or string")
    return command, tuple(args)


def _normalize_config(raw: dict[str, Any]) -> dict[str, MCPServerConfig]:
    raw_servers = raw.get("mcpServers", raw.get("servers", {}))
    if raw_servers is None:
        raw_servers = {}
    if not isinstance(raw_servers, dict):
        raise MCPError("MCP config must contain an object at mcpServers or servers.")

    servers: dict[str, MCPServerConfig] = {}
    for raw_name, raw_entry in raw_servers.items():
        name = _clean_token(str(raw_name))
        if not isinstance(raw_entry, dict):
            log.warning("Ignoring MCP server %s because its config is not an object", raw_name)
            continue
        try:
            raw_transport = raw_entry.get("type", raw_entry.get("transport", ""))
            transport_name = str(raw_transport or "").strip().lower()
            if not transport_name and raw_entry.get("url") is not None and raw_entry.get("command") is None:
                transport_name = "http"
            if transport_name in _HTTP_TRANSPORT_NAMES:
                transport = "http"
                command = None
                args: tuple[str, ...] = ()
                url = _normalize_http_url(raw_entry.get("url"))
                headers = _expand_header_map(raw_entry.get("headers"))
                verify_ssl = raw_entry.get("verify_ssl", raw_entry.get("tls_verify", True))
                if type(verify_ssl) is not bool:
                    raise MCPError("HTTP verify_ssl must be a boolean")
                raw_ca_file = raw_entry.get("ca_file", raw_entry.get("tls_ca_file"))
                ca_file = None
                if raw_ca_file not in (None, ""):
                    ca_file = str(Path(os.path.expandvars(str(raw_ca_file))).expanduser())
                if not verify_ssl and ca_file:
                    raise MCPError("HTTP ca_file cannot be combined with verify_ssl=false")
            elif transport_name in _STDIO_TRANSPORT_NAMES:
                transport = "stdio"
                command, args = _normalize_command(raw_entry.get("command"), raw_entry.get("args"))
                url = None
                headers = {}
                verify_ssl = True
                ca_file = None
            elif transport_name == "sse":
                raise MCPError('legacy SSE MCP transport is not supported; use Streamable HTTP (type="http")')
            else:
                raise MCPError(f"unsupported MCP transport {raw_transport!r}")
            cwd = raw_entry.get("cwd")
            if cwd is not None:
                cwd = str(Path(str(cwd)).expanduser())
            timeout = float(raw_entry.get("timeout_seconds", raw_entry.get("timeout", _DEFAULT_TIMEOUT_SECONDS)))
            servers[name] = MCPServerConfig(
                name=name,
                transport=transport,
                command=command,
                args=args,
                env=_expand_env_map(raw_entry.get("env")),
                cwd=cwd,
                url=url,
                headers=headers,
                verify_ssl=verify_ssl,
                ca_file=ca_file,
                enabled=raw_entry.get("enabled", True) is not False,
                timeout_seconds=min(_MAX_SERVER_TIMEOUT_SECONDS, max(1.0, timeout)),
                tool_prefix=str(raw_entry.get("tool_prefix") or raw_entry.get("prefix") or "").strip() or None,
            )
        except Exception as exc:
            log.warning("Ignoring invalid MCP server config for %s: %s", raw_name, exc)
    return servers


# ---------------------------------------------------------------------------
# External IO transport clients (owned exclusively by MCPClientDriver)
# ---------------------------------------------------------------------------

class StdioMCPClient:
    """Synchronous stdio client for modern and initialization-based MCP."""

    def __init__(self, config: MCPServerConfig):
        self.config = config
        self.process: subprocess.Popen[str] | None = None
        self._next_id = 1
        self._lifecycle_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._responses: dict[int, dict[str, Any]] = {}
        self._pending: set[int] = set()
        self._condition = threading.Condition()
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stdout_closed = False
        self._stderr_tail: deque[str] = deque(maxlen=40)
        self._initialized = False
        self._modern_confirmed = False
        self.protocol_version: str | None = None
        self.protocol_era: str | None = None
        self.server_info: dict[str, Any] = {}
        self.server_capabilities: dict[str, Any] = {}
        self.server_instructions: str | None = None
        self.supported_versions: list[str] = []
        # Diagnostic snapshot only, not a reusable TTL/authorization cache.
        self.discovery_result: dict[str, Any] | None = None
        self.tools_dirty = True
        self.last_error: str | None = None

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self) -> None:
        with self._lifecycle_lock:
            if self.is_running() and self._initialized and not self._stdout_closed:
                return
            self.close()
            self._stderr_tail.clear()
            env = os.environ.copy()
            env.update(self.config.env)
            argv = [self.config.command, *self.config.args]
            try:
                proc = subprocess.Popen(
                    argv,
                    cwd=self.config.cwd or None,
                    env=env,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                )
            except Exception as exc:
                self.last_error = f"failed to start MCP server {self.config.name}: {exc}"
                raise MCPError(self.last_error) from exc
            self.process = proc
            self._stdout_closed = False
            self._stdout_thread = threading.Thread(
                target=self._stdout_loop,
                args=(proc,),
                name=f"mcp-stdout-{self.config.name}",
                daemon=True,
            )
            self._stderr_thread = threading.Thread(
                target=self._stderr_loop,
                args=(proc,),
                name=f"mcp-stderr-{self.config.name}",
                daemon=True,
            )
            try:
                self._stdout_thread.start()
                self._stderr_thread.start()
                self._initialize()
            except Exception as exc:
                self.last_error = str(exc)
                self.close()
                raise

    def close(self) -> None:
        with self._lifecycle_lock:
            proc = self.process
            self.process = None
            self._initialized = False
            self._modern_confirmed = False
            self.protocol_version = None
            self.protocol_era = None
            self.server_info = {}
            self.server_capabilities = {}
            self.server_instructions = None
            self.supported_versions = []
            self.discovery_result = None
            self.tools_dirty = True
            with self._condition:
                self._responses.clear()
                self._pending.clear()
                self._condition.notify_all()
            if proc is None:
                return
            # EOF is the portable shutdown signal; then escalate and reap.
            try:
                with self._write_lock:
                    if proc.stdin is not None:
                        proc.stdin.close()
                try:
                    proc.wait(timeout=0.25)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        proc.wait(timeout=0.5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=1.0)
            except (OSError, ValueError, subprocess.TimeoutExpired):
                if proc.poll() is None:
                    try:
                        proc.kill()
                        proc.wait(timeout=1.0)
                    except (OSError, subprocess.TimeoutExpired):
                        pass
            for thread in (self._stdout_thread, self._stderr_thread):
                if thread is not None and thread.ident is not None and thread is not threading.current_thread():
                    thread.join(timeout=0.25)
            # Don't block closing a pipe still held by a descendant process.
            for stream, thread in ((proc.stdout, self._stdout_thread), (proc.stderr, self._stderr_thread)):
                if stream is not None and (thread is None or not thread.is_alive()):
                    stream.close()
            self._stdout_thread = self._stderr_thread = None

    def _stdout_loop(self, proc: subprocess.Popen[str]) -> None:
        try:
            if proc.stdout is None:
                return
            for line in proc.stdout:
                if proc is not self.process:
                    break
                text = line.strip()
                if not text:
                    continue
                try:
                    message = json.loads(text)
                except ValueError:
                    log.warning("Ignoring non-JSON line from MCP server %s: %r", self.config.name, text[:200])
                    continue
                self._handle_message(message, proc)
        except (OSError, ValueError, MCPError):
            log.debug("MCP stdout reader stopped for %s", self.config.name, exc_info=True)
        finally:
            with self._condition:
                if proc is self.process:
                    self._stdout_closed = True
                self._condition.notify_all()

    def _stderr_loop(self, proc: subprocess.Popen[str]) -> None:
        try:
            if proc.stderr is not None:
                for line in proc.stderr:
                    if proc is not self.process:
                        break
                    if line.rstrip():
                        self._stderr_tail.append(line.rstrip())
        except (OSError, ValueError):
            pass

    def _handle_message(self, message: Any, proc: subprocess.Popen[str] | None = None) -> None:
        if proc is not None and proc is not self.process:
            return
        if not isinstance(message, dict):
            return
        if "id" in message and ("result" in message or "error" in message):
            request_id = message["id"]
            # JSON-RPC string IDs are distinct from numbers; bool is not an ID.
            if type(request_id) is not int:
                return
            with self._condition:
                if request_id in self._pending:
                    self._responses[request_id] = message
                    self._condition.notify_all()
            return
        method = message.get("method")
        if "id" in message and method:
            # Modern stdio forbids server requests/client responses altogether.
            if self.protocol_era != "legacy":
                log.warning("Ignoring server request on modern MCP connection %s", self.config.name)
                return
            if method == "ping":
                response = {"result": {}}
            else:
                response = {"error": {"code": -32601, "message": "Unsupported client method"}}
            self._send_raw({"jsonrpc": "2.0", "id": message["id"], **response}, proc=proc)
        elif method == "notifications/tools/list_changed" and self.protocol_era == "legacy":
            self.tools_dirty = True

    def _send_raw(self, message: dict[str, Any], *, proc: subprocess.Popen[str] | None = None) -> None:
        proc = proc if proc is not None else self.process
        with self._write_lock:
            if proc is None or proc is not self.process or proc.stdin is None or proc.poll() is not None:
                raise MCPError(f"MCP server {self.config.name} is not running")
            try:
                proc.stdin.write(_compact_json(message) + "\n")
                proc.stdin.flush()
            except (OSError, ValueError) as exc:
                raise MCPError(f"write failed for MCP server {self.config.name}: {exc}") from exc

    def _request(self, method: str, params: dict[str, Any] | None = None, timeout: float | None = None) -> dict[str, Any]:
        # A recognized modern error pins the era, even if retrying later fails.
        attempted: set[str | None] = set()
        deadline = time.monotonic() + (self.config.timeout_seconds if timeout is None else timeout)
        while True:
            attempted.add(self.protocol_version)
            try:
                return self._request_once(method, params, max(0.0, deadline - time.monotonic()))
            except MCPRPCError as exc:
                if self.protocol_era != "modern" or exc.code not in _MODERN_ERROR_CODES:
                    raise
                self._modern_confirmed = True
                if exc.code != _UNSUPPORTED_PROTOCOL_VERSION:
                    raise
                data = exc.data
                if not isinstance(data, dict) or not isinstance(data.get("requested"), str):
                    raise MCPError("Malformed UnsupportedProtocolVersion error data") from exc
                version = self._select_modern_version(data.get("supported"))
                if version in attempted:
                    raise MCPError(f"MCP server {self.config.name} rejected mutually supported version {version}") from exc
                self.protocol_version = version

    def _request_once(self, method: str, params: dict[str, Any] | None, timeout: float) -> dict[str, Any]:
        proc = self.process
        if proc is None or proc.poll() is not None:
            raise MCPError(f"MCP server {self.config.name} is not running")
        with self._condition:
            request_id = self._next_id
            self._next_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if self.protocol_era == "modern":
            request_params = dict(params or {})
            meta = dict(request_params.get("_meta") or {})
            meta.update({
                _PROTOCOL_VERSION_META: self.protocol_version,
                _CLIENT_INFO_META: dict(_CLIENT_INFO),
                _CLIENT_CAPABILITIES_META: {},
            })
            request_params["_meta"] = meta
            message["params"] = request_params
        elif params is not None:
            message["params"] = params
        deadline = time.monotonic() + timeout
        with self._condition:
            self._pending.add(request_id)
        try:
            self._send_raw(message, proc=proc)
            with self._condition:
                while True:
                    response = self._responses.pop(request_id, None)
                    if response is not None:
                        break
                    if proc is not self.process or proc.poll() is not None or self._stdout_closed:
                        raise MCPError(f"MCP server {self.config.name} closed while waiting for {method}")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise MCPTimeoutError(f"timed out waiting for {method} from MCP server {self.config.name}")
                    self._condition.wait(timeout=min(0.25, remaining))
        except MCPTimeoutError:
            # Legacy initialize cannot be cancelled. Every other request can.
            if method != "initialize":
                try:
                    self._send_raw(
                        {
                            "jsonrpc": "2.0",
                            "method": "notifications/cancelled",
                            "params": {"requestId": request_id, "reason": "Request timed out"},
                        },
                        proc=proc,
                    )
                except MCPError:
                    pass
            raise
        finally:
            with self._condition:
                self._pending.discard(request_id)
                self._responses.pop(request_id, None)
        if response.get("jsonrpc") != "2.0" or ("result" in response) == ("error" in response):
            raise MCPError(f"Malformed JSON-RPC response to {method} on {self.config.name}")
        if "error" in response:
            error = response["error"]
            if not isinstance(error, dict) or type(error.get("code")) is not int or not isinstance(error.get("message"), str):
                raise MCPError(f"Malformed JSON-RPC error from {self.config.name}")
            raise MCPRPCError(method, self.config.name, error["code"], error["message"], error.get("data"))
        result = response.get("result")
        if not isinstance(result, dict):
            raise MCPError(f"{method} on {self.config.name} returned a non-object result")
        if self.protocol_era == "modern":
            result_type = result.get("resultType", "complete")
            if result_type != "complete":
                raise MCPError(f"{method} returned unsupported resultType {result_type!r}; no optional client capabilities are enabled")
            meta = result.get("_meta", {})
            if not isinstance(meta, dict):
                raise MCPError(f"{method} returned malformed result metadata")
            info = meta.get(_SERVER_INFO_META)
            if info is not None:
                self.server_info = self._validate_server_info(info)
        return result

    def _notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._send_raw(message)

    def _select_modern_version(self, versions: Any) -> str:
        if not isinstance(versions, list) or not versions or not all(isinstance(v, str) for v in versions):
            raise MCPError("MCP server returned missing or invalid supported versions")
        for version in _MODERN_PROTOCOL_VERSIONS:
            if version in versions:
                self.supported_versions = list(versions)
                return version
        raise MCPError(f"No mutually supported modern MCP version for {self.config.name}: {versions!r}")

    @staticmethod
    def _validate_server_info(info: Any) -> dict[str, Any]:
        if not isinstance(info, dict) or not all(isinstance(info.get(key), str) for key in ("name", "version")):
            raise MCPError("MCP server returned missing or invalid serverInfo")
        return dict(info)

    def _retain_server_details(self, result: dict[str, Any]) -> None:
        capabilities = result.get("capabilities")
        if not isinstance(capabilities, dict):
            raise MCPError("MCP server returned missing or invalid capabilities")
        instructions = result.get("instructions")
        if instructions is not None and not isinstance(instructions, str):
            raise MCPError("MCP server returned invalid instructions")
        self.server_capabilities = dict(capabilities)
        self.server_instructions = instructions

    def _initialize(self) -> None:
        self.protocol_era = "modern"
        self.protocol_version = _MODERN_PROTOCOL_VERSIONS[0]
        try:
            result = self._request("server/discover")
        except (MCPRPCError, MCPTimeoutError):
            if self._modern_confirmed:
                raise
            # All non-modern RPC errors and probe silence permit legacy fallback,
            # not just -32601. Transport failures/malformed success do not.
            self.protocol_era = "legacy"
            self.protocol_version = None
            result = self._request(
                "initialize",
                {
                    "protocolVersion": _LEGACY_PROTOCOL_VERSIONS[0],
                    "capabilities": {},
                    "clientInfo": dict(_CLIENT_INFO),
                },
            )
            version = result.get("protocolVersion")
            if version not in _LEGACY_PROTOCOL_VERSIONS:
                raise MCPError(f"MCP server {self.config.name} selected unsupported legacy version {version!r}")
            self.protocol_version = version
            self.server_info = self._validate_server_info(result.get("serverInfo"))
            self._retain_server_details(result)
            self._notify("notifications/initialized")
        else:
            self._modern_confirmed = True
            self.protocol_version = self._select_modern_version(result.get("supportedVersions"))
            self._retain_server_details(result)
            self.discovery_result = dict(result)
        self._initialized = True
        self.tools_dirty = True
        self.last_error = None

    def list_tools(self) -> list[dict[str, Any]]:
        self.start()
        if self.protocol_era == "legacy" and "tools" not in self.server_capabilities:
            self.tools_dirty = False
            return []
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            params = {"cursor": cursor} if cursor else None
            result = self._request("tools/list", params=params)
            batch = result.get("tools", [])
            if not isinstance(batch, list):
                raise MCPError(f"tools/list on {self.config.name} returned non-list tools")
            tools.extend(tool for tool in batch if isinstance(tool, dict))
            next_cursor = result.get("nextCursor")
            if not next_cursor:
                break
            if not isinstance(next_cursor, str) or next_cursor in seen_cursors:
                raise MCPError(f"tools/list on {self.config.name} returned an invalid or repeated cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        self.tools_dirty = False
        self.last_error = None
        return tools

    def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.start()
        if self.protocol_era == "legacy" and "tools" not in self.server_capabilities:
            raise MCPError(f"MCP server {self.config.name} does not advertise tools")
        result = self._request(
            "tools/call",
            {"name": tool_name, "arguments": arguments or {}},
            timeout=self.config.timeout_seconds,
        )
        self.last_error = None
        return result

    def stderr_tail(self) -> str:
        return "\n".join(self._stderr_tail)

    def status(self) -> dict[str, Any]:
        proc = self.process
        return {
            "running": bool(proc is not None and proc.poll() is None),
            "pid": proc.pid if proc is not None and proc.poll() is None else None,
            "protocol_version": self.protocol_version,
            "protocol_era": self.protocol_era,
            "server_info": dict(self.server_info),
            "server_capabilities": dict(self.server_capabilities),
            "server_instructions": self.server_instructions,
            "supported_versions": list(self.supported_versions),
            "tools_dirty": self.tools_dirty,
            "last_error": self.last_error,
            "stderr_tail": self.stderr_tail(),
        }


class StreamableHTTPMCPClient(StdioMCPClient):
    """MCP client for the Streamable HTTP transport.

    The protocol, version negotiation, pagination, and result validation are
    inherited from the stdio client.  Only the wire/lifecycle operations are
    transport-specific here, which keeps the existing stdio behavior intact.
    """

    def __init__(self, config: MCPServerConfig):
        if config.transport != "http":
            raise MCPError(f"HTTP client received {config.transport!r} configuration")
        super().__init__(config)
        self._session_id: str | None = None
        self._http_lock = threading.RLock()

    def is_running(self) -> bool:
        return self._initialized

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._initialized:
                return
            self.close()
            try:
                self._initialize()
            except Exception as exc:
                self.last_error = str(exc)
                self.close()
                raise

    def close(self) -> None:
        with self._lifecycle_lock:
            self._initialized = False
            self._modern_confirmed = False
            self.protocol_version = None
            self.protocol_era = None
            self.server_info = {}
            self.server_capabilities = {}
            self.server_instructions = None
            self.supported_versions = []
            self.discovery_result = None
            self.tools_dirty = True
            self._session_id = None
            with self._condition:
                self._responses.clear()
                self._pending.clear()
                self._condition.notify_all()

    def _request_headers(self, method: str | None = None) -> dict[str, str]:
        reserved = {"accept", "content-type", "mcp-method", "mcp-protocol-version", "mcp-session-id"}
        headers = {
            name: value for name, value in self.config.headers.items()
            if name.casefold() not in reserved
        }
        headers["Accept"] = "application/json, text/event-stream"
        headers["Content-Type"] = "application/json"
        if method:
            headers["Mcp-Method"] = method
        if self.protocol_version:
            headers["MCP-Protocol-Version"] = self.protocol_version
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        return headers

    @staticmethod
    def _sse_message(data_lines: list[str]) -> dict[str, Any] | None:
        if not data_lines:
            return None
        try:
            message = json.loads("\n".join(data_lines))
        except (TypeError, ValueError) as exc:
            raise MCPError("MCP HTTP server returned invalid SSE JSON") from exc
        if not isinstance(message, dict):
            raise MCPError("MCP HTTP server returned a non-object SSE message")
        return message

    def _accept_sse_message(self, message: dict[str, Any], expected_id: int | None) -> dict[str, Any] | None:
        method = message.get("method")
        if method == "notifications/tools/list_changed" and "id" not in message:
            self.tools_dirty = True
        if expected_id is None:
            return None
        response_id = message.get("id")
        if type(response_id) is int and response_id == expected_id:
            return message
        return None

    def _read_sse_response(
        self,
        response,
        *,
        expected_id: int | None,
        timeout: float,
    ) -> dict[str, Any] | None:
        import socket

        data_lines: list[str] = []
        deadline = time.monotonic() + timeout
        while True:
            if time.monotonic() >= deadline:
                raise MCPTimeoutError(f"timed out waiting for MCP HTTP response from {self.config.name}")
            try:
                line = response.readline()
            except (TimeoutError, socket.timeout) as exc:
                raise MCPTimeoutError(f"timed out reading MCP HTTP response from {self.config.name}") from exc
            if not line:
                if data_lines:
                    message = self._sse_message(data_lines)
                    if message is not None:
                        matched = self._accept_sse_message(message, expected_id)
                        if matched is not None:
                            return matched
                break
            if isinstance(line, bytes):
                line = line.decode("utf-8", errors="replace")
            line = line.rstrip("\r\n")
            if not line:
                message = self._sse_message(data_lines)
                data_lines = []
                if message is not None:
                    matched = self._accept_sse_message(message, expected_id)
                    if matched is not None:
                        return matched
                continue
            if line.startswith(":"):
                continue
            if line.startswith("data:"):
                value = line[5:]
                data_lines.append(value[1:] if value.startswith(" ") else value)
        if expected_id is None:
            return None
        raise MCPError(f"MCP HTTP response ended without response id {expected_id}")

    def _read_http_response(
        self,
        response,
        *,
        expected_id: int | None,
        timeout: float,
    ) -> dict[str, Any] | None:
        import socket

        status = int(getattr(response, "status", None) or response.getcode() or 0)
        headers = getattr(response, "headers", None)
        if headers is not None:
            session_id = headers.get("Mcp-Session-Id")
            if session_id:
                self._session_id = str(session_id)
            content_type = str(headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()
        else:
            content_type = ""
        if status == 202:
            response.read()
            if expected_id is not None:
                raise MCPError(f"MCP HTTP server accepted request {expected_id} without a response")
            return None
        if content_type == "text/event-stream":
            return self._read_sse_response(response, expected_id=expected_id, timeout=timeout)
        try:
            body = response.read()
        except (TimeoutError, socket.timeout) as exc:
            raise MCPTimeoutError(f"timed out reading MCP HTTP response from {self.config.name}") from exc
        if not body:
            if status >= 400:
                raise MCPError(f"MCP HTTP server returned HTTP {status} without a response body")
            return None
        try:
            message = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
        except (TypeError, ValueError) as exc:
            raise MCPError(f"MCP HTTP server returned invalid JSON (HTTP {status})") from exc
        if not isinstance(message, dict):
            raise MCPError("MCP HTTP server returned a non-object JSON response")
        return message

    def _post(
        self,
        message: dict[str, Any],
        *,
        expected_id: int | None,
        timeout: float,
    ) -> dict[str, Any] | None:
        import socket
        import ssl
        from urllib import error as urllib_error
        from urllib import request as urllib_request

        if not self.config.url:
            raise MCPError(f"MCP HTTP server {self.config.name} has no URL")
        if timeout <= 0:
            raise MCPTimeoutError(f"timed out before requesting MCP HTTP server {self.config.name}")
        ssl_context = None
        try:
            if self.config.ca_file:
                ssl_context = ssl.create_default_context(cafile=self.config.ca_file)
            elif not self.config.verify_ssl:
                ssl_context = ssl._create_unverified_context()
        except (OSError, ssl.SSLError, ValueError) as exc:
            raise MCPError(f"invalid TLS configuration for MCP HTTP server {self.config.name}: {exc}") from exc
        request = urllib_request.Request(
            self.config.url,
            data=_compact_json(message).encode("utf-8"),
            headers=self._request_headers(message.get("method")),
            method="POST",
        )
        with self._http_lock:
            response = None
            try:
                if ssl_context is None:
                    response = urllib_request.urlopen(request, timeout=max(0.1, timeout))
                else:
                    response = urllib_request.urlopen(
                        request, timeout=max(0.1, timeout), context=ssl_context
                    )
            except urllib_error.HTTPError as exc:
                response = exc
            except urllib_error.URLError as exc:
                reason = exc.reason
                if isinstance(reason, (TimeoutError, socket.timeout)):
                    raise MCPTimeoutError(f"timed out contacting MCP HTTP server {self.config.name}") from exc
                raise MCPError(f"MCP HTTP request failed for {self.config.name}: {reason}") from exc
            except (TimeoutError, socket.timeout) as exc:
                raise MCPTimeoutError(f"timed out contacting MCP HTTP server {self.config.name}") from exc
            try:
                return self._read_http_response(response, expected_id=expected_id, timeout=timeout)
            finally:
                if response is not None:
                    response.close()

    def _request_once(self, method: str, params: dict[str, Any] | None, timeout: float) -> dict[str, Any]:
        with self._condition:
            request_id = self._next_id
            self._next_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if self.protocol_era == "modern":
            request_params = dict(params or {})
            meta = dict(request_params.get("_meta") or {})
            meta.update({
                _PROTOCOL_VERSION_META: self.protocol_version,
                _CLIENT_INFO_META: dict(_CLIENT_INFO),
                _CLIENT_CAPABILITIES_META: {},
            })
            request_params["_meta"] = meta
            message["params"] = request_params
        elif params is not None:
            message["params"] = params
        try:
            response = self._post(message, expected_id=request_id, timeout=timeout)
        except MCPTimeoutError:
            if method != "initialize":
                try:
                    self._notify(
                        "notifications/cancelled",
                        {"requestId": request_id, "reason": "Request timed out"},
                    )
                except MCPError:
                    pass
            raise
        if response is None:
            raise MCPError(f"MCP HTTP server returned no response to {method}")
        if response.get("jsonrpc") != "2.0" or ("result" in response) == ("error" in response):
            raise MCPError(f"Malformed JSON-RPC response to {method} on {self.config.name}")
        response_id = response.get("id")
        if type(response_id) is not int or response_id != request_id:
            raise MCPError(f"MCP HTTP server returned mismatched response id for {method}")
        if "error" in response:
            error = response["error"]
            if not isinstance(error, dict) or type(error.get("code")) is not int or not isinstance(error.get("message"), str):
                raise MCPError(f"Malformed JSON-RPC error from {self.config.name}")
            raise MCPRPCError(method, self.config.name, error["code"], error["message"], error.get("data"))
        result = response.get("result")
        if not isinstance(result, dict):
            raise MCPError(f"{method} on {self.config.name} returned a non-object result")
        if self.protocol_era == "modern":
            result_type = result.get("resultType", "complete")
            if result_type != "complete":
                raise MCPError(f"{method} returned unsupported resultType {result_type!r}; no optional client capabilities are enabled")
            meta = result.get("_meta", {})
            if not isinstance(meta, dict):
                raise MCPError(f"{method} returned malformed result metadata")
            info = meta.get(_SERVER_INFO_META)
            if info is not None:
                self.server_info = self._validate_server_info(info)
        return result

    def _notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._post(message, expected_id=None, timeout=self.config.timeout_seconds)

    def status(self) -> dict[str, Any]:
        return {
            "transport": "http",
            "running": self._initialized,
            "pid": None,
            "protocol_version": self.protocol_version,
            "protocol_era": self.protocol_era,
            "server_info": dict(self.server_info),
            "server_capabilities": dict(self.server_capabilities),
            "server_instructions": self.server_instructions,
            "supported_versions": list(self.supported_versions),
            "session_bound": bool(self._session_id),
            "tls_verified": self.config.verify_ssl,
            "ca_file_configured": bool(self.config.ca_file),
            "tools_dirty": self.tools_dirty,
            "last_error": self.last_error,
        }


class MCPClientRegistry:
    """Loads MCP config, owns stdio/HTTP clients, and caches virtual tool schemas."""

    def __init__(
        self,
        *,
        on_catalog_change: Callable[[dict[str, MCPVirtualTool]], None] | None = None,
    ):
        self.config_path = _default_config_path()
        self._config_mtime_ns: int | None = None
        self._last_config_check = 0.0
        self._servers: dict[str, MCPServerConfig] = {}
        self._clients: dict[str, StdioMCPClient] = {}
        self._virtual_tools: dict[str, MCPVirtualTool] = {}
        self._server_virtuals: dict[str, set[str]] = {}
        self._next_retry_at: dict[str, float] = {}
        self._config_error: str | None = None
        self._lock = threading.RLock()
        self._discovery_threads: dict[str, threading.Thread] = {}
        self._discovery_started_at: dict[str, float] = {}
        self._discovery_status: dict[str, str] = {}
        self._on_catalog_change = on_catalog_change
        self._closed = False

    @staticmethod
    def _make_client(config: MCPServerConfig) -> StdioMCPClient:
        if config.transport == "http":
            return StreamableHTTPMCPClient(config)
        return StdioMCPClient(config)

    @property
    def virtual_tools(self) -> dict[str, MCPVirtualTool]:
        with self._lock:
            return dict(self._virtual_tools)

    def _emit_catalog_change(self, snapshot: dict[str, MCPVirtualTool]) -> None:
        cb = self._on_catalog_change
        if cb is not None:
            try:
                cb(snapshot)
            except Exception:
                log.debug("MCP catalog change callback failed", exc_info=True)

    def close_all(self) -> None:
        with self._lock:
            self._closed = True
            clients = list(self._clients.values())
            threads = list(self._discovery_threads.values())
        for client in clients:
            client.close()
        current = threading.current_thread()
        for thread in threads:
            if thread is not current and thread.is_alive():
                thread.join(timeout=2.0)
        with self._lock:
            remaining = [c for c in self._clients.values() if c not in clients]
            self._clients.clear()
            self._discovery_threads.clear()
        for client in remaining:
            client.close()

    def refresh(
        self,
        server: str | None = None,
        force: bool = False,
        wait_seconds: float | None = _DEFAULT_REFRESH_WAIT_SECONDS,
        enabled: Iterable[str] | None = None,
    ) -> str:
        self._load_config(force=True)
        names: list[str]
        with self._lock:
            if server:
                name = _clean_token(server)
                if name not in self._servers:
                    return f"ERROR: unknown MCP server {server!r}. Configured servers: {', '.join(sorted(self._servers)) or '(none)'}"
                names = [name]
            else:
                names = sorted(self._servers)
        for name in names:
            self._schedule_discovery(name, force=force)
        self._wait_for_discovery(names, wait_seconds or 0.0)
        return self.status_text(enabled=set(enabled) if enabled is not None else None)

    def sync_for_publish(self) -> None:
        # Poll config without ever performing network I/O or launching MCP servers.
        self._load_config(force=False)

    def _load_config(self, force: bool = False) -> None:
        catalog_snapshot: dict[str, MCPVirtualTool] | None = None
        with self._lock:
            now = time.monotonic()
            if not force and now - self._last_config_check < _CONFIG_POLL_SECONDS:
                return
            self._last_config_check = now

            path = _default_config_path()
            if path != self.config_path:
                self.config_path = path
                force = True
                self._config_mtime_ns = None

            try:
                mtime_ns = path.stat().st_mtime_ns if path.exists() else None
                if not force and mtime_ns == self._config_mtime_ns:
                    return
                raw = _load_config_document(path)
                servers = _normalize_config(raw)
                self._config_error = None
            except Exception as exc:
                self._config_error = str(exc)
                log.warning("Could not load MCP config %s: %s", path, exc)
                return

            removed = set(self._servers) - set(servers)
            changed = {
                name
                for name, cfg in servers.items()
                if self._servers.get(name) != cfg
            }
            clients_to_close: list[StdioMCPClient] = []
            for name in removed | changed:
                client = self._clients.pop(name, None)
                if client is not None:
                    clients_to_close.append(client)
                self._remove_server_virtuals(name)
                self._next_retry_at.pop(name, None)
                self._discovery_status.pop(name, None)

            self._servers = servers
            self._config_mtime_ns = mtime_ns
            if removed or changed:
                catalog_snapshot = dict(self._virtual_tools)

        for client in clients_to_close:
            client.close()
        if catalog_snapshot is not None:
            self._emit_catalog_change(catalog_snapshot)

    def _discover_all(self, force: bool = False) -> None:
        with self._lock:
            names = sorted(self._servers)
        for name in names:
            self._schedule_discovery(name, force=force)

    def _schedule_discovery(self, name: str, force: bool = False) -> None:
        now = time.monotonic()
        catalog_snapshot: dict[str, MCPVirtualTool] | None = None
        with self._lock:
            if self._closed:
                return
            cfg = self._servers.get(name)
            if cfg is None or not cfg.enabled:
                had_virtuals = bool(self._server_virtuals.get(name))
                self._remove_server_virtuals(name)
                self._discovery_status[name] = "disabled"
                if had_virtuals:
                    catalog_snapshot = dict(self._virtual_tools)
            else:
                thread = self._discovery_threads.get(name)
                if thread is not None and thread.is_alive():
                    return
                if not force and now < self._next_retry_at.get(name, 0.0):
                    return
                self._discovery_status[name] = "queued"
                thread = threading.Thread(
                    target=self._discover_server,
                    args=(name, force),
                    name=f"mcp-discover-{name}",
                    daemon=True,
                )
                self._discovery_threads[name] = thread
                self._discovery_started_at[name] = now
                thread.start()
        if catalog_snapshot is not None:
            self._emit_catalog_change(catalog_snapshot)

    def _wait_for_discovery(self, names: list[str], wait_seconds: float) -> None:
        if wait_seconds <= 0:
            return
        deadline = time.monotonic() + min(wait_seconds, 10.0)
        for name in names:
            with self._lock:
                thread = self._discovery_threads.get(name)
            if thread is None:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            thread.join(timeout=remaining)

    def _discover_server(self, name: str, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            if self._closed:
                if self._discovery_threads.get(name) is threading.current_thread():
                    self._discovery_threads.pop(name, None)
                return
            cfg = self._servers.get(name)
            if cfg is None or not cfg.enabled:
                self._discovery_status[name] = "disabled"
                return
            if not force and now < self._next_retry_at.get(name, 0.0):
                self._discovery_status[name] = "backoff"
                return
            client = self._clients.get(name)
            if client is None:
                client = self._make_client(cfg)
                self._clients[name] = client
            self._discovery_status[name] = "running"
            self._discovery_started_at[name] = now
        try:
            tools = client.list_tools()
        except Exception as exc:
            with self._lock:
                if self._closed or self._clients.get(name) is not client or self._servers.get(name) != cfg:
                    if self._discovery_threads.get(name) is threading.current_thread():
                        self._discovery_threads.pop(name, None)
                    return
                client.last_error = str(exc)
                self._next_retry_at[name] = time.monotonic() + _DISCOVERY_RETRY_SECONDS
                self._discovery_status[name] = f"error: {type(exc).__name__}: {exc}"
                current = threading.current_thread()
                if self._discovery_threads.get(name) is current:
                    self._discovery_threads.pop(name, None)
            log.warning("Could not discover tools for MCP server %s: %s", name, exc)
            return
        catalog_snapshot: dict[str, MCPVirtualTool] | None = None
        with self._lock:
            if self._closed or self._clients.get(name) is not client or self._servers.get(name) != cfg:
                if self._discovery_threads.get(name) is threading.current_thread():
                    self._discovery_threads.pop(name, None)
                return
            self._install_virtual_tools(name, tools)
            self._next_retry_at.pop(name, None)
            self._discovery_status[name] = f"ok: {len(tools)} tool(s)"
            current = threading.current_thread()
            if self._discovery_threads.get(name) is current:
                self._discovery_threads.pop(name, None)
            catalog_snapshot = dict(self._virtual_tools)
        if catalog_snapshot is not None:
            self._emit_catalog_change(catalog_snapshot)

    def _remove_server_virtuals(self, server_name: str) -> None:
        for virtual_name in self._server_virtuals.pop(server_name, set()):
            self._virtual_tools.pop(virtual_name, None)

    def _install_virtual_tools(self, server_name: str, tools: list[dict[str, Any]]) -> None:
        self._remove_server_virtuals(server_name)
        installed: set[str] = set()
        used_names = set(self._virtual_tools) | _RESERVED_CONTROL_NAMES
        for tool in tools:
            raw_name = str(tool.get("name") or "").strip()
            if not raw_name:
                continue
            description = str(tool.get("description") or f"MCP tool {raw_name} from server {server_name}")
            input_schema = tool.get("inputSchema")
            if not isinstance(input_schema, dict):
                input_schema = {"type": "object", "properties": {}}
            schema = self._openai_tool_schema(server_name, raw_name, description, input_schema)
            virtual_name = schema["function"]["name"]
            if virtual_name in used_names:
                digest = hashlib.sha1(f"{server_name}:{raw_name}".encode("utf-8")).hexdigest()[:8]
                virtual_name = _shorten_tool_name(f"{virtual_name}_{digest}")
                schema["function"]["name"] = virtual_name
            used_names.add(virtual_name)
            installed.add(virtual_name)
            self._virtual_tools[virtual_name] = MCPVirtualTool(
                virtual_name=virtual_name,
                server_name=server_name,
                tool_name=raw_name,
                schema=schema,
                description=description,
            )
        self._server_virtuals[server_name] = installed

    def _openai_tool_schema(
        self,
        server_name: str,
        tool_name: str,
        description: str,
        input_schema: dict[str, Any],
    ) -> dict[str, Any]:
        cfg = self._servers[server_name]
        prefix = cfg.tool_prefix if cfg.tool_prefix is not None else f"mcp_{_clean_token(server_name)}_"
        virtual_name = _shorten_tool_name(prefix + _clean_token(tool_name))
        parameters = dict(input_schema)
        parameters.setdefault("type", "object")
        parameters.setdefault("properties", {})
        return {
            "type": "function",
            "function": {
                "name": virtual_name,
                "description": f"[MCP:{server_name}/{tool_name}] {description}",
                "parameters": parameters,
            },
        }

    def search_tools(
        self,
        query: str = "",
        server: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Search names/descriptions; discover missing or dirty catalogs, never call tools."""
        _validate_search_args(query=query, server=server, limit=limit, offset=offset)
        self._load_config(force=False)
        with self._lock:
            if server is not None and _clean_token(server) not in self._servers:
                raise MCPError(f"unknown MCP server {server!r}")
            names = [_clean_token(server)] if server is not None else sorted(self._servers)
            discover = [
                name
                for name in names
                if self._servers[name].enabled
                and (
                    name not in self._server_virtuals
                    or getattr(self._clients.get(name), "tools_dirty", False)
                )
            ]
        for name in discover:
            self._schedule_discovery(name)
        self._wait_for_discovery(discover, 1.0)
        terms = query.casefold().split()
        with self._lock:
            matches = []
            for tool in sorted(self._virtual_tools.values(), key=lambda item: item.virtual_name):
                if tool.server_name not in names:
                    continue
                text = " ".join((tool.virtual_name, tool.server_name, tool.tool_name, tool.description)).casefold()
                if not all(term in text for term in terms):
                    continue
                matches.append({
                    "name": tool.virtual_name,
                    "server": tool.server_name,
                    "tool": tool.tool_name,
                    "description": tool.description[:320],
                })
            return {
                "matches": matches[offset : offset + limit],
                "total": len(matches),
                "next_offset": offset + limit if offset + limit < len(matches) else None,
                "discovery": {
                    name: self._discovery_status.get(name, "idle") if self._servers[name].enabled else "disabled"
                    for name in names
                    if name in self._servers
                },
                "config_error": self._config_error,
            }

    def call_virtual_tool(self, virtual_name: str, arguments: dict[str, Any]) -> str:
        with self._lock:
            if self._closed:
                return "ERROR: MCP client registry is closed."
            mapping = self._virtual_tools.get(virtual_name)
            if mapping is None:
                return f"ERROR: unknown MCP virtual tool {virtual_name!r}. Run mcp_refresh and mcp_status."
            cfg = self._servers.get(mapping.server_name)
            if cfg is None or not cfg.enabled:
                return f"ERROR: MCP server {mapping.server_name!r} is no longer configured or enabled."
            client = self._clients.get(mapping.server_name)
            if client is None:
                client = self._make_client(cfg)
                self._clients[mapping.server_name] = client
        try:
            result = client.call_tool(mapping.tool_name, arguments or {})
            return _format_call_result(mapping, result)
        except Exception as exc:
            client.last_error = str(exc)
            return f"ERROR: MCP tool {mapping.server_name}/{mapping.tool_name} failed: {exc}"

    def status_text(self, enabled: Iterable[str] | None = None) -> str:
        self._load_config(force=False)
        enabled_set = set(enabled) if enabled is not None else None
        lines = []
        with self._lock:
            if self._config_error:
                lines.append("Config error: " + _brief(self._config_error))
            for name, cfg in sorted(self._servers.items()):
                client = self._clients.get(name)
                discovery = self._discovery_status.get(name, "idle")
                running = client is not None and client.is_running()
                phase = "connected" if running else "not connected"
                if discovery in {"queued", "running"}:
                    phase = "discovering"
                if not cfg.enabled:
                    phase = "disabled"
                if phase == "connected" and client is not None and client.protocol_version:
                    phase += f" (MCP {client.protocol_version})"
                tools = self._server_virtuals.get(name)
                catalog = (
                    "catalog not loaded"
                    if tools is None
                    else f"{len(tools)} {'cached ' if not running else ''}tool{'s' if len(tools) != 1 else ''}"
                )
                parts = [f"{name}: {phase}", catalog]
                if enabled_set is not None:
                    parts.append(f"{len((tools or set()) & enabled_set)} enabled")
                if tools is not None and client is not None and client.tools_dirty:
                    parts.append("catalog stale; search to refresh")
                error = discovery if discovery.startswith("error:") else (client.last_error if client else None)
                if error:
                    parts.append(_brief(error) if str(error).startswith("error:") else "error: " + _brief(error))
                retry = self._next_retry_at.get(name, 0.0) - time.monotonic()
                if retry > 0:
                    parts.append(f"retry in {int(retry) + 1}s")
                lines.append("; ".join(parts))
            if not self._servers:
                lines.append("No MCP servers configured.")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pure formatting & validation helpers
# ---------------------------------------------------------------------------

def _validate_search_args(
    *,
    query: Any = "",
    server: Any = None,
    limit: Any = 20,
    offset: Any = 0,
) -> None:
    if not isinstance(query, str) or (server is not None and not isinstance(server, str)):
        raise MCPError("query and server must be strings")
    if type(limit) is not int or not 1 <= limit <= 50:
        raise MCPError("limit must be an integer from 1 to 50")
    if type(offset) is not int or offset < 0:
        raise MCPError("offset must be a nonnegative integer")


def _brief(text: Any, limit: int = 200) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def _format_search_result(result: dict[str, Any], enabled: set[str]) -> str:
    lines = [
        f"{item['name']}{' [enabled]' if item['name'] in enabled else ''} — {_brief(item['description'])}"
        for item in result["matches"]
    ]
    if not lines:
        if result["total"]:
            lines.append(f"No results on this page; {result['total']} matches total. Retry with offset=0.")
        elif not result["discovery"] and not result["config_error"]:
            lines.append("No MCP servers configured.")
        else:
            lines.append("No matching MCP tools in the loaded catalog.")
    if result["next_offset"] is not None:
        lines.append(f"More matches: use offset={result['next_offset']} with the same query and server ({result['total']} total).")
    for server, status in result["discovery"].items():
        if status in {"queued", "running"}:
            lines.append(f"{server}: discovery pending; retry search shortly.")
        elif status == "disabled":
            lines.append(f"{server}: disabled.")
        elif status == "backoff":
            lines.append(f"{server}: discovery retry delayed; see mcp_status.")
        elif status.startswith("error:"):
            lines.append(f"{server}: discovery failed — {_brief(status[6:])}. See mcp_status.")
    if result["config_error"]:
        lines.append("Config error: " + _brief(result["config_error"]))
    return "\n".join(lines)


def _format_call_result(mapping: MCPVirtualTool, result: dict[str, Any]) -> str:
    prefix = "MCP TOOL ERROR" if result.get("isError") else "MCP TOOL RESULT"
    lines = [f"{prefix}: {mapping.server_name}/{mapping.tool_name}"]

    structured = result.get("structuredContent")
    content = result.get("content")
    if isinstance(content, list) and content:
        for index, block in enumerate(content, start=1):
            if not isinstance(block, dict):
                lines.append(f"\n[{index}] {_json_dump(block)}")
                continue
            block_type = block.get("type")
            if block_type == "text":
                lines.append(str(block.get("text") or ""))
            elif block_type == "image":
                mime = block.get("mimeType") or "image/*"
                lines.append(f"[image content omitted from text result; mimeType={mime}]")
            elif block_type == "audio":
                mime = block.get("mimeType") or "audio/*"
                lines.append(f"[audio content omitted from text result; mimeType={mime}]")
            elif block_type == "resource":
                lines.append("[resource content]\n" + _json_dump(block.get("resource", block)))
            else:
                lines.append(_json_dump(block))
    if structured is not None:
        lines.append("\nstructuredContent:\n" + _json_dump(structured))
    if not isinstance(content, list) or not content:
        if structured is None:
            lines.append(_json_dump(result))
    return "\n".join(line for line in lines if line is not None)


# ---------------------------------------------------------------------------
# Pure Sans-I/O Events, IoRequests, Outcomes, and In-Memory Catalog
# ---------------------------------------------------------------------------

@dataclass
class MCPRequest(IoRequest):
    """Base class for all MCP client driver IO requests."""


@dataclass
class MCPStatusRequest(MCPRequest):
    """Request formatted MCP server status text."""

    enabled: tuple[str, ...] = ()


@dataclass
class MCPRefreshRequest(MCPRequest):
    """Reload MCP config and schedule discovery for one or all servers."""

    server: str | None = None
    force: bool = True
    wait_seconds: float = 0.0
    enabled: tuple[str, ...] | None = None


@dataclass
class MCPSearchRequest(MCPRequest):
    """Search the MCP tool catalog (discovering missing/dirty servers with bounded wait)."""

    query: str = ""
    server: str | None = None
    limit: int = 20
    offset: int = 0


@dataclass
class MCPCallToolRequest(MCPRequest):
    """Invoke a published virtual MCP endpoint over MCP ``tools/call``."""

    virtual_name: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class MCPSyncCatalogRequest(MCPRequest):
    """Poll MCP config without starting servers and return the current catalog snapshot."""

    force: bool = False


@dataclass(frozen=True)
class MCPStatusResult:
    """Driver response for ``MCPStatusRequest``."""

    text: str
    catalog: dict[str, MCPVirtualTool] = field(default_factory=dict)


@dataclass(frozen=True)
class MCPRefreshResult:
    """Driver response for ``MCPRefreshRequest``."""

    text: str
    catalog: dict[str, MCPVirtualTool] = field(default_factory=dict)


@dataclass(frozen=True)
class MCPSearchResult:
    """Driver response for ``MCPSearchRequest``."""

    result: dict[str, Any]
    catalog: dict[str, MCPVirtualTool] = field(default_factory=dict)


@dataclass(frozen=True)
class MCPCallResult:
    """Driver response for ``MCPCallToolRequest``."""

    text: str
    is_error: bool = False
    catalog: dict[str, MCPVirtualTool] | None = None


@dataclass(frozen=True)
class MCPSyncResult:
    """Driver response for ``MCPSyncCatalogRequest``."""

    catalog: dict[str, MCPVirtualTool] = field(default_factory=dict)


@dataclass
class MCPCatalogUpdated(Event):
    """Event emitted when the discovered MCP virtual tool catalog changes."""

    catalog: dict[str, MCPVirtualTool] = field(default_factory=dict)


class MCPCatalog:
    """Pure in-memory view of discovered virtual MCP tools shared by pipeline components.

    Contains only pure schema metadata (zero subprocesses, sockets, threads, or
    file I/O). Kept outside durable ``State`` attributes so session persistence
    records only explicit user selections (``state.mcp_enabled_tools``).
    """

    def __init__(self, tools: dict[str, MCPVirtualTool] | None = None) -> None:
        self._virtual_tools: dict[str, MCPVirtualTool] = dict(tools or {})

    @property
    def virtual_tools(self) -> dict[str, MCPVirtualTool]:
        return dict(self._virtual_tools)

    def update(self, tools: dict[str, MCPVirtualTool]) -> None:
        self._virtual_tools = dict(tools)

    def get(self, name: str) -> MCPVirtualTool | None:
        return self._virtual_tools.get(name)

    def __contains__(self, name: object) -> bool:
        return name in self._virtual_tools


@dataclass
class MCPClientState:
    """Durable per-session MCP virtual tool selection."""

    enabled_tools: list[str] = field(default_factory=list)


MCP_CLIENT_DURABLE_SPEC = DurableSpec(
    name="mcp_client",
    cls=MCPClientState,
    version=1,
    on_fork="copy",
)


@dataclass
class MCPClientRuntime:
    """Strictly ephemeral per-session MCP runtime catalog reference (never serialized)."""

    catalog: MCPCatalog = field(default_factory=MCPCatalog)


# ---------------------------------------------------------------------------
# Runtime Driver (imperative IO shell)
# ---------------------------------------------------------------------------

class MCPClientDriver(ThreadPoolDriver):
    """Runtime IO driver owning ``MCPClientRegistry`` and executing ``MCPRequest``s."""

    durability = "process_bound"
    request_types = (MCPRequest,)
    max_workers = 4
    worker_prefix = "agent-zoo-mcp"
    interrupt_on_quiesce = True

    def __init__(
        self,
        registry: MCPClientRegistry | None = None,
        *,
        max_workers: int = 4,
    ) -> None:
        super().__init__(max_workers=max_workers)
        self.registry = registry if registry is not None else MCPClientRegistry()

    def start(self, emit: EmitFn) -> None:
        super().start(emit)
        self.registry._on_catalog_change = self._on_catalog_change
        initial = self.registry.virtual_tools
        if initial:
            self._safe_emit(MCPCatalogUpdated(catalog=initial))

    def _on_catalog_change(self, catalog: dict[str, MCPVirtualTool]) -> None:
        with self._lock:
            if self._closed:
                return
        self._safe_emit(MCPCatalogUpdated(catalog=dict(catalog)))

    def quiesce(
        self,
        *,
        policy: str = "terminate",
        deadline: float | None = None,
    ) -> list[Event]:
        return super().quiesce(
            policy=policy,
            deadline=time.monotonic() if deadline is None else deadline,
        )

    def on_close(self) -> None:
        self.registry._on_catalog_change = None
        self.registry.close_all()

    def execute(self, req: IoRequest) -> Any:
        if isinstance(req, MCPStatusRequest):
            text = self.registry.status_text(enabled=set(req.enabled))
            return MCPStatusResult(text=text, catalog=self.registry.virtual_tools)
        if isinstance(req, MCPRefreshRequest):
            text = self.registry.refresh(
                server=req.server,
                force=req.force,
                wait_seconds=req.wait_seconds,
                enabled=req.enabled,
            )
            return MCPRefreshResult(text=text, catalog=self.registry.virtual_tools)
        if isinstance(req, MCPSearchRequest):
            result = self.registry.search_tools(
                query=req.query,
                server=req.server,
                limit=req.limit,
                offset=req.offset,
            )
            return MCPSearchResult(result=result, catalog=self.registry.virtual_tools)
        if isinstance(req, MCPCallToolRequest):
            text = self.registry.call_virtual_tool(req.virtual_name, dict(req.arguments))
            is_error = text.startswith(("ERROR:", "MCP TOOL ERROR"))
            return MCPCallResult(text=text, is_error=is_error, catalog=self.registry.virtual_tools)
        if isinstance(req, MCPSyncCatalogRequest):
            if req.force:
                self.registry._load_config(force=True)
            else:
                self.registry.sync_for_publish()
            return MCPSyncResult(catalog=self.registry.virtual_tools)
        raise TypeError(f"Unsupported MCPRequest: {type(req).__name__}")


# ---------------------------------------------------------------------------
# Pure Sans-I/O Pipeline Tools & Event Components
# ---------------------------------------------------------------------------

def _mcp_state(state: State) -> MCPClientState:
    return state.durable(MCP_CLIENT_DURABLE_SPEC)


def _enabled_names(state: State) -> set[str]:
    names = _mcp_state(state).enabled_tools
    return {name for name in names if isinstance(name, str)} if isinstance(names, list) else set()


def _session_catalog(state: State, shared: MCPCatalog) -> MCPCatalog:
    rt = state.runtime("mcp_client", lambda: MCPClientRuntime(catalog=shared))
    return rt.catalog


def _update_catalog(state: State, shared: MCPCatalog, tools: dict[str, MCPVirtualTool]) -> MCPCatalog:
    shared.update(tools)
    cat = _session_catalog(state, shared)
    if cat is not shared:
        cat.update(tools)
    return cat


def _owns_endpoint(entry: Any) -> bool:
    if not isinstance(entry, dict):
        return False
    # Recognize legacy eager publication as well as entries from this version.
    function = entry.get("schema", {}).get("function", {})
    return entry.get("mcp_endpoint") is True or str(function.get("description", "")).startswith("[MCP:")


def _publish_virtual_schemas(state: State, catalog: MCPCatalog) -> None:
    """Publish only explicitly enabled virtual MCP endpoints onto ``state.tool_schemas``."""
    for name, entry in list(state.tool_schemas.items()):
        if _owns_endpoint(entry):
            del state.tool_schemas[name]
    vtools = catalog.virtual_tools
    for name in sorted(_enabled_names(state) & vtools.keys()):
        if name in state.tool_schemas:
            log.warning("MCP endpoint %s conflicts with another tool; not published", name)
            continue
        state.tool_schemas[name] = {
            "available": True,
            "schema": vtools[name].schema,
            "mcp_endpoint": True,
        }


class _MCPBaseTool(Tool):
    """Base class for pure Sans-I/O MCP management tools."""

    optional_reads = {"tool_schemas"}
    writes = {"tool_schemas"}

    def __init__(self, catalog: MCPCatalog | None = None) -> None:
        super().__init__()
        self.catalog = catalog if catalog is not None else MCPCatalog()

    def _finish_ok(
        self,
        tc: ToolCall,
        result: Any,
        ts: float,
        emit: EmitFn,
    ) -> None:
        text = "" if result is None else str(result)
        tc.result = text
        if text.startswith(("ERROR:", "MCP TOOL ERROR")):
            tc.error = True
        tc.finished_at = ts
        tc._owner = None
        emit(ToolCallFinished(tc))


class MCPStatusTool(_MCPBaseTool):
    """Summarize MCP server availability, protocol, discovered/enabled counts, and errors."""

    name = "mcp_status"
    description = (
        "Summarize MCP server availability, protocol, discovered/enabled tool counts, "
        "and actionable errors. Use search_mcp to find endpoint names."
    )
    schema = {
        "type": "function",
        "function": {
            "name": "mcp_status",
            "description": description,
            "parameters": {"type": "object", "properties": {}},
        },
    }

    def call(self, state: State):
        outcome = yield MCPStatusRequest(enabled=tuple(sorted(_enabled_names(state))))
        if isinstance(outcome, MCPStatusResult):
            cat = _update_catalog(state, self.catalog, outcome.catalog)
            _publish_virtual_schemas(state, cat)
            return outcome.text
        return str(outcome)


class MCPRefreshTool(_MCPBaseTool):
    """Reload MCP config and schedule catalog discovery without enabling endpoints."""

    name = "mcp_refresh"
    description = (
        "Reload MCP config and refresh the catalog without enabling endpoints. "
        "Returns immediately by default; use search_mcp then enable_mcp_tool to expose selected schemas."
    )
    schema = {
        "type": "function",
        "function": {
            "name": "mcp_refresh",
            "description": description,
            "parameters": {
                "type": "object",
                "properties": {
                    "server": {
                        "type": ["string", "null"],
                        "description": "Optional configured MCP server name to refresh. Omit or null to refresh all servers.",
                    },
                    "wait_seconds": {
                        "type": ["number", "null"],
                        "description": "Optional small bounded wait for discovery to finish. Defaults to 0 (non-blocking) and is capped at 10 seconds.",
                    },
                },
            },
        },
    }

    def call(
        self,
        state: State,
        server: str | None = None,
        wait_seconds: float | None = None,
    ):
        outcome = yield MCPRefreshRequest(
            server=server or None,
            force=True,
            wait_seconds=float(wait_seconds or 0.0),
        )
        if isinstance(outcome, MCPRefreshResult):
            cat = _update_catalog(state, self.catalog, outcome.catalog)
            _publish_virtual_schemas(state, cat)
            return outcome.text
        return str(outcome)


class SearchMCPTool(_MCPBaseTool):
    """Search configured MCP endpoints by names/descriptions without loading schemas."""

    name = "search_mcp"
    description = (
        "Find configured MCP endpoints by names/descriptions without loading their schemas. "
        "Discovers missing catalogs with a bounded wait. Enable exact returned names with enable_mcp_tool before calling them."
    )
    schema = {
        "type": "function",
        "function": {
            "name": "search_mcp",
            "description": description,
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Case-insensitive words to match; empty lists the catalog."},
                    "server": {"type": ["string", "null"], "description": "Optional configured server name."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Results per page; default 20."},
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Page offset; default 0. Use the next offset shown when more matches are available.",
                    },
                },
            },
        },
    }

    def call(
        self,
        state: State,
        query: Any = "",
        server: Any = None,
        limit: Any = 20,
        offset: Any = 0,
    ):
        _validate_search_args(query=query, server=server, limit=limit, offset=offset)
        outcome = yield MCPSearchRequest(
            query=query,
            server=server,
            limit=limit,
            offset=offset,
        )
        if isinstance(outcome, MCPSearchResult):
            cat = _update_catalog(state, self.catalog, outcome.catalog)
            _publish_virtual_schemas(state, cat)
            search_result = outcome.result
        elif isinstance(outcome, dict):
            search_result = outcome
        else:
            return str(outcome)
        return _format_search_result(search_result, _enabled_names(state))


class EnableMCPTool(_MCPBaseTool):
    """Enable selected MCP endpoints for this session and expose their full schemas."""

    name = "enable_mcp_tool"
    description = (
        "Enable selected MCP endpoints for this session and expose their full schemas. "
        "Pass exact names returned by search_mcp. Additive and idempotent; unknown names reject the entire batch. "
        "Does not execute endpoints."
    )
    schema = {
        "type": "function",
        "function": {
            "name": "enable_mcp_tool",
            "description": description,
            "parameters": {
                "type": "object",
                "properties": {
                    "tool": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Exact endpoint names returned by search_mcp.",
                    },
                },
                "required": ["tool"],
            },
        },
    }
    writes = {"tool_schemas"}

    def call(self, state: State, tool: Any = None) -> str:
        if not isinstance(tool, list) or any(not isinstance(name, str) or not name for name in tool):
            raise MCPError("tool must be a list of exact endpoint names from search_mcp")
        cat = _session_catalog(state, self.catalog)
        vtools = cat.virtual_tools
        unknown = sorted(set(tool) - vtools.keys())
        if unknown:
            raise MCPError(f"Unknown MCP endpoint(s): {', '.join(unknown)}. Run search_mcp first. Nothing enabled.")
        for name in tool:
            existing = state.tool_schemas.get(name)
            if existing is not None and not _owns_endpoint(existing):
                raise MCPError(f"MCP endpoint {name!r} conflicts with another tool. Nothing enabled.")
        mcp_st = _mcp_state(state)
        mcp_st.enabled_tools = sorted(_enabled_names(state) | set(tool))
        _publish_virtual_schemas(state, cat)
        return _json_dump({"enabled": sorted(set(tool)), "enabled_count": len(mcp_st.enabled_tools)})


class MCPVirtualToolRouter:
    """Pure Sans-I/O component routing virtual MCP endpoint calls via ``MCPCallToolRequest``.

    Ordered after ``Tool`` and before ``UnknownToolFallback`` so real tools always
    take precedence on any name collision.
    """

    handles = (RunToolCall, IoCompleted, IoFailed, IoInterrupted)
    io_tag = "tool"
    reads = {"pending_tool_calls"}
    optional_reads = {"tool_schemas"}
    writes = {"tool_schemas"}

    def __init__(self, catalog: MCPCatalog | None = None) -> None:
        self.catalog = catalog if catalog is not None else MCPCatalog()

    def handle(self, event: Event, state: State, emit: EmitFn) -> None:
        if isinstance(event, RunToolCall):
            self._handle_run_tool_call(event, state, emit)
        elif isinstance(event, (IoCompleted, IoFailed, IoInterrupted)):
            self._handle_io_event(event, state, emit)

    def _handle_run_tool_call(self, event: RunToolCall, state: State, emit: EmitFn) -> None:
        tc = event.tool_call
        if tc.result is not None:
            return
        vtools = _session_catalog(state, self.catalog).virtual_tools
        if tc.name not in vtools:
            return
        existing = state.tool_schemas.get(tc.name)
        if existing is not None and not _owns_endpoint(existing):
            return  # Leave a colliding foreign tool call to its rightful handler.

        event.stop()
        if tc.started_at is None:
            tc.started_at = event.ts

        enabled = _enabled_names(state)
        if tc.name not in enabled:
            self._finish_error(
                tc,
                MCPError(f"MCP endpoint {tc.name!r} is disabled. Use enable_mcp_tool(tool=[{tc.name!r}]) first."),
                event.ts,
                emit,
            )
            return
        if not _owns_endpoint(state.tool_schemas.get(tc.name, {})):
            self._finish_error(
                tc,
                MCPError(f"MCP endpoint {tc.name!r} is not published; search and enable it again."),
                event.ts,
                emit,
            )
            return

        tc._owner = self
        tc.waiting_op = state.io.submit(
            MCPCallToolRequest(
                virtual_name=tc.name,
                arguments=dict(tc.parsed_args or {}),
            ),
            tag=self.io_tag,
        )

    def _handle_io_event(
        self,
        event: IoCompleted | IoFailed | IoInterrupted,
        state: State,
        emit: EmitFn,
    ) -> None:
        tc = self._find_waiting_call(state, event.op_id)
        if tc is None:
            return
        tc.waiting_op = None
        tc._owner = None
        if isinstance(event, IoCompleted):
            val = event.value
            if isinstance(val, MCPCallResult):
                if val.catalog is not None:
                    cat = _update_catalog(state, self.catalog, val.catalog)
                    _publish_virtual_schemas(state, cat)
                text = val.text
                is_error = bool(val.is_error or text.startswith(("ERROR:", "MCP TOOL ERROR")))
            else:
                text = "" if val is None else str(val)
                is_error = text.startswith(("ERROR:", "MCP TOOL ERROR"))
            tc.result = text
            tc.error = is_error
            tc.finished_at = event.ts
            emit(ToolCallFinished(tc))
        elif isinstance(event, IoInterrupted):
            reason = str(event.reason or "interrupted")
            tc.result = f"INTERRUPTED: MCP tool '{tc.name}' was interrupted ({reason})."
            tc.error = True
            tc.finished_at = event.ts
            emit(ToolCallFinished(tc))
        else:
            err = event.error
            exc = err if isinstance(err, BaseException) else RuntimeError(str(err))
            self._finish_error(tc, exc, event.ts, emit)

    def on_deadline(self, tc: ToolCall, state: State, emit: EmitFn) -> bool:
        if tc.waiting_op is not None:
            state.io.cancel(tc.waiting_op)
            tc.waiting_op = None
        tc.result = f"ERROR: Tool '{tc.name}' timed out (batch deadline exceeded)."
        tc.error = True
        tc._owner = None
        return False

    def _find_waiting_call(self, state: State, op_id: str) -> ToolCall | None:
        pending = state.pending_tool_calls
        cursor = state.tool_cursor
        if 0 <= cursor < len(pending):
            tc = pending[cursor]
            if tc._owner is self and tc.waiting_op == op_id:
                return tc
        for tc in pending:
            if tc._owner is self and tc.waiting_op == op_id:
                return tc
        return None

    @staticmethod
    def _finish_error(tc: ToolCall, exc: BaseException, ts: float, emit: EmitFn) -> None:
        tc.result = f"ERROR: {type(exc).__name__}: {exc}"
        tc.error = True
        tc.finished_at = ts
        tc._owner = None
        emit(ToolCallFinished(tc))


class MCPToolSchemaPublisher:
    """Pure Sans-I/O component publishing enabled virtual MCP schemas after ``SchemaPublisher``.

    Never performs network I/O or starts MCP servers. Also updates the in-memory
    catalog on ``MCPCatalogUpdated`` events pushed by ``MCPClientDriver`` and
    re-syncs on ``SessionRestored``.
    """

    handles = (RenderRequested, TurnSettled, MCPCatalogUpdated, SessionRestored)
    reads = {"tool_schemas"}
    writes = {"tool_schemas"}

    def __init__(self, catalog: MCPCatalog | None = None) -> None:
        self.catalog = catalog if catalog is not None else MCPCatalog()

    def handle(self, event: Event, state: State, emit: EmitFn) -> None:
        del emit
        if isinstance(event, MCPCatalogUpdated):
            cat = _update_catalog(state, self.catalog, event.catalog)
        else:
            cat = _session_catalog(state, self.catalog)
            if isinstance(event, SessionRestored) and cat is not self.catalog:
                cat.update(self.catalog.virtual_tools)
        _publish_virtual_schemas(state, cat)


# ---------------------------------------------------------------------------
# /config Settings (pure Sans-I/O via in-memory _SettingsBox + StoreUpdate)
# ---------------------------------------------------------------------------

class _SettingsBox:
    """Pure in-memory container for MCP settings loaded once at feature build time.

    Performs zero filesystem I/O; ``set_value`` updates memory and schedules
    crash-safe persistence via ``ctx.submit_persist_io(StoreUpdate(...))``.
    """

    def __init__(
        self,
        document: dict[str, Any],
        *,
        config_filename: str = "mcp_servers.json",
        load_error: str | None = None,
    ) -> None:
        self.document: dict[str, Any] = copy.deepcopy(document)
        self.config_filename: str = config_filename
        self.load_error: str | None = load_error

    def _entry(self, name: str, field_name: str) -> tuple[dict[str, Any], str]:
        if self.load_error is not None:
            raise SettingError(self.load_error)
        servers = self.document.get("mcpServers", self.document.get("servers", {})) or {}
        entry = servers.get(name)
        if not isinstance(entry, dict):
            raise SettingError("MCP server configuration changed; reload settings.")
        key = field_name
        if field_name == "timeout_seconds" and "timeout_seconds" not in entry and "timeout" in entry:
            key = "timeout"
        return entry, key

    def get_value(self, name: str, field_name: str) -> bool | int:
        entry, key = self._entry(name, field_name)
        raw = entry.get(key, True if field_name == "enabled" else _DEFAULT_TIMEOUT_SECONDS)
        if field_name == "enabled":
            if type(raw) is not bool:
                raise SettingError("MCP enabled must be a boolean.")
            return raw
        try:
            milliseconds = Decimal(str(raw)) * 1000
            if (
                not milliseconds.is_finite()
                or milliseconds != milliseconds.to_integral_value()
                or not 1000 <= milliseconds <= 600000
            ):
                raise ValueError
            return int(milliseconds)
        except (InvalidOperation, ValueError):
            raise SettingError("MCP timeout must be exact milliseconds between 1000 and 600000.") from None

    def set_value(self, name: str, field_name: str, value: Any, ctx: Any) -> None:
        entry, key = self._entry(name, field_name)
        if field_name == "enabled":
            if type(value) is not bool:
                raise SettingError("MCP enabled must be a boolean.")
            new_val: Any = value
        else:
            if type(value) is not int or not 1000 <= value <= 600000:
                raise SettingError("MCP timeout must be integer milliseconds between 1000 and 600000.")
            new_val = value / 1000

        previous = copy.deepcopy(self.document)
        entry[key] = new_val
        updated = copy.deepcopy(self.document)

        def _rollback() -> None:
            self.document = previous

        if ctx is not None and callable(getattr(ctx, "submit_persist_io", None)):
            ctx.submit_persist_io(
                StoreUpdate(
                    plugin=_PLUGIN_NAME,
                    scope="config",
                    config_filename=self.config_filename,
                    value=updated,
                ),
                rollback=_rollback,
            )
        if ctx is not None and callable(getattr(ctx, "submit_io", None)):
            ctx.submit_io(MCPSyncCatalogRequest(force=True), tag="mcp_settings")


def _resolve_config_filename(path: Path) -> str:
    """Preserve the relative path under ``plugin-configs/azo-plugin-mcp-client/``."""
    config_dir = _user_config_dir()
    try:
        rel = path.relative_to(config_dir).as_posix()
        if rel and rel != ".":
            return rel
    except ValueError:
        pass
    return path.name or "mcp_servers.json"


def _load_settings_box() -> _SettingsBox:
    """Build-time config loader populating an in-memory ``_SettingsBox``."""
    path = _default_config_path()
    config_filename = _resolve_config_filename(path)
    try:
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return _SettingsBox({}, config_filename=config_filename)
        if path.suffix.lower() in {".yaml", ".yml"}:
            import yaml

            document = yaml.safe_load(text)
        else:
            document = json.loads(_strip_json_line_comments(text))
        if not isinstance(document, dict):
            raise ValueError
        servers = document.get("mcpServers", document.get("servers", {}))
        if servers is not None and (
            not isinstance(servers, dict)
            or any(not isinstance(name, str) for name in servers)
        ):
            raise ValueError
        return _SettingsBox(document, config_filename=config_filename)
    except Exception:
        return _SettingsBox(
            {},
            config_filename=config_filename,
            load_error="Cannot read MCP configuration; repair the file and reload.",
        )


def _mcp_settings() -> list[Setting]:
    box = _load_settings_box()
    if box.load_error is not None:
        def unreadable(ctx):
            del ctx
            raise SettingError(box.load_error or "Cannot read MCP configuration; repair the file and reload.")

        return [
            Setting(
                "mcp.configuration",
                "Configuration unavailable",
                "bool",
                unreadable,
                group="MCP",
                apply="reload",
            )
        ]
    settings: list[Setting] = []
    servers = box.document.get("mcpServers", box.document.get("servers", {})) or {}
    for name, entry in servers.items():
        if not isinstance(entry, dict):
            continue
        # Stable keys do not depend on server-name punctuation or disclose connection data.
        prefix = "mcp.server_" + hashlib.sha256(name.encode()).hexdigest()[:16]
        for field_name, label, kind, bounds in (
            ("enabled", "Enabled", "bool", {}),
            ("timeout_seconds", "Timeout (ms)", "int", {"min": 1000, "max": 600000}),
        ):
            setting_key = "timeout_ms" if field_name == "timeout_seconds" else field_name
            settings.append(
                Setting(
                    f"{prefix}.{setting_key}",
                    f"{_clean_token(name)[:100]}: {label}",
                    kind,
                    get=lambda ctx, _box=box, n=name, f=field_name: _box.get_value(n, f),
                    set=lambda ctx, value, _box=box, n=name, f=field_name: _box.set_value(n, f, value, ctx),
                    group="MCP",
                    apply="live",
                    help=(
                        "Configured value; applied on the next MCP configuration poll/operation. "
                        "Adding or removing servers requires reloading the settings catalog."
                    ),
                    **bounds,
                )
            )
    return settings


# ---------------------------------------------------------------------------
# Feature Composition & Plugin Entry Point
# ---------------------------------------------------------------------------

def mcp_client_feature(
    *,
    catalog: MCPCatalog | None = None,
    registry: MCPClientRegistry | None = None,
    include_driver: bool = True,
) -> Feature:
    """Create the Sans-I/O MCP client ``Feature`` and optional ``MCPClientDriver``."""
    shared_catalog = catalog if catalog is not None else MCPCatalog()
    drivers: list[Any] = []
    if include_driver:
        if registry is not None:
            drivers.append(lambda _cfg=None, _reg=registry: MCPClientDriver(registry=_reg))
        else:
            drivers.append(lambda _cfg=None: MCPClientDriver())
    return Feature(
        name="mcp_client_addon",
        components=[
            MCPStatusTool(shared_catalog),
            MCPRefreshTool(shared_catalog),
            SearchMCPTool(shared_catalog),
            EnableMCPTool(shared_catalog),
            MCPVirtualToolRouter(shared_catalog),
            MCPToolSchemaPublisher(shared_catalog),
        ],
        order=[
            Tool,
            MCPVirtualToolRouter,
            UnknownToolFallback,
            SchemaPublisher,
            MCPToolSchemaPublisher,
        ],
        drivers=drivers,
        durable_state=MCP_CLIENT_DURABLE_SPEC,
    )


def register_features(builder, *, config):
    """Register MCP settings and Sans-I/O MCP client + driver features."""
    del config
    builder.add(Feature(name="mcp_settings", components=_mcp_settings(), order=[Setting]))
    builder.add(mcp_client_feature())
