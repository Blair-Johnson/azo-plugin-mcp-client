"""Userspace MCP client addon for agent-zoo.

This plugin discovers MCP tools into a searchable catalog, publishes only
session-enabled endpoints, and routes their calls over MCP ``tools/call``.
Discovery is explicit or background-only so unreliable servers cannot stall turns.

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

from __future__ import annotations

import atexit
import hashlib
import json
import logging
import os
from collections import deque
import re
import shlex
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from agent_utils import Feature
from agent_utils.tool import Tool

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
_CONFIG_POLL_SECONDS = 2.0
_DISCOVERY_RETRY_SECONDS = 60.0
_DEFAULT_REFRESH_WAIT_SECONDS = 0.0
_MAX_FUNCTION_NAME = 64


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
    """Normalized configuration for one stdio MCP server."""

    def __init__(
        self,
        *,
        name: str,
        command: str,
        args: tuple[str, ...] = (),
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        enabled: bool = True,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        tool_prefix: str | None = None,
    ):
        self.name = name
        self.command = command
        self.args = tuple(args)
        self.env = dict(env or {})
        self.cwd = cwd
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
            self.command,
            self.args,
            tuple(sorted(self.env.items())),
            self.cwd,
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
    for env_name in ("AGENT_ZOO_HOME", "AGENT_ZOO_STATE_ROOT"):
        env_path = os.environ.get(env_name, "").strip()
        if env_path:
            return Path(env_path).expanduser()
    return Path.home() / ".local" / "share" / "agent-zoo"


def _user_config_dir() -> Path:
    return _agent_zoo_data_root() / "plugin-configs" / _PLUGIN_NAME
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
            command, args = _normalize_command(raw_entry.get("command"), raw_entry.get("args"))
            cwd = raw_entry.get("cwd")
            if cwd is not None:
                cwd = str(Path(str(cwd)).expanduser())
            timeout = float(raw_entry.get("timeout_seconds", raw_entry.get("timeout", _DEFAULT_TIMEOUT_SECONDS)))
            servers[name] = MCPServerConfig(
                name=name,
                command=command,
                args=args,
                env=_expand_env_map(raw_entry.get("env")),
                cwd=cwd,
                enabled=raw_entry.get("enabled", True) is not False,
                timeout_seconds=min(_MAX_SERVER_TIMEOUT_SECONDS, max(1.0, timeout)),
                tool_prefix=str(raw_entry.get("tool_prefix") or raw_entry.get("prefix") or "").strip() or None,
            )
        except Exception as exc:
            log.warning("Ignoring invalid MCP server config for %s: %s", raw_name, exc)
    return servers

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
                    argv, cwd=self.config.cwd or None, env=env,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, encoding="utf-8", errors="replace", bufsize=1,
                )
            except Exception as exc:
                self.last_error = f"failed to start MCP server {self.config.name}: {exc}"
                raise MCPError(self.last_error) from exc
            self.process = proc
            self._stdout_closed = False
            self._stdout_thread = threading.Thread(
                target=self._stdout_loop, args=(proc,),
                name=f"mcp-stdout-{self.config.name}", daemon=True,
            )
            self._stderr_thread = threading.Thread(
                target=self._stderr_loop, args=(proc,),
                name=f"mcp-stderr-{self.config.name}", daemon=True,
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
                    self._send_raw({"jsonrpc": "2.0", "method": "notifications/cancelled",
                                    "params": {"requestId": request_id, "reason": "Request timed out"}}, proc=proc)
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
            result = self._request("initialize", {
                "protocolVersion": _LEGACY_PROTOCOL_VERSIONS[0],
                "capabilities": {}, "clientInfo": dict(_CLIENT_INFO),
            })
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
        result = self._request("tools/call", {"name": tool_name, "arguments": arguments or {}},
                               timeout=self.config.timeout_seconds)
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

class MCPClientRegistry:
    """Loads MCP config, owns stdio clients, and caches virtual tool schemas."""

    def __init__(self):
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
        atexit.register(self.close_all)

    @property
    def virtual_tools(self) -> dict[str, MCPVirtualTool]:
        with self._lock:
            return dict(self._virtual_tools)

    def close_all(self) -> None:
        with self._lock:
            clients = list(self._clients.values())
        for client in clients:
            client.close()

    def refresh(
        self,
        server: str | None = None,
        force: bool = False,
        wait_seconds: float | None = _DEFAULT_REFRESH_WAIT_SECONDS,
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
        return self.status_text()

    def sync_for_publish(self) -> None:
        # This method is called on every pipeline pass while publishing tool
        # schemas. It must never perform network I/O or launch MCP servers.
        self._load_config(force=False)

    def _load_config(self, force: bool = False) -> None:
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

        for client in clients_to_close:
            client.close()

    def _discover_all(self, force: bool = False) -> None:
        with self._lock:
            names = sorted(self._servers)
        for name in names:
            self._schedule_discovery(name, force=force)

    def _schedule_discovery(self, name: str, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            cfg = self._servers.get(name)
            if cfg is None or not cfg.enabled:
                self._remove_server_virtuals(name)
                self._discovery_status[name] = "disabled"
                return
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
            cfg = self._servers.get(name)
            if cfg is None or not cfg.enabled:
                self._discovery_status[name] = "disabled"
                return
            if not force and now < self._next_retry_at.get(name, 0.0):
                self._discovery_status[name] = "backoff"
                return
            client = self._clients.get(name)
            if client is None:
                client = StdioMCPClient(cfg)
                self._clients[name] = client
            self._discovery_status[name] = "running"
            self._discovery_started_at[name] = now
        try:
            tools = client.list_tools()
        except Exception as exc:
            with self._lock:
                if self._clients.get(name) is not client or self._servers.get(name) != cfg:
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
        with self._lock:
            if self._clients.get(name) is not client or self._servers.get(name) != cfg:
                if self._discovery_threads.get(name) is threading.current_thread():
                    self._discovery_threads.pop(name, None)
                return
            self._install_virtual_tools(name, tools)
            self._next_retry_at.pop(name, None)
            self._discovery_status[name] = f"ok: {len(tools)} tool(s)"
            current = threading.current_thread()
            if self._discovery_threads.get(name) is current:
                self._discovery_threads.pop(name, None)

    def _remove_server_virtuals(self, server_name: str) -> None:
        for virtual_name in self._server_virtuals.pop(server_name, set()):
            self._virtual_tools.pop(virtual_name, None)

    def _install_virtual_tools(self, server_name: str, tools: list[dict[str, Any]]) -> None:
        self._remove_server_virtuals(server_name)
        cfg = self._servers[server_name]
        installed: set[str] = set()
        used_names = set(self._virtual_tools) | {
            "mcp_status", "mcp_refresh", "search_mcp", "enable_mcp_tool", "mcp_virtual_tool_router",
        }
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
        self, query: str = "", server: str | None = None, limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Search names/descriptions; discover missing or dirty catalogs, never call tools."""
        if not isinstance(query, str) or (server is not None and not isinstance(server, str)):
            raise MCPError("query and server must be strings")
        if type(limit) is not int or not 1 <= limit <= 50:
            raise MCPError("limit must be an integer from 1 to 50")
        if type(offset) is not int or offset < 0:
            raise MCPError("offset must be a nonnegative integer")
        self._load_config(force=False)
        with self._lock:
            if server is not None and _clean_token(server) not in self._servers:
                raise MCPError(f"unknown MCP server {server!r}")
            names = [_clean_token(server)] if server is not None else sorted(self._servers)
            discover = [name for name in names if self._servers[name].enabled and (
                name not in self._server_virtuals
                or getattr(self._clients.get(name), "tools_dirty", False)
            )]
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
                    "name": tool.virtual_name, "server": tool.server_name,
                    "tool": tool.tool_name, "description": tool.description[:320],
                })
            return {
                "matches": matches[offset:offset + limit], "total": len(matches),
                "next_offset": offset + limit if offset + limit < len(matches) else None,
                "discovery": {name: self._discovery_status.get(name, "idle") if self._servers[name].enabled else "disabled" for name in names if name in self._servers},
                "config_error": self._config_error,
            }

    def call_virtual_tool(self, virtual_name: str, arguments: dict[str, Any]) -> str:
        mapping = self._virtual_tools.get(virtual_name)
        if mapping is None:
            return f"ERROR: unknown MCP virtual tool {virtual_name!r}. Run mcp_refresh and mcp_status."
        cfg = self._servers.get(mapping.server_name)
        if cfg is None or not cfg.enabled:
            return f"ERROR: MCP server {mapping.server_name!r} is no longer configured or enabled."
        client = self._clients.get(mapping.server_name)
        if client is None:
            client = StdioMCPClient(cfg)
            self._clients[mapping.server_name] = client
        try:
            result = client.call_tool(mapping.tool_name, arguments or {})
            return _format_call_result(mapping, result)
        except Exception as exc:
            client.last_error = str(exc)
            return f"ERROR: MCP tool {mapping.server_name}/{mapping.tool_name} failed: {exc}"

    def status_text(self, enabled: set[str] | None = None) -> str:
        self._load_config(force=False)
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
                if phase == "connected" and client.protocol_version:
                    phase += f" (MCP {client.protocol_version})"
                tools = self._server_virtuals.get(name)
                catalog = "catalog not loaded" if tools is None else f"{len(tools)} {'cached ' if not running else ''}tool{'s' if len(tools) != 1 else ''}"
                parts = [f"{name}: {phase}", catalog]
                if enabled is not None:
                    parts.append(f"{len((tools or set()) & enabled)} enabled")
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


def _brief(text: Any, limit: int = 200) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"


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

class MCPVirtualToolRouter(Tool):
    """Publish virtual MCP tools and route matching calls to MCP servers."""

    name = "mcp_virtual_tool_router"
    description = "Publish and route virtual tools backed by configured MCP servers."
    schema = {
        "type": "function",
        "function": {
            "name": "mcp_virtual_tool_router",
            "description": "Internal MCP virtual tool router; use mcp_status, mcp_refresh, or published mcp_* tools instead.",
            "parameters": {"type": "object", "properties": {}},
        },
    }

    init = {"mcp_enabled_tools": list}
    optional_reads = {"mcp_enabled_tools"}
    writes = {"mcp_enabled_tools", "tool_schemas"}

    @staticmethod
    def _enabled_names(state) -> set[str]:
        names = getattr(state, "mcp_enabled_tools", [])
        return {name for name in names if isinstance(name, str)} if isinstance(names, list) else set()

    @staticmethod
    def _owns_endpoint(entry: dict[str, Any]) -> bool:
        # Recognize legacy eager publication as well as entries from this version.
        function = entry.get("schema", {}).get("function", {})
        return entry.get("mcp_endpoint") is True or str(function.get("description", "")).startswith("[MCP:")

    def _enable_tools(self, state, names: Any) -> str:
        if not isinstance(names, list) or any(not isinstance(name, str) or not name for name in names):
            raise MCPError("tool must be a list of exact endpoint names from search_mcp")
        catalog = self.registry.virtual_tools
        unknown = sorted(set(names) - catalog.keys())
        if unknown:
            raise MCPError(f"Unknown MCP endpoint(s): {', '.join(unknown)}. Run search_mcp first. Nothing enabled.")
        for name in names:
            existing = state.tool_schemas.get(name)
            if existing is not None and not self._owns_endpoint(existing):
                raise MCPError(f"MCP endpoint {name!r} conflicts with another tool. Nothing enabled.")
        state.mcp_enabled_tools = sorted(self._enabled_names(state) | set(names))
        self._publish(state)
        return _json_dump({"enabled": sorted(set(names)), "enabled_count": len(state.mcp_enabled_tools)})

    def __init__(self, registry: MCPClientRegistry | None = None):
        super().__init__()
        self.registry = registry or MCPClientRegistry()

    def _publish(self, state) -> None:
        state.tool_schemas["mcp_status"] = {
            "available": True,
            "schema": {
                "type": "function",
                "function": {
                    "name": "mcp_status",
                    "description": "Summarize MCP server availability, protocol, discovered/enabled tool counts, and actionable errors. Use search_mcp to find endpoint names.",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
        }
        state.tool_schemas["mcp_refresh"] = {
            "available": True,
            "schema": {
                "type": "function",
                "function": {
                    "name": "mcp_refresh",
                    "description": "Reload MCP config and refresh the catalog without enabling endpoints. Returns immediately by default; use search_mcp then enable_mcp_tool to expose selected schemas.",
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
                            }
                        },
                    },
                },
            },
        }

        state.tool_schemas["search_mcp"] = {
            "available": True,
            "schema": {"type": "function", "function": {
                "name": "search_mcp",
                "description": "Find configured MCP endpoints by names/descriptions without loading their schemas. Discovers missing catalogs with a bounded wait. Enable exact returned names with enable_mcp_tool before calling them.",
                "parameters": {"type": "object", "properties": {
                    "query": {"type": "string", "description": "Case-insensitive words to match; empty lists the catalog."},
                    "server": {"type": ["string", "null"], "description": "Optional configured server name."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Results per page; default 20."},
                    "offset": {"type": "integer", "minimum": 0, "description": "Page offset; default 0. Use the next offset shown when more matches are available."},
                }},
            }},
        }
        state.tool_schemas["enable_mcp_tool"] = {
            "available": True,
            "schema": {"type": "function", "function": {
                "name": "enable_mcp_tool",
                "description": "Enable selected MCP endpoints for this session and expose their full schemas. Pass exact names returned by search_mcp. Additive and idempotent; unknown names reject the entire batch. Does not execute endpoints.",
                "parameters": {"type": "object", "properties": {
                    "tool": {"type": "array", "items": {"type": "string"}, "description": "Exact endpoint names returned by search_mcp."},
                }, "required": ["tool"]},
            }},
        }
        try:
            self.registry.sync_for_publish()
        except Exception as exc:
            log.warning("MCP publish sync failed: %s", exc)
        for name, entry in list(state.tool_schemas.items()):
            if self._owns_endpoint(entry):
                del state.tool_schemas[name]
        catalog = self.registry.virtual_tools
        for name in sorted(self._enabled_names(state) & catalog.keys()):
            if name in state.tool_schemas:
                log.warning("MCP endpoint %s conflicts with another tool; not published", name)
                continue
            state.tool_schemas[name] = {
                "available": True, "schema": catalog[name].schema, "mcp_endpoint": True,
            }

    def transform(self, state):
        for tc in getattr(state, "pending_tool_calls", []) or []:
            if tc.result is not None:
                continue
            catalog = self.registry.virtual_tools
            if tc.name not in {"mcp_status", "mcp_refresh", "search_mcp", "enable_mcp_tool"} and tc.name not in catalog:
                continue
            existing = state.tool_schemas.get(tc.name)
            if tc.name in catalog and existing is not None and not self._owns_endpoint(existing):
                continue  # Leave a colliding foreign tool call to its rightful handler.
            try:
                args = tc.parsed_args or {}
                if tc.name == "mcp_status":
                    tc.result = self.registry.status_text(enabled=self._enabled_names(state))
                elif tc.name == "mcp_refresh":
                    tc.result = self.registry.refresh(
                        server=args.get("server") or None, force=True,
                        wait_seconds=float(args.get("wait_seconds") or 0.0),
                    )
                elif tc.name == "search_mcp":
                    result = self.registry.search_tools(**args)
                    tc.result = _format_search_result(result, self._enabled_names(state))
                elif tc.name == "enable_mcp_tool":
                    tc.result = self._enable_tools(state, args.get("tool"))
                elif tc.name not in self._enabled_names(state):
                    raise MCPError(f"MCP endpoint {tc.name!r} is disabled. Use enable_mcp_tool(tool=[{tc.name!r}]) first.")
                elif not self._owns_endpoint(state.tool_schemas.get(tc.name, {})):
                    raise MCPError(f"MCP endpoint {tc.name!r} is not published; search and enable it again.")
                else:
                    tc.result = self.registry.call_virtual_tool(tc.name, args)
                if tc.result.startswith(("ERROR:", "MCP TOOL ERROR")):
                    tc.error = True
            except Exception as exc:
                tc.result = f"ERROR: {type(exc).__name__}: {exc}"
                tc.error = True
            self._notify(state, tc)
            self._publish(state)
        return state


def register_features(builder, *, session, config):
    builder.add(Feature(
        name="mcp_client_addon",
        components=[MCPVirtualToolRouter()],
    ))
