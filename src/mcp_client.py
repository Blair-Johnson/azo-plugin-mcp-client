"""Userspace MCP client addon for agent-zoo.

This plugin publishes already-discovered MCP tools as virtual agent-zoo tools
and routes matching tool calls over MCP ``tools/call``. Discovery is explicit
or background-only so unreliable MCP servers cannot stall normal harness turns.

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
_DEFAULT_PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2024-11-05")
_DEFAULT_TIMEOUT_SECONDS = 15.0
_MAX_SERVER_TIMEOUT_SECONDS = 30.0
_CONFIG_POLL_SECONDS = 2.0
_DISCOVERY_RETRY_SECONDS = 60.0
_DEFAULT_REFRESH_WAIT_SECONDS = 0.0
_MAX_FUNCTION_NAME = 64


class MCPError(RuntimeError):
    """Raised when an MCP server returns an error or violates the protocol."""


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
    install_root = os.environ.get("AGENT_ZOO_INSTALL_ROOT", "").strip()
    if install_root:
        return Path(install_root).expanduser()
    xdg_data = os.environ.get("XDG_DATA_HOME", "").strip()
    base = Path(xdg_data).expanduser() if xdg_data else Path.home() / ".local" / "share"
    return base / "agent-zoo"


def _user_config_dir() -> Path:
    return _agent_zoo_data_root() / "orchestrated" / "plugin-configs" / _PLUGIN_NAME
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
    """Small synchronous stdio MCP client built on JSON-RPC 2.0."""

    def __init__(self, config: MCPServerConfig):
        self.config = config
        self.process: subprocess.Popen[str] | None = None
        self._next_id = 1
        self._write_lock = threading.Lock()
        self._responses: dict[int, dict[str, Any]] = {}
        self._condition = threading.Condition()
        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stderr_tail: deque[str] = deque(maxlen=40)
        self._initialized = False
        self.protocol_version: str | None = None
        self.tools_dirty = True
        self.last_error: str | None = None

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self) -> None:
        if self.is_running() and self._initialized:
            return
        self.close()

        env = os.environ.copy()
        env.update(self.config.env)
        argv = [self.config.command, *self.config.args]
        try:
            self.process = subprocess.Popen(
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
        except FileNotFoundError as exc:
            raise MCPError(f"command not found for MCP server {self.config.name}: {argv[0]}") from exc
        except Exception as exc:
            raise MCPError(f"failed to start MCP server {self.config.name}: {exc}") from exc

        self._stdout_thread = threading.Thread(
            target=self._stdout_loop,
            name=f"mcp-stdout-{self.config.name}",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._stderr_loop,
            name=f"mcp-stderr-{self.config.name}",
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()
        self._initialize()

    def close(self) -> None:
        proc = self.process
        self.process = None
        self._initialized = False
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            pass
        with self._condition:
            self._condition.notify_all()

    def _stdout_loop(self) -> None:
        proc = self.process
        stdout = proc.stdout if proc is not None else None
        if stdout is None:
            return
        for line in stdout:
            text = line.strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except Exception:
                log.warning("Ignoring non-JSON line from MCP server %s: %r", self.config.name, text[:200])
                continue
            self._handle_message(message)
        with self._condition:
            self._condition.notify_all()

    def _stderr_loop(self) -> None:
        proc = self.process
        stderr = proc.stderr if proc is not None else None
        if stderr is None:
            return
        for line in stderr:
            text = line.rstrip()
            if text:
                self._stderr_tail.append(text)

    def _handle_message(self, message: Any) -> None:
        if not isinstance(message, dict):
            return
        if "id" in message and ("result" in message or "error" in message):
            try:
                request_id = int(message["id"])
            except Exception:
                return
            with self._condition:
                self._responses[request_id] = message
                self._condition.notify_all()
            return

        method = str(message.get("method") or "")
        if method == "notifications/tools/list_changed":
            self.tools_dirty = True
            return

        # MCP allows server-to-client requests only for capabilities the client
        # advertises. We advertise none, but respond defensively so a chatty
        # server is not left hanging forever.
        if "id" in message and method:
            self._send_raw({
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32601, "message": "agent-zoo MCP client does not implement server requests"},
            })

    def _send_raw(self, message: dict[str, Any]) -> None:
        proc = self.process
        stdin = proc.stdin if proc is not None else None
        if proc is None or stdin is None or proc.poll() is not None:
            raise MCPError(f"MCP server {self.config.name} is not running")
        data = _compact_json(message) + "\n"
        with self._write_lock:
            stdin.write(data)
            stdin.flush()

    def _request(self, method: str, params: dict[str, Any] | None = None, timeout: float | None = None) -> dict[str, Any]:
        proc = self.process
        if proc is None or proc.poll() is not None:
            raise MCPError(f"MCP server {self.config.name} is not running")
        with self._write_lock:
            request_id = self._next_id
            self._next_id += 1
            message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
            if params is not None:
                message["params"] = params
            proc.stdin.write(_compact_json(message) + "\n")  # type: ignore[union-attr]
            proc.stdin.flush()  # type: ignore[union-attr]

        deadline = time.monotonic() + (timeout or self.config.timeout_seconds)
        with self._condition:
            while True:
                response = self._responses.pop(request_id, None)
                if response is not None:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    tail = self.stderr_tail()
                    extra = f"\nstderr tail:\n{tail}" if tail else ""
                    raise MCPError(f"timed out waiting for {method} from MCP server {self.config.name}{extra}")
                self._condition.wait(timeout=min(0.25, remaining))

        if "error" in response:
            error = response.get("error") or {}
            if isinstance(error, dict):
                code = error.get("code", "?")
                message = error.get("message", "MCP error")
                data = error.get("data")
                suffix = f" data={_json_dump(data)}" if data is not None else ""
                raise MCPError(f"{method} failed on {self.config.name}: [{code}] {message}{suffix}")
            raise MCPError(f"{method} failed on {self.config.name}: {_json_dump(error)}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise MCPError(f"{method} on {self.config.name} returned a non-object result")
        return result

    def _notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self._send_raw(message)

    def _initialize(self) -> None:
        errors: list[str] = []
        for version in _DEFAULT_PROTOCOL_VERSIONS:
            try:
                result = self._request(
                    "initialize",
                    {
                        "protocolVersion": version,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "agent-zoo-userspace-mcp-client",
                            "version": "0.1.0",
                        },
                    },
                    timeout=self.config.timeout_seconds,
                )
                self.protocol_version = str(result.get("protocolVersion") or version)
                self._notify("notifications/initialized")
                self._initialized = True
                self.tools_dirty = True
                self.last_error = None
                return
            except Exception as exc:
                errors.append(f"{version}: {exc}")
        self.close()
        raise MCPError("initialize failed for MCP server " + self.config.name + ": " + "; ".join(errors))

    def list_tools(self) -> list[dict[str, Any]]:
        self.start()
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
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
            cursor = str(next_cursor)
        self.tools_dirty = False
        self.last_error = None
        return tools

    def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.start()
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
                client.last_error = str(exc)
                self._next_retry_at[name] = time.monotonic() + _DISCOVERY_RETRY_SECONDS
                self._discovery_status[name] = f"error: {type(exc).__name__}: {exc}"
                current = threading.current_thread()
                if self._discovery_threads.get(name) is current:
                    self._discovery_threads.pop(name, None)
            log.warning("Could not discover tools for MCP server %s: %s", name, exc)
            return
        with self._lock:
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
        used_names = set(self._virtual_tools)
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

    def status_text(self) -> str:
        self._load_config(force=False)
        now = time.monotonic()
        with self._lock:
            status = {
                "config_path": str(self.config_path),
                "config_exists": self.config_path.exists(),
                "config_error": self._config_error,
                "servers": {},
                "virtual_tools": sorted(self._virtual_tools),
            }
            servers_status: dict[str, Any] = status["servers"]
            for name, cfg in sorted(self._servers.items()):
                client = self._clients.get(name)
                thread = self._discovery_threads.get(name)
                started = self._discovery_started_at.get(name)
                servers_status[name] = {
                    "enabled": cfg.enabled,
                    "command": [cfg.command, *cfg.args],
                    "cwd": cfg.cwd,
                    "timeout_seconds": cfg.timeout_seconds,
                    "tool_count": len(self._server_virtuals.get(name, set())),
                    "client": client.status() if client is not None else None,
                    "discovery": {
                        "status": self._discovery_status.get(name, "idle"),
                        "running": bool(thread is not None and thread.is_alive()),
                        "elapsed_seconds": round(now - started, 3) if started else None,
                    },
                    "next_retry_seconds": max(0.0, self._next_retry_at.get(name, 0.0) - now),
                }
        return "MCP STATUS\n" + _json_dump(status)


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
                    "description": "Show configured MCP servers, connection status, and currently published virtual MCP tools.",
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
                    "description": "Reload MCP config and schedule MCP tool discovery. Returns immediately by default so unreachable servers do not stall the harness.",
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

        try:
            self.registry.sync_for_publish()
        except Exception as exc:
            log.warning("MCP publish sync failed: %s", exc)
        for virtual in self.registry.virtual_tools.values():
            state.tool_schemas[virtual.virtual_name] = {
                "available": True,
                "schema": virtual.schema,
            }

    def transform(self, state):
        virtual_tools = self.registry.virtual_tools
        for tc in getattr(state, "pending_tool_calls", []) or []:
            if tc.result is not None:
                continue
            if tc.name == "mcp_status":
                try:
                    tc.result = self.registry.status_text()
                except Exception as exc:
                    tc.result = f"ERROR: MCP status failed: {type(exc).__name__}: {exc}"
                    tc.error = True
                self._notify(state, tc)
                continue
            if tc.name == "mcp_refresh":
                try:
                    args = tc.parsed_args or {}
                    server = args.get("server")
                    wait_seconds = args.get("wait_seconds", _DEFAULT_REFRESH_WAIT_SECONDS)
                    try:
                        wait_value = float(wait_seconds or 0.0)
                    except Exception:
                        wait_value = 0.0
                    tc.result = self.registry.refresh(
                        server=server or None,
                        force=True,
                        wait_seconds=wait_value,
                    )
                except Exception as exc:
                    tc.result = f"ERROR: MCP refresh failed: {type(exc).__name__}: {exc}"
                    tc.error = True
                self._notify(state, tc)
                continue
            if tc.name in virtual_tools:
                result = self.registry.call_virtual_tool(tc.name, tc.parsed_args or {})
                tc.result = result
                if result.startswith("ERROR:") or result.startswith("MCP TOOL ERROR"):
                    tc.error = True
                self._notify(state, tc)
        return state


def register_features(builder, *, session, config):
    builder.add(Feature(
        name="mcp_client_addon",
        components=[MCPVirtualToolRouter()],
    ))
