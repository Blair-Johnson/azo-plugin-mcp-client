"""Dry-build both host pipelines against this checkout, without live installation."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import agent_zoo

host = Path(agent_zoo.__file__).resolve().parents[1]
source = Path(__file__).resolve().parents[1] / "src/mcp_client.py"
scripts = host / "skills/agent-zoo-userspace-plugins/scripts"
subprocess.run([sys.executable, str(scripts / "static_inspect_plugin.py"), str(source)], check=True)
with tempfile.TemporaryDirectory(prefix="mcp-plugin-check-") as temporary:
    root = Path(temporary)
    common = root / "plugins/common"
    common.mkdir(parents=True)
    (common / "mcp_client.py").symlink_to(source)
    env = dict(os.environ, AGENT_ZOO_HOME=str(root / "home"))
    result = subprocess.run([
        sys.executable, str(scripts / "inspect_agent_zoo_plugins.py"),
        "--repo-root", str(host), "--plugin-root", str(root / "plugins"),
        "--pipeline", "both", "--init-state", "--json",
    ], env=env, text=True, capture_output=True)
    if result.returncode:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)
    report = json.loads(result.stdout)
    if report.get("errors"):
        raise AssertionError(report["errors"])
    for name, pipeline in report["pipelines"].items():
        encoded = json.dumps(pipeline)
        assert "mcp_client_addon" in encoded, (name, pipeline)
        assert "mcp_enabled_tools" in encoded, (name, pipeline)
        print(f"{name}: MCP plugin registered; session selection state initialized")
