"""Start one local block-emulator run and reap only the processes we own.

The caller prepares a fresh output root and a configuration snapshot.  This
module never modifies the repository's configuration or kills unrelated nodes.
"""

from __future__ import annotations

import ipaddress
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from typing import Any


class LaunchError(RuntimeError):
    """A run could not start or finish normally; owned children were cleaned up."""


def _parse_addresses(addresses: list[str], expected: int) -> list[tuple[str, int]]:
    if len(addresses) != expected:
        raise LaunchError(f"Expected {expected} worker/supervisor addresses, got {len(addresses)}")
    endpoints: list[tuple[str, int]] = []
    for address in addresses:
        try:
            host, port_text = address.rsplit(":", 1)
            ip = ipaddress.IPv4Address(host)
            port = int(port_text)
        except (ValueError, AttributeError) as exc:
            raise LaunchError(f"Invalid local IPv4 address: {address!r}") from exc
        if not ip.is_loopback or not 1 <= port <= 65535:
            raise LaunchError(f"Only IPv4 loopback addresses and ports 1..65535 are supported: {address}")
        endpoints.append((str(ip), port))
    if len(set(endpoints)) != len(endpoints):
        raise LaunchError("Worker and supervisor addresses must be distinct")
    return endpoints


def _check_ports_free(endpoints: list[tuple[str, int]]) -> None:
    # Hold all binds together, so duplicates/overlapping binds cannot pass.
    sockets: list[socket.socket] = []
    try:
        for endpoint in endpoints:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sockets.append(probe)
            # Go's TCP listener enables SO_REUSEADDR. Match that behaviour so
            # TIME_WAIT from the preceding serial run does not block restart.
            # No SO_REUSEPORT: an active listener must still fail this bind.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(endpoint)
            except OSError as exc:
                raise LaunchError(f"Port unavailable: {endpoint[0]}:{endpoint[1]} ({exc})") from exc
    finally:
        for probe in sockets:
            probe.close()


def _listener_ready(endpoint: tuple[str, int]) -> bool:
    try:
        with socket.create_connection(endpoint, timeout=0.2):
            return True
    except OSError:
        return False


def launch_run(
    *, binary: Path, project_root: Path, config_path: Path, output_root: Path,
    shards: int, nodes: int, environment: dict, addresses: list[str],
    startup_timeout: float = 90, run_timeout: float = 0,
    shutdown_timeout: float = 90,
) -> dict:
    """Run all workers, then the supervisor, and wait for normal termination.

    ``run_timeout=0`` has no deadline; the observation window is not a shutdown
    command. Each child has its own process group. Any failure terminates and
    reaps only this invocation's live children. Progress and exit codes remain
    in ``output_root/launcher/processes.json``, including failed runs.
    """
    if shards <= 0 or nodes <= 0:
        raise LaunchError("Shard and node counts must be positive")
    if (not all(math.isfinite(value) for value in (startup_timeout, shutdown_timeout, run_timeout))
            or startup_timeout <= 0 or shutdown_timeout <= 0 or run_timeout < 0):
        raise LaunchError("Startup/shutdown timeouts must be positive; run timeout must be nonnegative")
    endpoints = _parse_addresses(addresses, shards * nodes + 1)
    launch_dir = Path(output_root) / "launcher"
    try:
        launch_dir.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        raise LaunchError(f"Cannot create fresh launcher directory {launch_dir}: {exc}") from exc

    children: list[dict[str, Any]] = []
    state: dict[str, Any] = {
        "status": "starting", "config_path": str(Path(config_path).resolve()),
        "started_unix_ns": time.time_ns(), "finished_unix_ns": None,
        "processes": [], "error": None,
    }
    metadata = launch_dir / "processes.json"
    env = {str(key): str(value) for key, value in environment.items()}
    env["BLOCKEMULATOR_CONFIG"] = str(Path(config_path).resolve())
    executable = str(Path(binary).resolve())

    def save() -> None:
        temporary = metadata.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(state, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temporary, metadata)

    def refresh() -> None:
        changed = False
        for child in children:
            record = child["record"]
            if record["exit_code"] is None:
                code = child["process"].poll()
                if code is not None:
                    record["exit_code"] = code
                    record["exited_unix_ns"] = time.time_ns()
                    changed = True
        if changed:
            save()

    def start(name: str, arguments: list[str]) -> dict[str, Any]:
        command = [executable, *arguments]
        log_path = launch_dir / f"{name}.log"
        record: dict[str, Any] = {
            "name": name, "command": command, "log": str(log_path),
            "pid": None, "started_unix_ns": time.time_ns(),
            "exited_unix_ns": None, "exit_code": None,
        }
        state["processes"].append(record)
        save()
        try:
            with log_path.open("xb") as log:
                process = subprocess.Popen(
                    command, cwd=str(project_root), env=env,
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        except OSError as exc:
            record["start_error"] = str(exc)
            save()
            raise LaunchError(f"Cannot start {name}: {exc}") from exc
        record["pid"] = process.pid
        child = {"record": record, "process": process}
        children.append(child)
        save()
        return child

    def check_nonzero() -> None:
        refresh()
        for child in children:
            record = child["record"]
            if record["exit_code"] not in (None, 0):
                raise LaunchError(
                    f"{record['name']} exited with code {record['exit_code']}; see {record['log']}"
                )

    def cleanup() -> None:
        errors = []
        # poll() before killpg() avoids signalling a group after its leader has
        # already been reaped and its PID could have been reused.
        for child in children:
            process = child["process"]
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    child["record"]["cleanup_signal"] = "SIGTERM"
                except ProcessLookupError:
                    pass
                except OSError as exc:
                    errors.append(f"TERM {process.pid}: {exc}")
        deadline = time.monotonic() + 5
        for child in children:
            process = child["process"]
            try:
                process.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                pass
        for child in children:
            process = child["process"]
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                    child["record"]["cleanup_signal"] = "SIGKILL"
                except ProcessLookupError:
                    pass
                except OSError as exc:
                    errors.append(f"KILL {process.pid}: {exc}")
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                errors.append(f"Could not reap PID {process.pid} after SIGKILL")
        refresh()
        if errors:
            state["cleanup_errors"] = errors

    save()
    try:
        _check_ports_free(endpoints)
        startup_deadline = time.monotonic() + startup_timeout
        workers = []
        for shard in range(shards):
            for node in range(nodes):
                workers.append(start(
                    f"s{shard}n{node}",
                    ["-s", str(shard), "-n", str(node), "-S", str(shards), "-N", str(nodes)],
                ))
        pending = set(range(len(workers)))
        while pending:
            check_nonzero()
            for child in workers:
                if child["record"]["exit_code"] is not None:
                    raise LaunchError(f"{child['record']['name']} exited before supervisor startup")
            if time.monotonic() >= startup_deadline:
                raise LaunchError(f"Worker listeners not ready within {startup_timeout:g} seconds")
            for index in tuple(pending):
                if _listener_ready(endpoints[index]):
                    pending.remove(index)
            if pending:
                time.sleep(0.1)
        # A worker can exit during the final listener probe.
        check_nonzero()
        if any(child["record"]["exit_code"] is not None for child in workers):
            raise LaunchError("A worker exited before supervisor startup")
        supervisor = start("supervisor", ["-c", "-S", str(shards), "-N", str(nodes)])
        state["status"] = "running"
        save()
        started = time.monotonic()
        shutdown_deadline: float | None = None
        while True:
            check_nonzero()
            now = time.monotonic()
            finished = [child["record"]["exit_code"] is not None for child in children]
            if all(finished):
                state["status"] = "completed"
                break
            # Workers may acknowledge Stop and exit before the supervisor's
            # final statistics flush. Bound that shutdown phase either way.
            if any(finished) and shutdown_deadline is None:
                shutdown_deadline = now + shutdown_timeout
                state["status"] = "stopping"
                save()
            if shutdown_deadline is not None and now >= shutdown_deadline:
                raise LaunchError(f"Processes did not finish within {shutdown_timeout:g} seconds after first exit")
            if run_timeout and now - started >= run_timeout:
                raise LaunchError(f"Run exceeded {run_timeout:g} seconds after supervisor startup")
            time.sleep(0.1)
    except KeyboardInterrupt:
        state["status"] = "failed"
        state["error"] = "Interrupted; owned processes were stopped"
        raise
    except Exception as exc:
        state["status"] = "failed"
        state["error"] = str(exc)
        if isinstance(exc, LaunchError):
            raise
        raise LaunchError(f"Launch failed: {exc}") from exc
    finally:
        cleanup()
        state["finished_unix_ns"] = time.time_ns()
        save()
    return state
