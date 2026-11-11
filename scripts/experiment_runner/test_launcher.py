"""Launcher lifecycle tests use fake processes, clocks and sockets only."""

import json
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import launcher


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


class FakeProcess:
    def __init__(self, pid, clock, exit_at=None, exit_code=0, ignore_term=False):
        self.pid = pid
        self.clock = clock
        self.exit_at = exit_at
        self.exit_code = exit_code
        self.forced = None
        self.ignore_term = ignore_term
        self.wait_calls = 0

    def poll(self):
        if self.forced is not None:
            return self.forced
        if self.exit_at is not None and self.clock.value >= self.exit_at:
            return self.exit_code
        return None

    def wait(self, timeout=None):
        self.wait_calls += 1
        code = self.poll()
        if code is None:
            raise subprocess.TimeoutExpired("fake", timeout)
        return code


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.clock = FakeClock()
        self.processes = []
        self.commands = []
        self.signals = []
        self.plans = [(1.0, 0, False), (1.2, 0, False)]
        self.ready = True
        self.spawn_error_index = None

        def spawn(command, **kwargs):
            index = len(self.processes)
            self.commands.append((command, kwargs))
            if index == self.spawn_error_index:
                raise OSError("fake spawn failure")
            exit_at, code, ignore_term = self.plans[index]
            process = FakeProcess(1000 + index, self.clock, exit_at, code, ignore_term)
            self.processes.append(process)
            return process

        def killpg(pid, sig):
            self.signals.append((pid, sig))
            process = next(item for item in self.processes if item.pid == pid)
            self.assertIsNone(process.poll(), "Never signal a reaped child")
            if sig != signal.SIGTERM or not process.ignore_term:
                process.forced = -sig

        patches = [
            patch.object(launcher.subprocess, "Popen", side_effect=spawn),
            patch.object(launcher.os, "killpg", side_effect=killpg),
            patch.object(launcher.time, "monotonic", side_effect=self.clock.monotonic),
            patch.object(launcher.time, "sleep", side_effect=self.clock.sleep),
            patch.object(launcher, "_check_ports_free"),
            patch.object(launcher, "_listener_ready", side_effect=lambda endpoint: self.ready),
        ]
        for replacement in patches:
            replacement.start()
            self.addCleanup(replacement.stop)

    def run_launcher(self, **overrides):
        kwargs = dict(
            binary=self.root / "blockEmulator", project_root=self.root,
            config_path=self.root / "runtime_config.json", output_root=self.root / "result",
            shards=1, nodes=1, environment={"PATH": "/bin", "TEST": "value"},
            addresses=["127.0.0.1:28800", "127.0.0.1:18800"],
            startup_timeout=1, shutdown_timeout=1,
        )
        kwargs.update(overrides)
        return launcher.launch_run(**kwargs)

    def read_state(self):
        return json.loads((self.root / "result/launcher/processes.json").read_text())

    def test_normal_exit_commands_and_metadata(self):
        result = self.run_launcher()
        self.assertEqual(result["status"], "completed")
        self.assertEqual([item["exit_code"] for item in result["processes"]], [0, 0])
        self.assertEqual(self.commands[0][0][1:], ["-s", "0", "-n", "0", "-S", "1", "-N", "1"])
        self.assertEqual(self.commands[1][0][1:], ["-c", "-S", "1", "-N", "1"])
        for _, arguments in self.commands:
            self.assertTrue(arguments["start_new_session"])
            self.assertEqual(arguments["env"]["TEST"], "value")
            self.assertEqual(arguments["env"]["BLOCKEMULATOR_CONFIG"], str((self.root / "runtime_config.json").resolve()))
        self.assertEqual(self.signals, [])
        self.assertTrue(all(item.wait_calls for item in self.processes))
        self.assertEqual(self.read_state(), result)

    def test_zero_exit_before_supervisor_is_failure(self):
        self.plans = [(0, 0, False)]
        with self.assertRaisesRegex(launcher.LaunchError, "before supervisor"):
            self.run_launcher()
        self.assertEqual(len(self.processes), 1)
        self.assertEqual(self.read_state()["status"], "failed")
        self.assertEqual(self.signals, [])

    def test_nonzero_stops_only_live_owned_processes(self):
        self.plans = [(0.3, 2, False), (None, 0, False)]
        with self.assertRaisesRegex(launcher.LaunchError, "code 2"):
            self.run_launcher()
        self.assertEqual(self.signals, [(1001, signal.SIGTERM)])
        self.assertEqual(self.read_state()["processes"][0]["exit_code"], 2)
        self.assertEqual(self.read_state()["processes"][1]["exit_code"], -signal.SIGTERM)

    def test_startup_timeout_cleans_workers_without_supervisor(self):
        self.plans = [(None, 0, False)]
        self.ready = False
        with self.assertRaisesRegex(launcher.LaunchError, "listeners not ready"):
            self.run_launcher()
        self.assertEqual(len(self.processes), 1)
        self.assertEqual(self.signals, [(1000, signal.SIGTERM)])

    def test_supervisor_exit_bounds_worker_shutdown(self):
        self.plans = [(None, 0, False), (0.2, 0, False)]
        with self.assertRaisesRegex(launcher.LaunchError, "after first exit"):
            self.run_launcher()
        self.assertEqual(self.signals, [(1000, signal.SIGTERM)])

    def test_worker_exit_bounds_supervisor_shutdown(self):
        self.plans = [(0.2, 0, False), (None, 0, False)]
        with self.assertRaisesRegex(launcher.LaunchError, "after first exit"):
            self.run_launcher()
        self.assertEqual(self.signals, [(1001, signal.SIGTERM)])

    def test_timeout_kills_term_resistant_owned_child(self):
        self.plans = [(None, 0, True), (None, 0, False)]
        with self.assertRaisesRegex(launcher.LaunchError, "Run exceeded"):
            self.run_launcher(run_timeout=0.2)
        self.assertEqual(self.signals, [
            (1000, signal.SIGTERM), (1001, signal.SIGTERM), (1000, signal.SIGKILL),
        ])
        self.assertTrue(all(item.wait_calls for item in self.processes))

    def test_spawn_error_preserves_failed_command_and_cleans_worker(self):
        self.plans = [(None, 0, False)]
        self.spawn_error_index = 1
        with self.assertRaisesRegex(launcher.LaunchError, "Cannot start supervisor"):
            self.run_launcher()
        state = self.read_state()
        self.assertEqual(state["status"], "failed")
        self.assertIn("start_error", state["processes"][1])
        self.assertIsNone(state["processes"][1]["pid"])
        self.assertEqual(self.signals, [(1000, signal.SIGTERM)])

    def test_keyboard_interrupt_records_failure_and_cleans_worker(self):
        self.plans = [(None, 0, False)]
        with patch.object(launcher, "_listener_ready", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.run_launcher()
        self.assertEqual(self.signals, [(1000, signal.SIGTERM)])
        self.assertEqual(self.read_state()["status"], "failed")

    def test_busy_port_does_not_spawn_anything(self):
        with patch.object(launcher, "_check_ports_free", side_effect=launcher.LaunchError("Port unavailable")):
            with self.assertRaisesRegex(launcher.LaunchError, "Port unavailable"):
                self.run_launcher()
        self.assertEqual(self.processes, [])
        self.assertEqual(self.read_state()["status"], "failed")

    def test_existing_launcher_directory_is_never_overwritten(self):
        directory = self.root / "result/launcher"
        directory.mkdir(parents=True)
        sentinel = directory / "processes.json"
        sentinel.write_text("keep me")
        with self.assertRaisesRegex(launcher.LaunchError, "fresh launcher directory"):
            self.run_launcher()
        self.assertEqual(sentinel.read_text(), "keep me")
        self.assertEqual(self.processes, [])

    def test_nonfinite_timeouts_are_rejected_before_spawning(self):
        for option, value in [("run_timeout", float("nan")), ("startup_timeout", float("inf")),
                              ("shutdown_timeout", float("-inf"))]:
            with self.subTest(option=option):
                with self.assertRaisesRegex(launcher.LaunchError, "timeouts"):
                    self.run_launcher(**{option: value})
        self.assertEqual(self.processes, [])


class AddressTests(unittest.TestCase):
    def test_loopback_only_and_unique(self):
        self.assertEqual(launcher._parse_addresses(["127.0.0.1:1234"], 1), [("127.0.0.1", 1234)])
        for addresses, count in [
            (["192.168.1.2:1234"], 1), (["localhost:1234"], 1),
            (["::1:1234"], 1), (["127.0.0.1:0"], 1),
            (["127.0.0.1:65536"], 1), (["127.0.0.1:1234"] * 2, 2),
            (["127.0.0.1:1234"], 2),
        ]:
            with self.subTest(addresses=addresses, count=count):
                with self.assertRaises(launcher.LaunchError):
                    launcher._parse_addresses(addresses, count)

    def test_bind_failure_closes_every_probe(self):
        first, second = MagicMock(), MagicMock()
        second.bind.side_effect = OSError("busy")
        with patch.object(launcher.socket, "socket", side_effect=[first, second]):
            with self.assertRaisesRegex(launcher.LaunchError, "Port unavailable"):
                launcher._check_ports_free([("127.0.0.1", 1), ("127.0.0.1", 2)])
        first.close.assert_called_once()
        second.close.assert_called_once()
        for probe in (first, second):
            probe.setsockopt.assert_called_once_with(launcher.socket.SOL_SOCKET, launcher.socket.SO_REUSEADDR, 1)


if __name__ == "__main__":
    unittest.main()
