"""Validate fixed-window cutoffs against synthetic raw transaction events."""
import csv
import json
import tempfile
import unittest
from pathlib import Path

import summarize_e2_window as e2
from test_summarize import Fixture
from test_summarize_e2 import source


class FixedWindowE2Tests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.output = self.root / "analysis"

    def fixture(self):
        f = Fixture(self.root / "raw", expected=8, covered=960)
        f.meta.update(completion_scope="e2", e2_stop_policy="fixed_window", e2_observation_seconds=960,
                      completion_check=dict(expected_transactions=8, unique_injected=6,
                                            unique_completed=4, unique_final_sources=4))
        for i, at in enumerate((0, 10, 300, 599, 600, 959), 1):
            f.inject(source(i), at)
        for i, at in ((1, 60), (2, 130), (3, 600), (4, 900)):
            f.stage(source(i), at)
        return f

    def test_fixed_cutoff_and_unconfirmed_cohort_members(self):
        f = self.fixture()
        f.write()
        info = e2.summarize_e2_window(f.directory, self.output)
        self.assertTrue(info["valid_for_e2"])
        self.assertFalse(info["all_input_complete"])
        self.assertEqual((info["observation_injected"], info["observation_completed"]), (6, 4))
        with (self.output / "e2_throughput.csv").open() as handle:
            throughput = list(csv.DictReader(handle))
        self.assertEqual([(int(w["injected"]), int(w["completed"])) for w in throughput], [(4, 2), (6, 4)])
        with (self.output / "e2_latency.csv").open() as handle:
            latency, = csv.DictReader(handle)
        self.assertEqual(latency["injected"], "4")
        self.assertEqual([float(latency[f"confirmation_rate_within_{s}s"]) for s in (60, 120, 300)],
                         [0.25, 0.5, 0.75])
        self.assertEqual(latency["unconfirmed_within_300s"], "1")
        self.assertEqual(float(latency["p50_latency_seconds"]), 120)
        self.assertEqual(latency["p95_latency_seconds"], "")
        self.assertEqual(latency["p95_status"], "beyond_followup")

    def test_incomplete_observation_cannot_publish(self):
        f = self.fixture()
        f.meta["observed_duration_ns"] = 959 * e2.ledger.NS
        f.write()
        with self.assertRaisesRegex(e2.ledger.InvalidRun, "960-second"):
            e2.summarize_e2_window(f.directory, self.output)
        info = json.loads((self.output / "e2_validation.json").read_text())
        self.assertFalse(info["valid_for_e2"])
        self.assertFalse(list(self.output.glob("*.csv")))

    def test_raw_event_at_cutoff_cannot_publish(self):
        f = self.fixture()
        f.inject(source(1), 960)
        f.write()
        with self.assertRaisesRegex(e2.ledger.InvalidRun, "integrity"):
            e2.summarize_e2_window(f.directory, self.output)
        info = json.loads((self.output / "e2_validation.json").read_text())
        self.assertEqual(info["issue_counts"], {"integrity:invalid_event": 1})
        self.assertIn("fixed E2 cutoff", info["examples"][0]["message"])

    def test_explicit_short_smoke_windows(self):
        f = Fixture(self.root / "raw", expected=3, covered=20)
        f.meta.update(completion_scope="e2", e2_stop_policy="fixed_window", e2_observation_seconds=20,
                      completion_check=dict(expected_transactions=3, unique_injected=2,
                                            unique_completed=1, unique_final_sources=1))
        f.inject(source(1), 0)
        f.inject(source(2), 11)
        f.stage(source(1), 4)
        f.write()
        info = e2.summarize_e2_window(f.directory, self.output, [10, 20])
        self.assertEqual(info["windows_seconds"], [10, 20])
        self.assertEqual(info["latency_policy"]["thresholds_seconds"], [2, 4, 10])
        self.assertEqual(info["latency"]["confirmed_within_4s"], 1)

    def test_online_count_mismatch_cannot_publish(self):
        f = self.fixture()
        f.meta["completion_check"]["unique_completed"] = 5
        f.write()
        with self.assertRaisesRegex(e2.ledger.InvalidRun, "integrity"):
            e2.summarize_e2_window(f.directory, self.output)
        info = json.loads((self.output / "e2_validation.json").read_text())
        self.assertEqual(info["issue_counts"], {"integrity:online_offline_count_mismatch": 1})


if __name__ == "__main__":
    unittest.main()
