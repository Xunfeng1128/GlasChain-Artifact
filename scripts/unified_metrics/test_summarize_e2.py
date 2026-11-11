"""Offline E2 protocol fixtures; no emulator processes are started."""
import copy
import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import summarize_e2 as e2
from test_summarize import Fixture


def source(n):
    return f"0x{n:064x}"


class E2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "analysis"
        self.prefix = self.root / "cohort.csv"
        self.ids = [source(i) for i in range(1, 5)]
        self.prefix.write_text("source_tx_hash\n" + "\n".join(self.ids[:2]) + "\n", encoding="ascii")
        self.windows = [60 * e2.ledger.NS, 120 * e2.ledger.NS]

    def fixture(self, scoped):
        f = Fixture(self.root / "raw", expected=4 if scoped else 3, covered=160)
        a, b, c, _ = self.ids
        # CSV injection order may differ from the prespecified valid-input order.
        f.inject(b)
        f.inject(c, time=1)
        f.inject(a, time=7)
        f.stage(b, 60)                    # exclusive boundary
        f.stage(a, 30, "RELAY1")          # not a completed transaction yet
        row = f.stage(a, 150, "RELAY2")   # after both windows, still in E2-b
        f.rows.append(copy.deepcopy(row)) # same receipt replay
        if scoped:
            f.meta.update(completion_scope="e2", e2_prefix_sha256=hashlib.sha256(self.prefix.read_bytes()).hexdigest(),
                          e2_prefix_count=2, e2_observation_seconds=120,
                          completion_check=dict(expected_transactions=4, unique_injected=3, unique_completed=2,
                                                unique_final_sources=2, e2_prefix_count=2,
                                                e2_prefix_injected=2, e2_prefix_completed=2))
        else:
            f.stage(c, 50)
        return f

    def run_e2(self, f, prefixes=(1, 2)):
        f.write()
        return e2.summarize_e2(f.directory, self.output, self.prefix, self.windows, prefixes)

    def rows(self, name):
        with (self.output / name).open() as handle:
            return list(csv.DictReader(handle))

    def test_scoped_validation_partial_background_edges_and_input_order(self):
        result = self.run_e2(self.fixture(True))
        self.assertTrue(result["valid_for_e2"])
        self.assertFalse(result["all_input_complete"])
        self.assertEqual((result["outstanding"], result["unissued"]), (1, 1))
        self.assertEqual(result["duplicate_replay_rows"], {"stage": 1})
        windows = self.rows("e2_throughput.csv")
        self.assertEqual([int(w["completed"]) for w in windows], [0, 1])
        self.assertAlmostEqual(float(windows[1]["tps"]), 1 / 120)
        lat = self.rows("e2_latency.csv")
        self.assertEqual([float(l["mean_latency_seconds"]) for l in lat], [143.0, 101.5])
        self.assertEqual(int(lat[1]["confirmed_before_observation_end"]), 1)
        self.assertEqual(float(lat[1]["p95_latency_seconds"]), 143)

    def test_full_legacy_archive_and_unrelated_outputs_untouched(self):
        self.output.mkdir()
        (self.output / "validation.json").write_text("legacy")
        info = self.run_e2(self.fixture(False))
        self.assertTrue(info["all_input_complete"])
        self.assertEqual((self.output / "validation.json").read_text(), "legacy")
        self.assertEqual([int(w["completed"]) for w in self.rows("e2_throughput.csv")], [1, 2])
        with self.assertRaises(e2.ledger.InvalidRun):
            e2.summarize_e2(self.root / "raw", self.output, self.prefix, self.windows, [1, 2])

    def test_legacy_full_validation_rejects_scoped_even_if_fully_complete(self):
        f = self.fixture(False)
        f.meta["completion_scope"] = "e2"
        f.write()
        with self.assertRaisesRegex(e2.ledger.InvalidRun, "completion scope"):
            e2.ledger.summarize(f.directory, self.output, self.windows[0], self.windows[1], allow_incomplete=True)

    def test_failed_or_incomplete_e2_never_publishes_csvs(self):
        def missing_prefix(f):
            f.rows = [row for row in f.rows if row["stage"] != "RELAY2"]

        def conflicting(f):
            row = copy.deepcopy(next(row for row in f.rows if row["stage"] == "RELAY2"))
            row["block_hash"] = "different_block"
            f.rows.append(row)

        cases = [missing_prefix, conflicting, lambda f: f.meta.update(status="recording"),
                 lambda f: f.meta.update(e2_prefix_sha256="wrong"),
                 lambda f: f.meta["completion_check"].update(unique_injected=4),
                 lambda f: f.meta.update(completion_failure="stopped early"),
                 lambda f: f.meta.update(observed_duration_ns=119 * e2.ledger.NS)]
        for i, mutate in enumerate(cases):
            with self.subTest(case=i):
                f = self.fixture(True)
                f.directory = self.root / f"run{i}"
                self.output = self.root / f"out{i}"
                mutate(f)
                with self.assertRaises(e2.ledger.InvalidRun):
                    self.run_e2(f)
                info = json.loads((self.output / "e2_validation.json").read_text())
                self.assertFalse(info["valid_for_e2"])
                self.assertFalse(list(self.output.glob("*.csv")))

    def test_incomplete_full_run_cannot_be_promoted_to_e2(self):
        f = self.fixture(True)
        del f.meta["completion_scope"]
        with self.assertRaises(e2.ledger.InvalidRun):
            self.run_e2(f)

    def test_reject_duplicate_input_and_noncanonical_hashes(self):
        for i, invalid in enumerate([self.ids[0], self.ids[1][2:], "invalid", ""]):
            with self.subTest(value=invalid):
                self.prefix.write_text(f"source_tx_hash\n{self.ids[0]}\n{invalid}\n")
                f = self.fixture(True)
                f.directory = self.root / f"run{i}"
                self.output = self.root / f"out{i}"
                with self.assertRaises(e2.ledger.InvalidRun):
                    self.run_e2(f)

    def test_nearest_rank_p95(self):
        ids = [source(i + 100) for i in range(21)]
        self.prefix.write_text("source_tx_hash\n" + "\n".join(ids) + "\n")
        f = Fixture(self.root / "raw", expected=21, covered=160)
        for i, key in enumerate(ids):
            f.inject(key)
            f.stage(key, i + 1)
        self.run_e2(f, prefixes=[21])
        self.assertEqual(float(self.rows("e2_latency.csv")[0]["p95_latency_seconds"]), 20)


if __name__ == "__main__":
    unittest.main()
