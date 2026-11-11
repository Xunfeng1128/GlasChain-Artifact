"""Small generated fixtures only: these tests never launch a blockchain process."""
import copy
import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("summarize", Path(__file__).with_name("summarize.py"))
metrics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(metrics)
NS = metrics.NS


class Fixture:
    def __init__(self, directory, expected=1, covered=200, fee=5, gas_limit=1000):
        self.directory = directory
        self.rows = []
        self.height = 0
        self.meta = {
            "schema_version": 1, "status": "closed", "clock": "supervisor_receipt_monotonic",
            "config": {"method": 9, "consensus_method": "P_Louvain_Contrib", "shard_count": 4,
                       "expected_transactions": expected, "inject_speed": 2000,
                       "block_gas_limit": gas_limit, "cross_call_gas": fee,
                       "transactions_file": "fixture.csv", "calls_file": "fixture_calls.csv",
                       "config_path": "fixture.json", "config_sha256": "fixture"},
            "first_injection_unix_ns": 1_700_000_000 * NS,
            "observed_duration_ns": int(covered * NS),
        }

    def row(self, event, time, **kwargs):
        result = dict.fromkeys(metrics.COLUMNS, "")
        result.update(event=event, elapsed_ns=str(int(time * NS)), observed_unix_ns=str(self.meta["first_injection_unix_ns"] + int(time * NS)))
        result.update({k: str(v) for k, v in kwargs.items()})
        self.rows.append(result)
        return result

    def inject(self, source, time=0, base=40):
        return self.row("inject", time, source_tx_hash=source, stage="TOP", base_gas=base)

    def stage(self, source, time, stage="LOCAL", calls=0, base=40, block=None, length=1):
        if block is None:
            self.height += 1
            block = self.height
            self.row("block", time, shard_id=0, block_height=block, block_hash=f"block-{block}", block_body_length=length)
        attached = calls * self.meta["config"]["cross_call_gas"] if stage != "RELAY1" else 0
        top = int(stage != "LOCAL")
        internal = int(calls > 0)
        return self.row("stage", time, source_tx_hash=source, stage=stage,
                        worker_commit_unix_ns=self.meta["first_injection_unix_ns"] + int(time * NS) - 1,
                        shard_id=0, block_height=block, block_hash=f"block-{block}", block_body_length=length,
                        base_gas=base, stage_gas=base + attached, cross_call_count=calls,
                        attached_gas=attached, top_cross=top, internal_cross=internal,
                        union_cross=int(bool(top or internal)))

    def write(self):
        self.directory.mkdir()
        for event, key in (("inject", "injection_rows"), ("block", "block_rows"), ("stage", "stage_rows")):
            self.meta[key] = sum(row["event"] == event for row in self.rows)
        with (self.directory / "events.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=metrics.COLUMNS)
            writer.writeheader()
            writer.writerows(self.rows)
        self.write_meta()

    def write_meta(self):
        (self.directory / "run.json").write_text(json.dumps(self.meta))


class SummarizeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def run_fixture(self, fixture, duration=125, **kwargs):
        fixture.write()
        return metrics.summarize(fixture.directory, self.root / "out", 60 * NS, int(duration * NS), **kwargs)

    def windows(self):
        with (self.root / "out" / "windows.csv").open() as handle:
            return list(csv.DictReader(handle))

    def test_boundaries_partial_window_union_duplicates_and_late_relay1(self):
        f = Fixture(self.root / "run", expected=5)
        for source in "abcde":
            f.inject(source)
        f.stage("a", 59)                    # local, no crosses
        f.stage("b", 60, "RELAY2")          # TOP only, bin 1
        f.stage("b", 61, "RELAY1", calls=9) # out-of-order receipt; potential calls ignored
        f.stage("c", 119, calls=1)           # internal only
        replay = f.stage("d", 120, "RELAY2", calls=2)  # both, union counts once
        f.stage("d", 50, "RELAY1")
        f.stage("e", 125)                   # excludes exact requested endpoint
        replay = copy.deepcopy(replay)
        replay["elapsed_ns"] = str(121 * NS)
        replay["observed_unix_ns"] = str(f.meta["first_injection_unix_ns"] + 121 * NS)
        f.rows.insert(0, replay)             # later callback appears first in file
        result = self.run_fixture(f)
        windows = self.windows()
        self.assertEqual([int(row["completed"]) for row in windows], [1, 2, 1])
        self.assertEqual(windows[2]["duration_seconds"], "5.0")
        self.assertEqual(float(windows[2]["tps"]), 0.2)
        self.assertEqual(result["primary"]["completed"], 4)
        self.assertEqual(result["primary"]["tps"], 4 / 125)
        self.assertEqual(result["primary"]["top_cross_ratio"], 0.5)
        self.assertEqual(result["primary"]["internal_cross_ratio"], 0.5)
        self.assertEqual(result["primary"]["union_cross_ratio"], 0.75)
        self.assertEqual(result["full_run"]["completed_unique"], 5)
        self.assertEqual(result["duplicate_replay_rows"]["stage"], 1)
        self.assertTrue(result["valid_for_publication"])
        self.assertFalse((self.root / "out" / ".dedup.sqlite3").exists())

    def test_empty_window_zero_tps_na_ratio(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 130)
        result = self.run_fixture(f, duration=180)
        self.assertEqual(self.windows()[0]["top_cross_ratio"], "NA")
        self.assertEqual(float(self.windows()[0]["tps"]), 0)
        self.assertEqual(result["primary"]["completed"], 1)

    def test_no_completion_in_observation_summary_ratios_null(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 130)
        result = self.run_fixture(f, duration=60)
        self.assertEqual(result["primary"]["tps"], 0)
        self.assertIsNone(result["primary"]["union_cross_ratio"])

    def test_internal_cross_with_zero_fee_still_counted(self):
        f = Fixture(self.root / "run", fee=0)
        f.inject("a")
        f.stage("a", 20, calls=2)
        self.assertEqual(self.run_fixture(f)["primary"]["internal_cross_ratio"], 1)

    def test_relay1_straddles_window_and_never_counts_completion(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 59, "RELAY1")
        f.stage("a", 61, "RELAY2")
        self.run_fixture(f)
        self.assertEqual([int(row["completed"]) for row in self.windows()], [0, 1, 0])

    def test_full_run_requires_completion_even_if_primary_interval_covered(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)
        report = json.loads((self.root / "out" / "validation.json").read_text())
        self.assertIn("incomplete:unfinished_transaction", report["issue_counts"])
        self.assertFalse((self.root / "out" / "windows.csv").exists())

    def test_diagnostic_incomplete_does_not_pad_unobserved_time(self):
        f = Fixture(self.root / "run", covered=70)
        f.inject("a")
        result = self.run_fixture(f, allow_incomplete=True)
        self.assertFalse(result["valid_for_publication"])
        self.assertEqual(result["observation"]["analyzed_duration_seconds"], 70)
        self.assertEqual(self.windows()[1]["duration_seconds"], "10.0")
        self.assertIsNone(result["full_run"]["drain_seconds"])

    def test_short_coverage_fails_default(self):
        f = Fixture(self.root / "run", covered=70)
        f.inject("a")
        f.stage("a", 20)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_unclosed_log_fails(self):
        f = Fixture(self.root / "run")
        f.meta["status"] = "recording"
        f.inject("a")
        f.stage("a", 20)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_metadata_raw_count_detects_truncation(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 20)
        f.write()
        f.meta["stage_rows"] += 1
        f.write_meta()
        with self.assertRaises(metrics.InvalidRun):
            metrics.summarize(f.directory, self.root / "out", 60 * NS, 125 * NS, allow_incomplete=True)

    def test_duplicate_execution_in_distinct_blocks_is_always_invalid(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 20)
        f.stage("a", 21)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f, allow_incomplete=True)

    def test_conflicting_replay_is_invalid(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        row = copy.deepcopy(f.stage("a", 20))
        row.update(cross_call_count="1", attached_gas="5", stage_gas="45", internal_cross="1", union_cross="1")
        f.rows.append(row)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_local_plus_relay_is_invalid(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 20)
        f.stage("a", 30, "RELAY1")
        f.stage("a", 40, "RELAY2")
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_missing_relay1_is_incomplete(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 40, "RELAY2")
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_block_gas_sum_exceeding_limit_fails(self):
        f = Fixture(self.root / "run", expected=2, gas_limit=70)
        f.inject("a")
        f.inject("b")
        f.stage("a", 20, length=2)
        f.stage("b", 20, block=1, length=2)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_bad_gas_and_final_flags_are_rejected(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        row = f.stage("a", 20, calls=1)
        row["stage_gas"] = "40"
        row["internal_cross"] = "0"
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_expected_source_count_and_unknown_source_fail(self):
        f = Fixture(self.root / "run", expected=2)
        f.inject("a")
        f.stage("b", 20)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)

    def test_same_block_replay_and_injection_replay_deduplicated(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 20)
        f.rows += copy.deepcopy(f.rows)
        result = self.run_fixture(f)
        self.assertEqual(result["full_run"]["dispatched_unique"], 1)
        self.assertEqual(result["full_run"]["completed_unique"], 1)
        self.assertEqual(result["duplicate_replay_rows"], {"inject": 1, "block": 1, "stage": 1})

    def test_stage_timestamp_must_match_its_block_receipt(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        row = f.stage("a", 59)
        row["elapsed_ns"] = str(60 * NS)  # otherwise silently moves completion to bin 1
        row["observed_unix_ns"] = str(f.meta["first_injection_unix_ns"] + 60 * NS)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f, allow_incomplete=True)
        report = json.loads((self.root / "out" / "validation.json").read_text())
        self.assertIn("integrity:stage_block_receipt_mismatch", report["issue_counts"])

    def test_complete_block_replay_may_arrive_out_of_order(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 59)
        later = copy.deepcopy(f.rows[1:])
        for row in later:
            row["elapsed_ns"] = str(61 * NS)
            row["observed_unix_ns"] = str(f.meta["first_injection_unix_ns"] + 61 * NS)
        f.rows = later + f.rows
        self.run_fixture(f)
        self.assertEqual([int(row["completed"]) for row in self.windows()], [1, 0, 0])

    def test_warmup_start_and_duration(self):
        f = Fixture(self.root / "run", expected=2)
        f.inject("a")
        f.inject("b")
        f.stage("a", 59)
        f.stage("b", 60)
        result = self.run_fixture(f, duration=65, start_ns=60 * NS)
        self.assertEqual(result["primary"]["completed"], 1)
        self.assertEqual(result["primary"]["tps"], 1 / 65)
        self.assertEqual(self.windows()[0]["start_seconds"], "60.0")

    def test_refuses_overwrite(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 20)
        self.run_fixture(f)
        with self.assertRaises(metrics.InvalidRun):
            metrics.summarize(f.directory, self.root / "out", 60 * NS, 125 * NS)

    def test_actual_injection_latency_gas_weighting_and_completion_cohort(self):
        f = Fixture(self.root / "run", expected=3, covered=200)
        f.inject("a", base=10)
        f.inject("b", time=4, base=30)
        f.inject("b", time=7, base=30)  # retry must not shorten latency
        f.inject("c", time=10, base=60)
        f.stage("a", 10, base=10)
        f.stage("b", 5, "RELAY1", base=30)
        f.stage("b", 20, "RELAY2", calls=2, base=30)
        f.stage("c", 30, base=60)
        result = self.run_fixture(f, duration=20, start_ns=10 * NS)
        window, full = result["primary"], result["full_run"]
        self.assertEqual(window["completed"], 2)  # includes 10; excludes 30
        self.assertEqual(window["tps"], 2 / 20)
        self.assertEqual(window["base_gas"], 40)  # excludes RELAY1 and attached Gas
        self.assertEqual(window["base_gas_per_second"], 2)
        self.assertEqual(window["mean_base_gas"], 20)
        self.assertEqual(window["latency_sum_ns"], 26 * NS)
        self.assertEqual(window["gas_latency_sum_ns"], 580 * NS)
        self.assertEqual(window["mean_latency_seconds"], 13)
        self.assertEqual(window["gas_weighted_mean_latency_seconds"], 14.5)
        self.assertEqual(window["injected"], 1)
        self.assertEqual(window["injected_base_gas"], 60)
        self.assertEqual(window["outstanding_at_start"], 2)
        self.assertEqual(window["outstanding_at_end"], 1)
        self.assertEqual(window["outstanding_base_gas_at_end"], 60)
        self.assertEqual(full["completed"], 3)  # full includes last receipt at 30
        self.assertEqual(full["duration_seconds"], 30)  # not logger close at 200
        self.assertEqual(full["tps"], 3 / 30)
        self.assertEqual(full["base_gas"], 100)
        self.assertEqual(full["base_gas_per_second"], 100 / 30)
        self.assertEqual(full["mean_latency_seconds"], 46 / 3)
        self.assertEqual(full["gas_weighted_mean_latency_seconds"], 17.8)
        self.assertEqual(full["drain_seconds"], 20)
        self.assertEqual(full["outstanding_base_gas"], 0)
        self.assertEqual(float(self.windows()[0]["mean_latency_seconds"]), 13)

    def test_bin_sums_and_backlog_match_selected_interval(self):
        f = Fixture(self.root / "run", expected=3)
        f.inject("a", base=10)
        f.inject("b", time=60, base=20)
        f.inject("c", time=125, base=30)  # exact end injection excluded
        f.stage("a", 60, base=10)
        f.stage("b", 125, base=20)  # exact end completion excluded
        f.stage("c", 130, base=30)
        result = self.run_fixture(f)
        bins = self.windows()
        self.assertEqual([int(row["injected"]) for row in bins], [1, 1, 0])
        self.assertEqual([int(row["completed"]) for row in bins], [0, 1, 0])
        self.assertEqual([int(row["outstanding_at_end"]) for row in bins], [1, 1, 1])
        self.assertEqual([int(row["outstanding_base_gas_at_end"]) for row in bins], [10, 20, 20])
        for key in ("completed", "base_gas", "latency_sum_ns", "gas_latency_sum_ns", "injected", "injected_base_gas"):
            self.assertEqual(sum(int(row[key]) for row in bins), result["primary"][key])
        self.assertEqual(result["primary"]["injected_before_end"], 2)
        self.assertEqual(result["primary"]["completed_before_end"], 1)

    def test_empty_completion_sample_has_zero_goodput_and_null_means(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 130)
        result = self.run_fixture(f, duration=60)
        primary = result["primary"]
        self.assertEqual(primary["base_gas_per_second"], 0)
        for key in ("mean_latency_seconds", "gas_weighted_mean_latency_seconds", "mean_base_gas"):
            self.assertIsNone(primary[key])
            self.assertEqual(self.windows()[0][key], "NA")
        self.assertEqual(primary["outstanding_at_end"], 1)
        self.assertEqual(primary["outstanding_base_gas_at_end"], 40)

    def test_integer_gas_and_weighted_sums_beyond_sqlite_int64(self):
        base = 2**62
        f = Fixture(self.root / "run", expected=2, gas_limit=2**63 - 1, fee=0)
        f.inject("a", base=base)
        f.inject("b", time=1, base=base)
        f.stage("a", 2, base=base)
        f.stage("b", 3, base=base)
        result = self.run_fixture(f)
        for cohort in (result["primary"], result["full_run"]):
            self.assertEqual(cohort["base_gas"], 2**63)
            self.assertIsInstance(cohort["base_gas"], int)
            self.assertEqual(cohort["gas_latency_sum_ns"], 2**64 * NS)
            self.assertIsInstance(cohort["gas_latency_sum_ns"], int)
            self.assertEqual(cohort["gas_weighted_mean_latency_seconds"], 2)

    def test_block_gas_sum_beyond_int64_is_rejected_exactly(self):
        f = Fixture(self.root / "run", expected=2, gas_limit=2**63 - 1, fee=0)
        for source in "ab":
            f.inject(source, base=2**62)
        f.stage("a", 20, base=2**62, length=2)
        f.stage("b", 20, base=2**62, length=2, block=1)
        with self.assertRaises(metrics.InvalidRun):
            self.run_fixture(f)
        report = json.loads((self.root / "out" / "validation.json").read_text())
        self.assertIn("integrity:block_gas_overflow", report["issue_counts"])

    def test_zero_duration_full_run_has_null_rates(self):
        f = Fixture(self.root / "run")
        f.inject("a")
        f.stage("a", 0)
        full = self.run_fixture(f)["full_run"]
        self.assertEqual(full["duration_seconds"], 0)
        self.assertIsNone(full["tps"])
        self.assertIsNone(full["base_gas_per_second"])
        self.assertEqual(full["mean_latency_seconds"], 0)

    def test_incomplete_diagnostics_preserve_outstanding_gas(self):
        f = Fixture(self.root / "run", expected=2)
        f.inject("a", base=20)
        f.inject("b", time=10, base=50)
        f.stage("a", 20, base=20)
        result = self.run_fixture(f, allow_incomplete=True)
        self.assertFalse(result["valid_for_publication"])
        self.assertFalse(result["full_run"]["complete"])
        self.assertEqual(result["full_run"]["outstanding"], 1)
        self.assertEqual(result["full_run"]["outstanding_base_gas"], 50)
        self.assertEqual(result["primary"]["outstanding_base_gas_at_end"], 50)
        self.assertIsNone(result["full_run"]["drain_seconds"])


if __name__ == "__main__":
    unittest.main()
