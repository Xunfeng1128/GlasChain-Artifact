#!/usr/bin/env python3
"""Validate a fixed-duration E2 run and measure throughput and confirmation deadlines."""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

if __package__:
    from . import summarize as ledger
    from .summarize_e2 import write_csv
else:
    import summarize as ledger
    from summarize_e2 import write_csv


WINDOWS_SECONDS = (600, 960)
CLOCK = "supervisor_receipt_monotonic"
OUTPUTS = ("e2_throughput.csv", "e2_latency.csv")


def validate_windows(windows):
    if (not isinstance(windows, (list, tuple)) or len(windows) != 2
            or any(type(v) is not int or v <= 0 for v in windows)
            or windows[-1] - windows[0] < 3 or windows[-1] > WINDOWS_SECONDS[-1]
            or (windows[-1] == WINDOWS_SECONDS[-1] and tuple(windows) != WINDOWS_SECONDS)):
        raise ledger.InvalidRun("fixed-window E2 requires 600/960 seconds, or two shorter smoke windows at least 3 seconds apart")


def latency_policy(windows=WINDOWS_SECONDS):
    validate_windows(windows)
    dispatch_end, observation_end = windows
    followup = min(300, observation_end - dispatch_end)
    return {
        "dispatch_start_seconds": 0,
        "dispatch_end_seconds": dispatch_end,
        "thresholds_seconds": [max(1, followup // 5), max(2, 2 * followup // 5), followup],
        "followup_seconds": followup,
        "percentile_method": "nearest_rank",
    }


def throughput_rows(db, windows):
    for end in windows:
        cutoff = end * ledger.NS
        completed, = db.execute(
            "SELECT count(*) FROM stages WHERE stage IN ('LOCAL','RELAY2') AND elapsed < ?",
            (cutoff,),
        ).fetchone()
        injected, = db.execute("SELECT count(*) FROM injected WHERE elapsed < ?", (cutoff,)).fetchone()
        yield {"start_seconds": 0, "end_seconds": end, "completed": completed,
               "tps": completed / end, "injected": injected, "outstanding_at_end": injected - completed}


def latency_row(db, windows, policy):
    dispatch_end = policy["dispatch_end_seconds"]
    dispatch_cutoff = dispatch_end * ledger.NS
    followup = policy["followup_seconds"]
    observation_cutoff = windows[-1] * ledger.NS
    total, = db.execute("SELECT count(*) FROM injected WHERE elapsed < ?", (dispatch_cutoff,)).fetchone()
    db.execute("""
        CREATE TABLE confirmed_latency AS
        SELECT s.elapsed - i.elapsed AS latency
        FROM injected i JOIN stages s USING (source)
        WHERE i.elapsed < ? AND s.stage IN ('LOCAL','RELAY2')
          AND s.elapsed < ? AND s.elapsed - i.elapsed <= ?
    """, (dispatch_cutoff, observation_cutoff, followup * ledger.NS))
    db.execute("CREATE INDEX confirmed_latency_order ON confirmed_latency(latency)")
    result = {"dispatch_start_seconds": 0, "dispatch_end_seconds": dispatch_end,
              "followup_seconds": followup, "injected": total}
    for deadline in policy["thresholds_seconds"]:
        confirmed, = db.execute(
            "SELECT count(*) FROM confirmed_latency WHERE latency <= ?", (deadline * ledger.NS,)
        ).fetchone()
        result[f"confirmed_within_{deadline}s"] = confirmed
        result[f"confirmation_rate_within_{deadline}s"] = ledger.ratio(confirmed, total)
    result[f"unconfirmed_within_{followup}s"] = total - confirmed
    for percentile in (50, 95):
        rank = (total * percentile + 99) // 100
        if 0 < rank <= confirmed:
            latency, = db.execute(
                "SELECT latency FROM confirmed_latency ORDER BY latency LIMIT 1 OFFSET ?", (rank - 1,)
            ).fetchone()
            result[f"p{percentile}_latency_seconds"] = ledger.seconds(latency)
            result[f"p{percentile}_status"] = "measured"
        else:
            result[f"p{percentile}_latency_seconds"] = None
            result[f"p{percentile}_status"] = "beyond_followup"
    return result


def validate_metadata(meta, duration):
    if (meta.get("schema_version") != 1 or meta.get("clock") != CLOCK
            or meta.get("completion_scope") != "e2" or meta.get("e2_stop_policy") != "fixed_window"):
        raise ledger.InvalidRun("unsupported schema, clock, or E2 stop policy")
    if meta.get("status") != "closed" or meta.get("completion_failure"):
        raise ledger.InvalidRun("E2 requires a normally closed recorder")
    covered = ledger.integer(meta["observed_duration_ns"], "observed_duration_ns")
    ledger.integer(meta["first_injection_unix_ns"], "first_injection_unix_ns", 1)
    if meta.get("e2_observation_seconds") != duration or covered != duration * ledger.NS:
        raise ledger.InvalidRun(f"recorder did not cover exactly the {duration}-second E2 observation")
    if meta.get("e2_prefix_count") or meta.get("e2_prefix_sha256"):
        raise ledger.InvalidRun("fixed-window E2 must not require a transaction prefix")
    return covered


def validate_ledger(db, meta, issues):
    cfg = meta["config"]
    expected = ledger.integer(cfg["expected_transactions"], "expected_transactions", 1)
    ledger.query_issues(db, issues, require_all_completed=False)
    ledger.check_block_gas(db, ledger.integer(cfg["block_gas_limit"], "block_gas_limit", 1), issues)
    noncanonical = db.execute("""
        SELECT source FROM injected WHERE length(source)!=66 OR substr(source,1,2)!='0x'
           OR substr(source,3) GLOB '*[^0-9a-f]*' LIMIT 1
    """).fetchone()
    if noncanonical is not None:
        issues.add("noncanonical_source", repr(noncanonical))
    injected, first = db.execute("SELECT count(*),min(elapsed) FROM injected").fetchone()
    completed, = db.execute("SELECT count(*) FROM stages WHERE stage IN ('LOCAL','RELAY2')").fetchone()
    if first != 0:
        issues.add("invalid_origin", "first injection must have zero elapsed_ns")
    if completed > injected or injected > expected:
        issues.add("count_mismatch", f"injected={injected}, completed={completed}, expected={expected}")
    online = meta["completion_check"]
    for key, offline in (("expected_transactions", expected), ("unique_injected", injected),
                         ("unique_completed", completed), ("unique_final_sources", completed)):
        if ledger.integer(online[key], key) != offline:
            issues.add("online_offline_count_mismatch", f"{key} differs from raw events")
    return {"expected_transactions": expected, "unique_injected": injected, "unique_completed": completed,
            "outstanding": injected - completed, "unissued": expected - injected,
            "all_input_complete": injected == completed == expected}


def summarize_e2_window(run_dir, output_dir, windows=WINDOWS_SECONDS):
    policy = latency_policy(windows)
    run_dir, output_dir = Path(run_dir).resolve(), Path(output_dir).resolve()
    if output_dir == run_dir or run_dir in output_dir.parents:
        raise ledger.InvalidRun("E2 output must be outside the raw logs directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    validation = output_dir / "e2_validation.json"
    if any((output_dir / name).exists() for name in OUTPUTS):
        raise ledger.InvalidRun("E2 results already exist, choose a fresh output directory")
    # Reserve the analysis before writing tables and publish the validation last.
    with validation.open("x", encoding="utf-8") as f:
        json.dump({"valid_for_e2": False, "status": "analyzing"}, f)
    info = {"schema_version": 1, "valid_for_e2": False, "clock": CLOCK, "run_dir": str(run_dir),
            "completion_scope": "e2", "e2_stop_policy": "fixed_window",
            "windows_seconds": list(windows), "latency_policy": policy}
    issues = ledger.Issues()
    with tempfile.TemporaryDirectory(dir=output_dir, prefix=".e2-window-") as work:
        tmp = Path(work)
        published = []
        try:
            meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
            info["observed_duration_seconds"] = ledger.seconds(validate_metadata(meta, windows[-1]))
            db = sqlite3.connect(tmp / "ledger.sqlite3")
            try:
                ledger.initialize(db)
                raw, replays = ledger.ingest(db, run_dir / "events.csv", meta, issues, exclude_cutoff=True)
                info.update(raw_rows=raw, duplicate_replay_rows=replays)
                info.update(validate_ledger(db, meta, issues))
                if issues.counts:
                    raise ledger.InvalidRun("E2 event integrity checks failed")
                throughput = list(throughput_rows(db, windows))
                latency = latency_row(db, windows, policy)
            finally:
                db.close()
            for name, rows in zip(OUTPUTS, (throughput, [latency])):
                write_csv(tmp / name, rows)
                os.link(tmp / name, output_dir / name)
                published.append(output_dir / name)
            info.update(valid_for_e2=True, latency=latency, observation_injected=throughput[-1]["injected"],
                        observation_completed=throughput[-1]["completed"],
                        observation_outstanding=throughput[-1]["outstanding_at_end"])
        except (OSError, ValueError, KeyError, TypeError, csv.Error, sqlite3.Error, ledger.InvalidRun) as exc:
            if not issues.counts:
                issues.add("invalid_e2_window", str(exc))
            for path in published:
                path.unlink(missing_ok=True)
            raise ledger.InvalidRun(f"{exc}; see {validation}") from exc
        finally:
            detail = issues.report()
            detail.pop("valid_for_publication")
            ledger.write_json(tmp / validation.name, {**info, **detail})
            os.replace(tmp / validation.name, validation)
    return info


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--windows", nargs="+", type=int, default=WINDOWS_SECONDS)
    args = parser.parse_args(argv)
    try:
        summarize_e2_window(args.run_dir, args.output_dir, args.windows)
    except (OSError, ValueError, ledger.InvalidRun) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"Wrote fixed-window E2 throughput, latency and validation to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
