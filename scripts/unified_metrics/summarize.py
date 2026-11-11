#!/usr/bin/env python3
"""Validate and summarize unified logical-transaction metrics (Python stdlib only).

Input is a *closed* run.json/events.csv pair, never legacy TPS/CTX CSV files.
SQLite is a temporary on-disk deduplication index; CSV input is streamed.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path

NS = 1_000_000_000
COLUMNS = [
    "event", "source_tx_hash", "stage", "elapsed_ns", "observed_unix_ns",
    "worker_commit_unix_ns", "shard_id", "block_height", "block_hash",
    "block_body_length", "base_gas", "stage_gas", "cross_call_count",
    "attached_gas", "top_cross", "internal_cross", "union_cross",
]
WINDOW_COLUMNS = [
    "start_seconds", "end_seconds", "duration_seconds", "completed",
    "top_cross_completed", "internal_cross_completed", "union_cross_completed",
    "tps", "top_cross_ratio", "internal_cross_ratio", "union_cross_ratio",
    "base_gas", "base_gas_per_second", "mean_latency_seconds",
    "gas_weighted_mean_latency_seconds", "mean_base_gas",
    "latency_sum_ns", "gas_latency_sum_ns", "injected", "injected_base_gas",
    "injected_before_end", "completed_before_end",
    "outstanding_at_start", "outstanding_at_end",
    "outstanding_base_gas_at_start", "outstanding_base_gas_at_end",
]


class InvalidRun(Exception):
    pass


class Issues:
    def __init__(self):
        self.counts = Counter()
        self.examples = []

    def add(self, code, message, *, incomplete=False):
        kind = "incomplete" if incomplete else "integrity"
        self.counts[(kind, code)] += 1
        if len(self.examples) < 100:
            self.examples.append({"kind": kind, "code": code, "message": message})

    def report(self):
        return {
            "valid_for_publication": not bool(self.counts),
            "integrity_issue_count": sum(n for (k, _), n in self.counts.items() if k == "integrity"),
            "incomplete_issue_count": sum(n for (k, _), n in self.counts.items() if k == "incomplete"),
            "issue_counts": {f"{kind}:{code}": n for (kind, code), n in sorted(self.counts.items())},
            "examples": self.examples,
            "examples_limit": 100,
        }


def integer(value, name, minimum=0):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value and value.lstrip("-").isdigit():
        result = int(value)
    else:
        raise ValueError(f"{name} must be an integer, got {value!r}")
    if not minimum <= result <= 2**63 - 1:
        raise ValueError(f"{name} outside [{minimum}, int64 max]")
    return result


def boolean(value, name):
    if value in ("1", "true"):
        return 1
    if value in ("0", "false"):
        return 0
    raise ValueError(f"{name} must be 0/1 or true/false, got {value!r}")


def seconds_ns(value, name, positive=False):
    try:
        exact = Decimal(str(value)) * NS
        if not exact.is_finite() or exact != exact.to_integral_value():
            raise ValueError()
        result = int(exact)
        if result < 0 or (positive and result == 0):
            raise ValueError()
        return result
    except (InvalidOperation, ValueError, OverflowError):
        raise ValueError(f"{name} must be {'positive' if positive else 'nonnegative'} seconds with at most 9 decimals") from None


def seconds(value):
    return value / NS


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


class ExactSum:
    """Keep validation sums exact even beyond SQLite's signed 64-bit range."""

    def __init__(self):
        self.value = 0

    def step(self, value):
        self.value += value

    def finalize(self):
        # SQLite cannot return a Python int larger than int64, but text is exact.
        return str(self.value)


def empty_cohort():
    return dict.fromkeys(("completed", "top_cross_completed", "internal_cross_completed",
                          "union_cross_completed", "base_gas", "latency_sum_ns",
                          "gas_latency_sum_ns", "injected", "injected_base_gas"), 0)


def add_completion(cohort, base, latency, top, internal, union):
    cohort["completed"] += 1
    cohort["top_cross_completed"] += top
    cohort["internal_cross_completed"] += internal
    cohort["union_cross_completed"] += union
    cohort["base_gas"] += base
    cohort["latency_sum_ns"] += latency
    # Multiply in Python, not SQL: base*latency can exceed int64 in ordinary runs.
    cohort["gas_latency_sum_ns"] += base * latency


def cohort_metrics(cohort, duration_ns):
    result = dict(cohort)
    count, base = cohort["completed"], cohort["base_gas"]
    duration = seconds(duration_ns) if duration_ns is not None else None
    result.update({
        "tps": ratio(count, duration),
        "base_gas_per_second": ratio(base, duration),
        "mean_latency_seconds": ratio(cohort["latency_sum_ns"], count * NS),
        "gas_weighted_mean_latency_seconds": ratio(cohort["gas_latency_sum_ns"], base * NS),
        "mean_base_gas": ratio(base, count),
        "top_cross_ratio": ratio(cohort["top_cross_completed"], count),
        "internal_cross_ratio": ratio(cohort["internal_cross_completed"], count),
        "union_cross_ratio": ratio(cohort["union_cross_completed"], count),
    })
    return result


def aggregate_cohorts(db, start_ns, end_ns, window_ns):
    """One streamed join; memory depends on bins, never transaction count."""
    full, selected, before = empty_cohort(), empty_cohort(), empty_cohort()
    bins = defaultdict(empty_cohort)
    for base, sent, finished, top, internal, union in db.execute("""
        SELECT i.base,i.elapsed,s.elapsed,s.top,s.internal,s.union_cross
        FROM injected i LEFT JOIN stages s
          ON s.source=i.source AND s.stage IN ('LOCAL','RELAY2')
    """):
        full["injected"] += 1
        full["injected_base_gas"] += base
        if sent < start_ns:
            before["injected"] += 1
            before["injected_base_gas"] += base
        elif sent < end_ns:
            selected["injected"] += 1
            selected["injected_base_gas"] += base
            bucket = bins[(sent - start_ns) // window_ns]
            bucket["injected"] += 1
            bucket["injected_base_gas"] += base
        if finished is None:
            continue
        latency = finished - sent
        add_completion(full, base, latency, top, internal, union)
        if finished < start_ns:
            add_completion(before, base, latency, top, internal, union)
        elif finished < end_ns:
            add_completion(selected, base, latency, top, internal, union)
            bucket = bins[(finished - start_ns) // window_ns]
            add_completion(bucket, base, latency, top, internal, union)
    return full, selected, before, bins


def flow_metrics(cohort, before):
    """Outstanding means injected before the cutoff but not completed before it."""
    initial = before["injected"] - before["completed"]
    initial_gas = before["injected_base_gas"] - before["base_gas"]
    return {
        "injected_before_end": before["injected"] + cohort["injected"],
        "completed_before_end": before["completed"] + cohort["completed"],
        "outstanding_at_start": initial,
        "outstanding_at_end": initial + cohort["injected"] - cohort["completed"],
        "outstanding_base_gas_at_start": initial_gas,
        "outstanding_base_gas_at_end": initial_gas + cohort["injected_base_gas"] - cohort["base_gas"],
    }


def initialize(db):
    db.create_aggregate("exact_sum", 1, ExactSum)
    db.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=FILE;
        PRAGMA cache_size=-32768;
        CREATE TABLE injected (
            source TEXT PRIMARY KEY, base INTEGER NOT NULL,
            elapsed INTEGER NOT NULL, observed INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TABLE blocks (
            shard INTEGER NOT NULL, height INTEGER NOT NULL, hash TEXT NOT NULL,
            length INTEGER NOT NULL, elapsed INTEGER NOT NULL, observed INTEGER NOT NULL,
            PRIMARY KEY (shard,height,hash)
        ) WITHOUT ROWID;
        CREATE TABLE stages (
            source TEXT NOT NULL, stage TEXT NOT NULL,
            shard INTEGER NOT NULL, height INTEGER NOT NULL, hash TEXT NOT NULL,
            length INTEGER NOT NULL, base INTEGER NOT NULL, gas INTEGER NOT NULL,
            calls INTEGER NOT NULL, attached INTEGER NOT NULL,
            top INTEGER NOT NULL, internal INTEGER NOT NULL, union_cross INTEGER NOT NULL,
            elapsed INTEGER NOT NULL, observed INTEGER NOT NULL, worker INTEGER NOT NULL,
            PRIMARY KEY (source,stage)
        ) WITHOUT ROWID;
    """)


def ingest(db, csv_path, meta, issues, *, exclude_cutoff=False):
    raw = Counter()
    replays = Counter()
    cfg = meta["config"]
    shard_count = integer(cfg["shard_count"], "config.shard_count", 1)
    gas_limit = integer(cfg["block_gas_limit"], "config.block_gas_limit", 1)
    call_fee = integer(cfg["cross_call_gas"], "config.cross_call_gas")
    covered = integer(meta["observed_duration_ns"], "observed_duration_ns")
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, strict=True)
        if reader.fieldnames != COLUMNS:
            raise InvalidRun(f"events.csv header must exactly equal {COLUMNS}")
        for line, row in enumerate(reader, 2):
            event = row.get("event")
            raw[event] += 1
            try:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError("wrong number of CSV fields (possibly truncated)")
                if event not in ("inject", "block", "stage"):
                    raise ValueError(f"unknown event {event!r}")
                elapsed = integer(row["elapsed_ns"], "elapsed_ns")
                observed = integer(row["observed_unix_ns"], "observed_unix_ns", 1)
                if elapsed > covered:
                    raise ValueError("event elapsed time exceeds run.json observation duration")
                if exclude_cutoff and elapsed == covered:
                    raise ValueError("event falls at the fixed E2 cutoff")
                source = row["source_tx_hash"]
                stage = row["stage"]
                if event == "inject":
                    if not source or stage != "TOP":
                        raise ValueError("inject needs source_tx_hash and stage=TOP")
                    base = integer(row["base_gas"], "base_gas", 1)
                    if base > gas_limit:
                        raise ValueError("injected base Gas exceeds block Gas limit")
                    previous = db.execute("SELECT base FROM injected WHERE source=?", (source,)).fetchone()
                    if previous:
                        replays["inject"] += 1
                        if previous[0] != base:
                            raise ValueError(f"conflicting injected base Gas for {source}")
                        db.execute("UPDATE injected SET elapsed=min(elapsed,?),observed=min(observed,?) WHERE source=?", (elapsed, observed, source))
                    else:
                        db.execute("INSERT INTO injected VALUES (?,?,?,?)", (source, base, elapsed, observed))
                else:
                    shard = integer(row["shard_id"], "shard_id")
                    height = integer(row["block_height"], "block_height")
                    block_hash = row["block_hash"]
                    length = integer(row["block_body_length"], "block_body_length")
                    if shard >= shard_count or not block_hash:
                        raise ValueError("invalid shard ID or empty block hash")
                    identity = (shard, height, block_hash)
                    if event == "block":
                        if source or stage:
                            raise ValueError("block event must have empty source/stage")
                        previous = db.execute("SELECT length FROM blocks WHERE shard=? AND height=? AND hash=?", identity).fetchone()
                        if previous:
                            replays["block"] += 1
                            if previous[0] != length:
                                raise ValueError(f"conflicting block body length for {identity}")
                            db.execute("UPDATE blocks SET elapsed=min(elapsed,?),observed=min(observed,?) WHERE shard=? AND height=? AND hash=?", (elapsed, observed) + identity)
                        else:
                            db.execute("INSERT INTO blocks VALUES (?,?,?,?,?,?)", identity + (length, elapsed, observed))
                    else:
                        if not source or stage not in ("LOCAL", "RELAY1", "RELAY2"):
                            raise ValueError("stage event needs source and LOCAL/RELAY1/RELAY2")
                        base = integer(row["base_gas"], "base_gas", 1)
                        gas = integer(row["stage_gas"], "stage_gas", 1)
                        calls = integer(row["cross_call_count"], "cross_call_count")
                        attached = integer(row["attached_gas"], "attached_gas")
                        top = boolean(row["top_cross"], "top_cross")
                        internal = boolean(row["internal_cross"], "internal_cross")
                        union = boolean(row["union_cross"], "union_cross")
                        worker = integer(row["worker_commit_unix_ns"], "worker_commit_unix_ns", 1)
                        expected_attached = calls * call_fee if stage != "RELAY1" else 0
                        if gas != base + expected_attached or attached != expected_attached or gas > gas_limit:
                            raise ValueError("stage Gas violates immutable-base + attached-call billing or capacity")
                        # RELAY1 may expose potential internal calls. Completion flags are
                        # evaluated only at LOCAL/RELAY2, under their actual execution map.
                        if stage != "RELAY1" and (top != int(stage == "RELAY2") or internal != int(calls > 0) or union != int(bool(top or internal))):
                            raise ValueError("final-stage cross flags disagree with stage/call count")
                        semantic = identity + (length, base, gas, calls, attached, top, internal, union)
                        previous = db.execute("SELECT shard,height,hash,length,base,gas,calls,attached,top,internal,union_cross FROM stages WHERE source=? AND stage=?", (source, stage)).fetchone()
                        if previous:
                            replays["stage"] += 1
                            if previous[:3] != identity:
                                raise ValueError(f"duplicate execution: {source}/{stage} appears in different blocks")
                            if previous != semantic:
                                raise ValueError(f"conflicting replay semantics for {source}/{stage}")
                            db.execute("UPDATE stages SET elapsed=min(elapsed,?),observed=min(observed,?),worker=min(worker,?) WHERE source=? AND stage=?", (elapsed, observed, worker, source, stage))
                        else:
                            db.execute("INSERT INTO stages VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (source, stage) + semantic + (elapsed, observed, worker))
            except (ValueError, sqlite3.Error) as exc:
                issues.add("invalid_event", f"events.csv:{line}: {exc}")
            if line % 10_000 == 0:
                db.commit()
    db.commit()
    db.execute("CREATE INDEX stages_block ON stages(shard,height,hash)")
    for event in ("inject", "block", "stage"):
        expected_raw = integer(meta[f"{event if event != 'inject' else 'injection'}_rows"], f"{event} raw row count")
        if raw[event] != expected_raw:
            issues.add("raw_count_mismatch", f"{event}: CSV {raw[event]} != run.json {expected_raw}; possible truncation")
    return dict(raw), dict(replays)


def query_issues(db, issues, *, require_all_completed=True):
    queries = [
        ("unknown_source", "SELECT s.source,s.stage FROM stages s LEFT JOIN injected i ON i.source=s.source WHERE i.source IS NULL", False),
        ("base_mismatch", "SELECT s.source,s.stage FROM stages s JOIN injected i ON i.source=s.source WHERE s.base!=i.base", False),
        ("before_injection", "SELECT s.source,s.stage FROM stages s JOIN injected i ON i.source=s.source WHERE s.elapsed<i.elapsed", False),
        ("unlisted_block", "SELECT s.source,s.stage FROM stages s LEFT JOIN blocks b USING(shard,height,hash) WHERE b.hash IS NULL", False),
        ("stage_body_length_mismatch", "SELECT s.source,s.stage FROM stages s JOIN blocks b USING(shard,height,hash) WHERE s.length!=b.length", False),
        ("stage_block_receipt_mismatch", "SELECT s.source,s.stage FROM stages s JOIN blocks b USING(shard,height,hash) WHERE s.elapsed!=b.elapsed OR s.observed!=b.observed", False),
        ("forked_block_height", "SELECT shard,height FROM blocks GROUP BY shard,height HAVING count(*)>1", False),
        ("mixed_local_and_relay", "SELECT source FROM stages GROUP BY source HAVING sum(stage='LOCAL')>0 AND sum(stage!='LOCAL')>0", False),
        ("missing_relay1", "SELECT s.source FROM stages s LEFT JOIN stages r ON r.source=s.source AND r.stage='RELAY1' WHERE s.stage='RELAY2' AND r.source IS NULL", True),
        ("unfinished_transaction", "SELECT i.source FROM injected i LEFT JOIN stages s ON s.source=i.source AND s.stage IN ('LOCAL','RELAY2') WHERE s.source IS NULL", True),
        ("block_body_incomplete", "SELECT b.shard,b.height,b.hash,b.length,count(s.source) FROM blocks b LEFT JOIN stages s USING(shard,height,hash) GROUP BY b.shard,b.height,b.hash HAVING b.length>count(s.source)", True),
        ("block_body_extra_stages", "SELECT b.shard,b.height,b.hash,b.length,count(s.source) FROM blocks b LEFT JOIN stages s USING(shard,height,hash) GROUP BY b.shard,b.height,b.hash HAVING b.length<count(s.source)", False),
    ]
    for code, sql, incomplete in queries:
        if code == "unfinished_transaction" and not require_all_completed:
            continue  # E2 verifies every sampled source separately; other sources can remain in flight.
        for record in db.execute(sql):
            issues.add(code, repr(record), incomplete=incomplete)


def check_block_gas(db, gas_limit, issues):
    for record in db.execute("SELECT shard,height,hash,exact_sum(gas) FROM stages GROUP BY shard,height,hash"):
        if int(record[3]) > gas_limit:
            issues.add("block_gas_overflow", repr(record))


def summarize(run_dir, output_dir, window_ns, duration_ns, start_ns=0, allow_incomplete=False):
    run_dir, output_dir = Path(run_dir).resolve(), Path(output_dir).resolve()
    if output_dir == run_dir or (output_dir.exists() and (not output_dir.is_dir() or any(output_dir.iterdir()))):
        raise InvalidRun("output-dir must be a new or empty directory, distinct from run-dir")
    output_dir.mkdir(parents=True, exist_ok=True)
    issues = Issues()
    db_path = output_dir / ".dedup.sqlite3"
    db = None
    try:
        meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
        if meta.get("schema_version") != 1 or meta.get("clock") != "supervisor_receipt_monotonic":
            raise InvalidRun("unsupported schema_version or observation clock")
        if meta.get("completion_scope", "full") != "full":
            raise InvalidRun("full-run analysis requires full completion scope; use summarize_e2.py for E2")
        if meta.get("status") not in ("closed", "recording"):
            raise InvalidRun("invalid run status")
        if meta["status"] != "closed":
            issues.add("not_closed", "run.json does not confirm a successfully closed logger", incomplete=True)
        covered = integer(meta["observed_duration_ns"], "observed_duration_ns")
        integer(meta["first_injection_unix_ns"], "first_injection_unix_ns", 1)
        cfg = meta["config"]
        expected = integer(cfg["expected_transactions"], "expected_transactions", 1)
        requested_end = start_ns + duration_ns
        if covered < requested_end:
            issues.add("insufficient_coverage", f"observed {seconds(covered)} s, requested endpoint {seconds(requested_end)} s", incomplete=True)
        if covered <= start_ns:
            raise InvalidRun("no observed coverage in the requested interval")
        end_ns = min(requested_end, covered)
        db = sqlite3.connect(db_path)
        initialize(db)
        raw, replays = ingest(db, run_dir / "events.csv", meta, issues)
        query_issues(db, issues)
        gas_limit = integer(cfg["block_gas_limit"], "block_gas_limit", 1)
        check_block_gas(db, gas_limit, issues)
        dispatched, last_dispatch, first_dispatch = db.execute("SELECT count(*),max(elapsed),min(elapsed) FROM injected").fetchone()
        completed, last_completion = db.execute("SELECT count(*),max(elapsed) FROM stages WHERE stage IN ('LOCAL','RELAY2')").fetchone()
        if first_dispatch != 0:
            issues.add("invalid_origin", f"first unique injection must have elapsed_ns=0, found {first_dispatch}")
        if dispatched != expected:
            issues.add("injected_count_mismatch", f"unique injected {dispatched} != configured {expected}", incomplete=True)
        if completed != dispatched:
            issues.add("completed_count_mismatch", f"unique completed {completed} != unique injected {dispatched}", incomplete=True)
        report = issues.report()
        report.update({"schema_version": 1, "run_dir": str(run_dir), "raw_rows": raw, "duplicate_replay_rows": replays, "allow_incomplete": allow_incomplete})
        write_json(output_dir / "validation.json", report)
        if report["integrity_issue_count"] or (report["incomplete_issue_count"] and not allow_incomplete):
            raise InvalidRun(f"run validation failed; see {output_dir / 'validation.json'}")

        full, selected, before, bins = aggregate_cohorts(db, start_ns, end_ns, window_ns)
        running = dict(before)
        with (output_dir / "windows.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=WINDOW_COLUMNS)
            writer.writeheader()
            for index, begin in enumerate(range(start_ns, end_ns, window_ns)):
                finish = min(begin + window_ns, end_ns)
                cohort = bins.get(index, empty_cohort())
                row = {"start_seconds": seconds(begin), "end_seconds": seconds(finish), "duration_seconds": seconds(finish - begin)}
                row.update(cohort_metrics(cohort, finish - begin))
                row.update(flow_metrics(cohort, running))
                writer.writerow({key: "NA" if value is None else value for key, value in row.items()})
                for key, value in cohort.items():
                    running[key] += value
        complete = report["valid_for_publication"]
        primary = cohort_metrics(selected, end_ns - start_ns)
        primary.update(flow_metrics(selected, before))
        full_run = cohort_metrics(full, last_completion)
        full_run.update({
            "complete": complete, "duration_seconds": seconds(last_completion) if last_completion is not None else None,
            "expected_transactions": expected, "dispatched_unique": dispatched, "completed_unique": completed,
            "outstanding": dispatched - completed, "outstanding_base_gas": full["injected_base_gas"] - full["base_gas"],
            "last_dispatch_seconds": seconds(last_dispatch) if last_dispatch is not None else None,
            "last_completion_seconds": seconds(last_completion) if last_completion is not None else None,
            "drain_seconds": max(0, seconds(last_completion - last_dispatch)) if complete and last_dispatch is not None and last_completion is not None else None,
        })
        summary = {
            "schema_version": 1, "valid_for_publication": complete,
            "clock": meta["clock"], "completion_timestamp": "earliest supervisor receipt of LOCAL or RELAY2 (not worker commit time)",
            "latency_definition": "earliest final supervisor receipt minus earliest actual injection; excludes pre-injection waiting",
            "time_origin": "first actual injection on the supervisor monotonic clock",
            "config": cfg, "run_dir": str(run_dir),
            "observation": {"start_seconds": seconds(start_ns), "requested_duration_seconds": seconds(duration_ns), "analyzed_duration_seconds": seconds(end_ns - start_ns), "end_seconds": seconds(end_ns), "actual_covered_seconds": seconds(covered), "window_seconds": seconds(window_ns), "interval": "[start,end)", "includes_reconfiguration_pauses": True},
            "primary": primary,
            "full_run": full_run,
            "duplicate_replay_rows": replays,
            "ratios_are_fractions": True,
            "notes": ["Multiply ratios by 100 only for percent axes.", "Use aggregate numerators/denominators; never average window ratios or mean latencies.", "A RELAY1 event is execution work, not a completed logical transaction.", "All four interval metrics use the same final-receipt cohort; later completions are excluded.", "Goodput and latency weights use immutable base Gas once per source, excluding attached/relay overhead.", "Full-run duration ends at the last final receipt, including that completion; logger close time is only observation coverage.", "Outstanding at an interval endpoint includes earlier injections and excludes completions strictly before that endpoint.", "Diagnostic incomplete output is not publication-valid."],
        }
        write_json(output_dir / "summary.json", summary)
        return summary
    except (OSError, ValueError, KeyError, csv.Error, sqlite3.Error, InvalidRun) as exc:
        # A late write/SQLite failure must not leave an apparently valid report.
        # Keep the detailed validation report when validation itself rejected it.
        if not isinstance(exc, InvalidRun) or not (output_dir / "validation.json").exists():
            issues.add("fatal_processing_error", str(exc))
            try:
                write_json(output_dir / "validation.json", issues.report())
            except OSError:
                pass  # e.g. full disk; the command still exits unsuccessfully.
        for name in ("summary.json", "windows.csv"):
            (output_dir / name).unlink(missing_ok=True)
        raise InvalidRun(str(exc)) from exc
    finally:
        if db is not None:
            db.close()
        db_path.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path, help="Directory containing run.json and events.csv")
    parser.add_argument("--output-dir", required=True, type=Path, help="New/empty output directory; raw logs are never modified")
    parser.add_argument("--window-seconds", default="60")
    parser.add_argument("--duration-seconds", required=True, help="Fixed observation length, including pauses")
    parser.add_argument("--start-seconds", default="0", help="Optional common warmup exclusion; default is no exclusion")
    parser.add_argument("--allow-incomplete", action="store_true", help="Diagnostic output only; does not waive integrity errors")
    args = parser.parse_args(argv)
    try:
        result = summarize(args.run_dir, args.output_dir, seconds_ns(args.window_seconds, "window-seconds", True), seconds_ns(args.duration_seconds, "duration-seconds", True), seconds_ns(args.start_seconds, "start-seconds"), args.allow_incomplete)
    except (InvalidRun, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"Wrote {args.output_dir / 'windows.csv'} and summary.json; valid_for_publication={result['valid_for_publication']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
