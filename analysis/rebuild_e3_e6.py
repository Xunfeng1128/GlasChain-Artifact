#!/usr/bin/env python3
"""Rebuild the E3–E6 figure source tables from complete 500,000-source runs."""
import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from glas_analysis.batch_stats import gas_tables, logical_backlog, required, source_digest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "scripts"))
from unified_metrics import summarize as ledger

SETTINGS = ((8, 1000), (8, 2000), (16, 1000), (16, 2000))
METHODS = {"glas": 12, "clpa": 1, "plouvain": 9, "hash": 3}
COUNT = 500_000
EVENTS = "result/unified_metrics/events.csv"
RUN = "result/unified_metrics/run.json"
SUMMARY = "analysis/summary.json"
VALIDATION = "analysis/validation.json"
TABLES = {
    "cross": "e3/e3_a_total_cross_shard_ratio/figure_data.csv",
    "composition": "e3/e3_b_cross_shard_composition/figure_data.csv",
    "gas_weighted": "e3/e3_c_gas_weighted_cross_shard_ratio/figure_data.csv",
    "extra": "e3/e3_d_extra_execution_gas/figure_data.csv",
    "backlog": "e4/e4_backlog_drain/figure_data.csv",
    "drain": "e4/e4_backlog_drain/peak_and_drain.csv",
    "shards": "e5/e5_a_shard_gas_distribution/figure_data.csv",
    "maximum": "e5/e5_b_max_shard_execution_gas/figure_data.csv",
    "completion": "e6/e6_shard_completion_time/figure_data.csv",
}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identify_runs(root):
    by_setting = {}
    for folder, children, names in os.walk(root):
        if "run_state.json" not in names:
            continue
        children.clear()
        run = Path(folder)
        state = read_json(run / "run_state.json")
        run_id = state.get("run_id", "")
        if not run_id.startswith("e3_") or state.get("status") != "completed":
            continue
        cfg = read_json(run / RUN)["config"]
        setting = (cfg["shard_count"], cfg["inject_speed"])
        label = run_id.split("_")[1]
        method = "glas" if label in ("glas", "glas13") else label
        if setting not in SETTINGS or method not in METHODS or cfg["expected_transactions"] != COUNT:
            continue
        required(cfg["consensus_method"] == METHODS[method], f"unexpected method ID for {run_id}")
        key = (*setting, method)
        required(key not in by_setting, f"multiple completed results for {key}. Supply a directory with one result per condition.")
        by_setting[key] = (run, state)
    ordered = [(*setting, method) for setting in SETTINGS for method in METHODS]
    required(set(by_setting) == set(ordered), f"missing E3 conditions: {sorted(set(ordered) - set(by_setting))}")
    return [(key, *by_setting[key]) for key in ordered]


def read_metrics(db, key, run_id, summary, source_hashes):
    k, rate, method = key
    common = dict(shards=k, inject_tps=rate, method=method, run_id=run_id)
    count, base, first, last_send = db.execute(
        "SELECT count(*), exact_sum(base), min(elapsed), max(elapsed) FROM injected").fetchone()
    base = int(base)
    final = db.execute("""
        SELECT count(*), max(elapsed), sum(top), sum(internal), sum(union_cross),
               exact_sum(CASE WHEN union_cross THEN base ELSE 0 END)
        FROM stages WHERE stage IN ('LOCAL','RELAY2')
    """).fetchone()
    n_final, last_final, top, internal, union, crossing_gas = final
    full = summary["full_run"]
    required(first == 0 and n_final == count == full["dispatched_unique"] == full["completed_unique"] == COUNT
             and full["outstanding"] == 0 and base == full["base_gas"], f"cohort mismatch for {run_id}")
    required((top, internal, union) == tuple(full[f"{field}_cross_completed"] for field in ("top", "internal", "union")),
             f"cross-shard counters disagree for {run_id}")
    required(last_send / ledger.NS == full["last_dispatch_seconds"]
             and last_final / ledger.NS == full["last_completion_seconds"], f"run endpoints differ for {run_id}")
    logical, peak, peak_ns = logical_backlog(db, count, last_send, last_final)
    by_shard, system = gas_tables(db, k, count, base)
    cohort = source_digest(db)
    source = dict(completed_sources_sha256=cohort, source_summary_sha256=source_hashes[SUMMARY],
                  source_events_sha256=source_hashes[EVENTS])
    completion = []
    for sid, n, last, last_local, n_local in db.execute("""
        SELECT shard, count(*), max(elapsed),
               max(CASE WHEN stage IN ('LOCAL','RELAY2') THEN elapsed END),
               sum(CASE WHEN stage IN ('LOCAL','RELAY2') THEN 1 ELSE 0 END)
        FROM stages GROUP BY shard ORDER BY shard
    """):
        required(n == by_shard[sid]["physical_stages"] and last_local is not None, f"empty shard in {run_id}: {sid}")
        completion.append(dict(**common, shard_id=sid, last_stage_elapsed_ns=last,
                               last_final_elapsed_ns=last_local, physical_stages=n, final_confirmations=n_local))
    required(len(completion) == k, f"missing shard receipt data in {run_id}")
    top_only = union - internal
    counts = dict(**common, independent_runs=1, completed_unique=count, **source)
    crossing = dict(total_cross_unique=union, total_cross_pct=100 * union / count)
    maximum = system["max_shard_gas"]
    return {
        "cross": [dict(**counts, **crossing)],
        "composition": [dict(**counts, top_cross_unique=top, top_only_unique=top_only,
                             call_cross_unique=internal, top_and_call_unique=top-top_only,
                             top_only_pct=100 * top_only / count, call_cross_pct=100 * internal / count, **crossing)],
        "gas_weighted": [dict(**counts, top_cross_unique=top, call_cross_unique=internal, total_cross_unique=union,
                              base_gas=base, cross_base_gas=int(crossing_gas),
                              gas_weighted_cross_pct=100 * int(crossing_gas) / base)],
        "extra": [dict(**common, completed=count, base_gas=base, total_execution_gas=system["total_execution_gas"],
                       relay1_base_gas=sum(r["relay1_base_gas"] for r in by_shard),
                       attached_gas=sum(r["attached_gas"] for r in by_shard),
                       extra_execution_gas=system["extra_execution_gas"])],
        "backlog": [dict(**common, **r) for r in logical],
        "drain": [dict(condition=run_id.removesuffix("_r01"), **common, independent_runs=1,
                       completed=count, peak_logical_backlog=peak, peak_logical_backlog_seconds=peak_ns/ledger.NS,
                       last_dispatch_seconds=last_send/ledger.NS, last_completion_seconds=last_final/ledger.NS,
                       drain_seconds=(last_final-last_send)/ledger.NS, **source)],
        "shards": [dict(**common, **r) for r in by_shard],
        "maximum": [dict(**common, completed=count, max_shard_gas=maximum,
                         max_shard_ids=" ".join(str(r["shard_id"]) for r in by_shard if r["execution_gas"] == maximum))],
        "completion": completion,
    }, (cohort, base)


def measure(key, run, state, scratch):
    expected = state["artifacts_sha256"]
    sha = {rel: digest(run / rel) for rel in (EVENTS, RUN, SUMMARY, VALIDATION)}
    required(all(expected.get(rel) == value for rel, value in sha.items()), f"run artifact checksum differs: {run}")
    meta, summary, validation = (read_json(run / rel) for rel in (RUN, SUMMARY, VALIDATION))
    required(meta["status"] == "closed" and meta.get("completion_scope", "full") == "full"
             and meta["clock"] == "supervisor_receipt_monotonic" and summary["full_run"]["complete"]
             and summary["valid_for_publication"] and validation["valid_for_publication"],
             f"run is not a validated full completion: {run}")
    with TemporaryDirectory(prefix="glaschain-analysis-", dir=scratch) as tmp:
        with closing(sqlite3.connect(Path(tmp) / "ledger.sqlite3")) as db:
            ledger.initialize(db)
            issues = ledger.Issues()
            ledger.ingest(db, run / EVENTS, meta, issues)
            ledger.query_issues(db, issues, require_all_completed=True)
            ledger.check_block_gas(db, meta["config"]["block_gas_limit"], issues)
            required(not issues.counts, f"invalid event ledger: {run}: {issues.counts}")
            return read_metrics(db, key, state["run_id"], summary, sha)


def write_tables(out, tables, cohort):
    for key, relative in TABLES.items():
        target = out / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        rows = tables[key]
        with target.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    for key, filename, fields in (
        ("shards", "validation.json", {}),
        ("completion", "source_validation.json",
         {"independent_runs_per_method_per_setting": 1, "stage_filter": ["LOCAL", "RELAY1", "RELAY2"]}),
    ):
        target = out / TABLES[key]
        record = dict(valid=True, runs_checked=16, shard_rows=len(tables[key]),
                      figure_data_sha256=digest(target), completed_sources_sha256=cohort[0], base_gas=cohort[1], **fields)
        target.with_name(filename).write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True, help="Figure-source directory")
    parser.add_argument("--scratch", type=Path, help="Optional existing directory for temporary SQLite")
    args = parser.parse_args()
    tables = {key: [] for key in TABLES}
    cohort = None
    out = args.out.resolve()
    for key, run, state in identify_runs(args.runs.resolve()):
        required(not out.is_relative_to(run), "output directory must be outside the run")
        rows, identity = measure(key, run, state, args.scratch)
        required(cohort is None or identity == cohort, "E3 runs use different source hashes or base Gas")
        cohort = identity
        for name, values in rows.items():
            tables[name].extend(values)
        print(f"Validated {state['run_id']}", flush=True)
    write_tables(out, tables, cohort)


if __name__ == "__main__":
    main()
