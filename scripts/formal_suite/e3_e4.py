#!/usr/bin/env python3
"""Offline E3 and E4 tables from one completed full-scope run. Starts no workers."""
import argparse
import csv
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from unified_metrics import summarize as ledger

QUEUE = "queue_curve.csv"
DRAIN = "drain_summary.csv"
SHARDS = "shard_workloads.csv"
SYSTEM = "system_workload.csv"


def required(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for buf in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            h.update(buf)
    return h.hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_pools(paths, t0_ns):
    # PBFT migration proposals create height gaps. A normal local block is one sample.
    events, samples = [], {}
    for shard, path in enumerate(paths):
        last_height, last_time = -1, -1
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle, strict=True)
            fields = {"Block Height", "TimeStamp - Commit (unixMill)", "TxPool Size", "# of all Txs in this block"}
            required(reader.fieldnames and fields.issubset(reader.fieldnames), f"missing physical pool fields in {path}")
            for row in reader:
                height = ledger.integer(row["Block Height"], "block height")
                stamp = ledger.integer(row["TimeStamp - Commit (unixMill)"], "local commit", 1)
                queue = ledger.integer(row["TxPool Size"], "local queue")
                length = ledger.integer(row["# of all Txs in this block"], "block body length")
                required(height > last_height and stamp >= last_time,
                         f"nonmonotonic heights or commit times in {path} at {height}")
                last_height, last_time = height, stamp
                samples[shard, height] = length
                events.append((stamp * 1_000_000 - t0_ns, shard, height, queue))
        required(last_height >= 0, f"no pool samples for shard {shard}")
    return events, samples


def pool_curve(events, n_shards, last_send, last_final):
    # Preserve unknown shards until first observation. A block at the same
    # timestamp as another updates the estimate before it is emitted.
    timeline = {}
    for elapsed, shard, height, queue in sorted(events):
        timeline.setdefault(max(0, elapsed), {})[shard] = queue
    for t in (0, last_send, last_final):
        timeline.setdefault(t, {})
    latest, rows, peak = {}, [], None
    for t in sorted(timeline):
        latest.update(timeline[t])
        total = sum(latest.values()) if len(latest) == n_shards else None
        if total is not None and t <= last_final:
            peak = total if peak is None else max(peak, total)
        rows.append(dict(elapsed_seconds=t / ledger.NS, sampled_pool_size=total, observed_shards=len(latest),
                         reporting_shards=len(timeline[t]), last_injection=int(t == last_send),
                         last_confirmation=int(t == last_final)))
    required(peak is not None, "no all-shard pool estimate during the completed transaction run")
    return rows, peak


def source_digest(db):
    # Cross-method evidence independent of dispatch order, shard and stage.
    h = hashlib.sha256()
    for (source,) in db.execute("SELECT source FROM injected ORDER BY source"):
        h.update(source.encode("ascii") + b"\n")
    return h.hexdigest()


def gas_tables(db, n_shards, count, base_gas):
    by_shard = {}
    sql = """
        SELECT shard, count(*), exact_sum(gas), exact_sum(attached),
               exact_sum(CASE WHEN stage='RELAY1' THEN base ELSE 0 END)
        FROM stages GROUP BY shard
    """
    for shard, stages, gas, attached, relay in db.execute(sql):
        by_shard[shard] = stages, int(gas), int(attached), int(relay)
    total = sum(values[1] for values in by_shard.values())
    extra = total - base_gas
    # Cost conserved: RELAY1 is the duplicate physical base charge of a
    # cross-shard source; attached Gas is already contained in stage_gas.
    required(extra == sum(attached + relay for _, _, attached, relay in by_shard.values()),
             "extra Gas differs from repeated relay base plus attached Gas")
    rows = []
    for shard in range(n_shards):
        stages, gas, attached, relay = by_shard.get(shard, (0, 0, 0, 0))
        rows.append(dict(shard_id=shard, physical_stages=stages, execution_gas=gas,
                         attached_gas=attached, relay1_base_gas=relay, share=gas/total if total else None))
    maximum = max(row["execution_gas"] for row in rows)
    system = dict(completed=count, base_gas=base_gas, total_execution_gas=total, extra_execution_gas=extra,
                  extra_gas_per_completed=extra/count, mean_shard_gas=total/n_shards, max_shard_gas=maximum,
                  max_to_mean_shard_gas=maximum*n_shards/total if total else None)
    return rows, system


def write_tables(out, tables, evidence):
    # Publish complete files without replacing historical or conflicting data.
    # Repeating a command after interruption fills only missing matching files.
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".publishing-", dir=out) as folder:
        stage = Path(folder)
        for name, rows in tables.items():
            with (stage/name).open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
        record = {**evidence, "outputs_sha256": {name:digest(stage/name) for name in tables}}
        ledger.write_json(stage/"validation.json", record)
        for name in (*tables, "validation.json"):
            try:
                os.link(stage/name, out/name)
            except FileExistsError:
                required(digest(stage/name) == digest(out/name), f"existing output differs: {out/name}")


def cached(out, fingerprint, names):
    proof = out/"validation.json"
    if not proof.is_file():
        return False
    v = read_json(proof)
    required(v.get("input_fingerprint") == fingerprint, f"existing analysis is for different inputs: {out}")
    hashes = v.get("outputs_sha256", {})
    required(v.get("valid") is True and set(hashes) == set(names), f"incomplete analysis record: {out}")
    for name in names:
        required((out/name).is_file() and digest(out/name) == hashes[name], f"table missing or changed: {out/name}")
    return True


def analyze(run, e3_out, e4_out):
    run, e3, e4 = (Path(p).resolve() for p in (run, e3_out, e4_out))
    required(e3 != e4 and e3 not in e4.parents and e4 not in e3.parents,
             "output directories must be separate, non-nested E3/E4 directories")
    for out in (e3, e4):
        required(out != run and out not in run.parents and run not in out.parents, "output must be outside the raw run")
    raw = run/"result/unified_metrics"
    state = read_json(run/"run_state.json")
    meta = read_json(raw/"run.json")
    summary = read_json(run/"analysis/summary.json")
    validation = read_json(run/"analysis/validation.json")
    required(state.get("status") == "completed" and meta.get("completion_scope","full") == "full"
             and meta.get("status") == "closed" and meta.get("clock") == "supervisor_receipt_monotonic"
             and validation.get("valid_for_publication") is True and summary.get("valid_for_publication") is True
             and summary["full_run"].get("complete") is True, "a validated completed full run is required")
    count = ledger.integer(meta["config"]["expected_transactions"], "expected transactions", 1)
    n_shards = ledger.integer(meta["config"]["shard_count"], "shard count", 1)
    elapsed = ledger.integer(meta["observed_duration_ns"], "duration", 1)
    pools = [run/f"result/pbft_shardNum={n_shards}/Shard{k}{n_shards}.csv" for k in range(n_shards)]
    known = state["artifacts_sha256"]
    inputs = {}
    for path in [raw/"run.json", raw/"events.csv", run/"analysis/summary.json", run/"analysis/validation.json", *pools]:
        key = path.relative_to(run).as_posix()
        value = digest(path)
        if path not in pools or key in known:
            required(known.get(key) == value, f"run artifact missing from fingerprint or modified: {path}")
        inputs[key] = value
    inputs["analysis_code"] = digest(Path(__file__))
    inputs["ledger_code"] = digest(Path(ledger.__file__))
    fingerprint = hashlib.sha256(json.dumps(inputs,sort_keys=True).encode("utf-8")).hexdigest()
    e3_ok = cached(e3, fingerprint, [QUEUE,DRAIN])
    e4_ok = cached(e4, fingerprint, [SHARDS,SYSTEM])
    if e3_ok and e4_ok:
        return
    full = summary["full_run"]
    with tempfile.TemporaryDirectory(prefix="e3-e4-ledger-") as folder:
        with closing(sqlite3.connect(Path(folder)/"stages.sqlite3")) as db:
            ledger.initialize(db)
            issues = ledger.Issues()
            ledger.ingest(db, raw/"events.csv", meta, issues)
            # The verified run_state binds these exact logs to the upstream full
            # publication checks. Reuse its validated, idempotent row rules.
            required(not issues.counts, f"inconsistent raw events: {issues.counts}")
            injected, base, first, last_send = db.execute("SELECT count(*),exact_sum(base),min(elapsed),max(elapsed) FROM injected").fetchone()
            final_count, last_final = db.execute("SELECT count(*),max(elapsed) FROM stages WHERE stage IN ('LOCAL','RELAY2')").fetchone()
            required(injected == final_count == count == full["dispatched_unique"] == full["completed_unique"]
                     and full["outstanding"] == 0 and first == 0 and last_send <= last_final <= elapsed,
                     "full-run completion counts or times mismatch")
            base = int(base)
            required(base == full["base_gas"] and last_send/ledger.NS == full["last_dispatch_seconds"]
                     and last_final/ledger.NS == full["last_completion_seconds"], "full summary differs from raw events")
            common = dict(run_id=state["run_id"], method=meta["config"]["method"], shards=n_shards,
                          injection_rate=meta["config"]["inject_speed"], completed_sources_sha256=source_digest(db))
            pool_events, samples = read_pools(pools, meta["first_injection_unix_ns"])
            for sid, height, length in db.execute("SELECT shard,height,length FROM blocks"):
                required(samples.get((sid,height)) == length,
                         f"per-shard CSV lacks matching block height/body: shard={sid} height={height}")
            curve, peak = pool_curve(pool_events, n_shards, last_send, last_final)
            drain = dict(**common, completed=count, last_injection_seconds=last_send/ledger.NS,
                         last_confirmation_seconds=last_final/ledger.NS, drain_seconds=(last_final-last_send)/ledger.NS,
                         peak_sampled_pool_size=peak)
            shard_rows, system = gas_tables(db, n_shards, count, base)
    evidence = dict(valid=True, **common, input_fingerprint=fingerprint, inputs_sha256=inputs)
    if not e3_ok:
        write_tables(e3, {QUEUE: curve, DRAIN: [drain]}, evidence)
    if not e4_ok:
        write_tables(e4, {SHARDS: shard_rows, SYSTEM: [{**common, **system}]}, evidence)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--e3-out", required=True, type=Path)
    parser.add_argument("--e4-out", required=True, type=Path)
    args = parser.parse_args()
    try:
        analyze(args.run, args.e3_out, args.e4_out)
    except (OSError, KeyError, ValueError, TypeError, sqlite3.Error, ledger.InvalidRun) as exc:
        parser.exit(2, f"E3/E4 analysis failed: {exc}\n")
    print("E3/E4 validated; queue, drain, per-shard and system Gas tables ready.")


if __name__ == "__main__":
    main()
