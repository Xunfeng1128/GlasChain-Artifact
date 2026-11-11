#!/usr/bin/env python3
"""E1: observed cross-shard outcomes and per-epoch block-span throughput of a validated full run."""
import argparse
import csv
import hashlib
import json
import math
import os
import re
import sys
import tempfile
from pathlib import Path

METHODS = {1: "CLPA", 8: "TxAllo_Contrib", 9: "P-Louvain_Contrib", 10: "GARET-FS", 12: "GLAS_D"}
STAGES = {"inject": 1, "LOCAL": 2, "RELAY1": 4, "RELAY2": 8}
AVERAGE = "result/supervisor_measureOutput/Average_TPS.csv"


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def required_json(run, name, artifacts):
    path = run / name
    if sha(path) != artifacts[name]:
        raise ValueError(f"run artifact fingerprint mismatch: {name}")
    return json.loads(path.read_text())


def epochs(run):
    output = {}
    with (run / AVERAGE).open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            e = int(row["EpochID"])
            local, first, second = (int(row[k]) for k in ("Normal tx # in this epoch",
                "Relay1 tx # in this epoch", "Relay2 tx # in this epoch"))
            count = local + (first + second) / 2
            if e != len(output) or min(local, first, second) < 0 or count != float(row["Total tx # in this epoch"]):
                raise ValueError(f"invalid or duplicated epoch row {e} in {AVERAGE}")
            start, end = int(row["Epoch start time"]), int(row["Epoch end time"])
            rate = float(row["Avg. TPS of this epoch"])
            if not count:
                duration = rate = None
                status = "no_nonempty_block"
            elif math.isinf(rate) and rate > 0 and start == end:
                duration = rate = None
                status = "positive_count_zero_span"
            elif math.isfinite(rate) and rate > 0 and end >= start:
                # The Go rate uses the untruncated span; the two CSV timestamps only retain ms.
                duration = count / rate
                if abs(duration - (end-start)/1000) > .00101:
                    raise ValueError(f"epoch {e}: rate inconsistent with block-span timestamps")
                status = "measured"
            else:
                raise ValueError(f"invalid epoch throughput or reversed time at epoch {e}")
            output[e] = dict(epoch=e, local=local, relay1=first, relay2=second,
                             adjusted_stage_count=count, start_unix_ms=start, end_unix_ms=end,
                             nonempty_block_span_seconds=duration, epoch_stage_tps=rate, status=status)
    if not output:
        raise ValueError("empty Average_TPS.csv")
    return output


def epochs_by_block(run, shards):
    lookup = {}
    for sid in range(shards):
        path = run / f"result/pbft_shardNum={shards}/Shard{sid}{shards}.csv"
        with path.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                key = (sid, int(row["Block Height"]))
                e = int(row["EpochID of this block"])
                if e < 0 or key in lookup:
                    raise ValueError(f"invalid or repeated block in {path}: {key}")
                lookup[key] = e
    return lookup


def outcomes(run, shards, epoch_rows, artifacts):
    source_flags = {}
    counts = {e:dict(local=0,relay1=0,relay2=0,top=0,union=0) for e in epoch_rows}
    gas = [0] * shards
    block_epochs = epochs_by_block(run, shards)
    relative = "result/unified_metrics/events.csv"
    # Hash the same raw rows used for analysis. Completed run_state binds these
    # bytes to the strict validator that has already checked conflicting replays.
    digest = hashlib.sha256()
    with (run / relative).open("rb") as raw:
        def lines():
            for line in raw:
                digest.update(line)
                yield line.decode("utf-8")
        reader = csv.DictReader(lines())
        for row in reader:
            if row["event"] == "block":
                continue
            stage = row["stage"] if row["event"] == "stage" else row["event"]
            flag = STAGES[stage]
            h = row["source_tx_hash"]
            if len(h) != 66 or not h.startswith("0x"):
                raise ValueError("invalid source transaction hash")
            h = bytes.fromhex(h[2:])
            old = source_flags.get(h, 0)
            if old & flag:
                continue
            source_flags[h] = old | flag
            if stage == "inject":
                continue
            sid = int(row["shard_id"])
            key = (sid, int(row["block_height"]))
            if key not in block_epochs or block_epochs[key] not in counts:
                raise ValueError(f"cannot associate stage block {key} with a measured epoch")
            c = counts[block_epochs[key]]
            c[stage.lower()] += 1
            gas[sid] += int(row["stage_gas"])
            if stage in ("LOCAL","RELAY2"):
                c["top"] += int(row["top_cross"])
                c["union"] += int(row["union_cross"])
    if digest.hexdigest() != artifacts[relative]:
        raise ValueError("raw events fingerprint differs from strict validation input")
    if any(bits not in (3,13) for bits in source_flags.values()):
        raise ValueError("injected cohort and finalized unique source hashes differ")
    identity = hashlib.sha256()
    for source_hash in sorted(source_flags):
        identity.update(source_hash)
    return len(source_flags), identity.hexdigest(), counts, gas


def analyze(run):
    run = Path(run)
    state = json.loads((run/"run_state.json").read_text())
    if state["status"] != "completed":
        raise ValueError("E1 requires a completed runner result")
    arts = state["artifacts_sha256"]
    summary = required_json(run, "analysis/summary.json", arts)
    validation = required_json(run, "analysis/validation.json", arts)
    meta = required_json(run, "result/unified_metrics/run.json", arts)
    full = summary["full_run"]
    n = int(meta["config"]["expected_transactions"])
    if (n <= 0 or meta.get("completion_scope","full") != "full" or meta["status"] != "closed" or
            not validation.get("valid_for_publication") or not summary.get("valid_for_publication") or
            not full.get("complete") or full["outstanding"] != 0 or
            full["dispatched_unique"] != n or full["completed_unique"] != n):
        raise ValueError("E1 requires strict validation and completion of all configured source transactions")
    cfg = meta["config"]
    shards = int(cfg["shard_count"])
    datasets = state["fingerprint"]["data_sha256"]
    tx_digest, calls_digest = datasets["UnifiedTransactionsFile"], datasets["UnifiedCallsFile"]
    if not all(isinstance(v,str) and re.fullmatch("[0-9a-f]{64}",v) for v in (tx_digest,calls_digest)):
        raise ValueError("missing input fingerprints")
    # New formal runs pin the epoch report and the per-shard height/epoch joins.
    # Older strictly validated runs did not archive these hashes; their stage counts
    # are still independently reconciled with the validated events below.
    if "result/e1_checkpoint.json" in arts:
        reports = [AVERAGE] + [f"result/pbft_shardNum={shards}/Shard{sid}{shards}.csv" for sid in range(shards)]
        for name in reports:
            if sha(run / name) != arts[name]:
                raise ValueError(f"epoch report fingerprint mismatch: {name}")
    measured = epochs(run)
    count, identity, stats, gas = outcomes(run, shards, measured, arts)
    if count != n:
        raise ValueError("source cohort size differs from expected input")
    for e,row in measured.items():
        c = stats[e]
        if any(c[k] != row[k] for k in ("local","relay1","relay2")):
            raise ValueError(f"legacy epoch {e} differs from validated unique block stages")
        finals = c["local"]+c["relay2"]
        row.update(unique_final_count=finals, top_cross_completed=c["top"], union_cross_completed=c["union"],
                   top_cross_ratio=c["top"]/finals if finals else None,
                   union_cross_ratio=c["union"]/finals if finals else None)
    top,union = sum(x["top"] for x in stats.values()),sum(x["union"] for x in stats.values())
    if top != full["top_cross_completed"] or union != full["union_cross_completed"]:
        raise ValueError("epoch cross-shard counts differ from validated full run")
    rows = list(measured.values())
    total = sum(x["adjusted_stage_count"]for x in rows)
    if total != n:
        raise ValueError("complete run's stage-adjusted transaction sum differs from input size")
    span = sum(x["nonempty_block_span_seconds"]or 0 for x in rows)
    unavailable = sum(x["status"]=="positive_count_zero_span" for x in rows)
    method = METHODS[int(cfg["consensus_method"])]
    if method == "GLAS_D":
        runtime = required_json(run, "runtime_config.json", arts)
        if (runtime.get("GLASPendingSnapshotPolicy"), runtime.get("GLASPendingExecutionPolicy"),
                runtime.get("GLASPendingSnapshotTiming")) != ("local_window","window_first","after_last_commit"):
            raise ValueError("GLAS_Pending is not the specified D strategy")
    common = dict(run_id=state["run_id"], method=method, shards=shards, injection_rate=cfg["inject_speed"],
                  source_transactions=n)
    system_gas = sum(gas)
    result = dict(**common, cohort_sha256=identity, input_transactions_sha256=tx_digest,
                  input_calls_sha256=calls_digest, measured_epochs=sum(x["status"]=="measured" for x in rows),
                  empty_epochs=sum(x["status"]=="no_nonempty_block" for x in rows),
                  nonempty_zero_span_epochs=unavailable, adjusted_stage_count=total, measured_span_seconds=span,
                  weighted_epoch_stage_tps=total/span if span and not unavailable else None,
                  top_cross_completed=top, top_cross_ratio=top/n, union_cross_completed=union,
                  union_cross_ratio=union/n, runtime_stage_gas_peak_to_mean=max(gas)*shards/system_gas if system_gas else None,
                  evaluation_scope="observed_execution_under_runtime_placements")
    return [dict(**common, **row) for row in rows], [result]


def save(run, out):
    run, out = Path(run).resolve(), Path(out).resolve()
    if run == out or run in out.parents or out in run.parents:
        raise ValueError("E1 analysis must use a separate sibling output directory")
    out.parent.mkdir(parents=True, exist_ok=True)
    data = analyze(run)
    names = ("epoch_tps.csv","allocation_summary.csv")
    with tempfile.TemporaryDirectory(prefix=".e1-", dir=out.parent) as tmp:
        stage = Path(tmp)/"analysis"
        stage.mkdir()
        for name, rows in zip(names,data):
            with (stage/name).open("w",newline="",encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        if out.exists():
            if all((out/name).is_file() and (out/name).read_bytes()==(stage/name).read_bytes() for name in names):
                return
            raise ValueError(f"conflicting existing E1 output: {out}")
        os.rename(stage,out)


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--run",type=Path,required=True)
    cli.add_argument("--out",type=Path,required=True)
    args = cli.parse_args()
    try:
        save(args.run,args.out)
    except (OSError,ValueError,KeyError) as exc:
        cli.exit(2,f"E1 analysis failed: {exc}\n")
