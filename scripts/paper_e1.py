#!/usr/bin/env python3
"""Rebuild the Section VI E1 figure tables from completed E1 runs."""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path

from formal_suite.e1 import AVERAGE, epochs, sha

ROOT = Path(__file__).resolve().parents[1]
PROVENANCE = ROOT / "provenance/formal-runs.json"
EVENTS = "result/unified_metrics/events.csv"
METADATA = "result/unified_metrics/run.json"
RUNTIME = "runtime_config.json"
STATE = "run_state.json"
CHECKPOINT = "result/e1_checkpoint.json"
METHODS = {"glas": 12, "clpa": 1, "plouvain": 9, "txallo": 8, "garet": 10}
CROSSING = ("top_cross", "internal_cross", "union_cross")
FULL_FIELDS = (
    "dispatched_unique", "completed_unique", "outstanding", "base_gas",
    "top_cross_completed", "top_cross_ratio", "internal_cross_completed",
    "internal_cross_ratio", "union_cross_completed", "union_cross_ratio",
    "duration_seconds", "last_dispatch_seconds", "last_completion_seconds", "drain_seconds",
)
NS = 1_000_000_000


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"), parse_float=Decimal)


def verified_json(run, relative, hashes):
    raw = (run / relative).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == hashes[relative], f"checksum mismatch: {run / relative}")
    return json.loads(raw.decode("utf-8-sig"), parse_float=Decimal)


def scan_blocks(path, shards):
    """Hash all rows while extracting earliest receipts for distinct nonempty blocks."""
    digest = hashlib.sha256()
    blocks = {}
    counts = dict(injection_rows=0, stage_rows=0, block_rows=0)
    with path.open("rb") as stream:
        header = stream.readline()
        digest.update(header)
        fields = next(csv.reader([header.decode("utf-8-sig")]))
        columns = [fields.index(x) for x in (
            "elapsed_ns", "shard_id", "block_height", "block_hash", "block_body_length",
        )]
        require(max(columns) < 10, f"unexpected block event schema: {path}")
        carry = b""
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
            data = carry + chunk
            end = data.rfind(b"\n")
            require(end >= 0, f"event record exceeds chunk size: {path}")
            carry = data[end + 1:]
            data = b"\n" + data[:end + 1]
            counts["injection_rows"] += data.count(b"\ninject,")
            counts["stage_rows"] += data.count(b"\nstage,")
            start = data.find(b"\nblock,")
            while start >= 0:
                stop = data.find(b"\n", start + 1)
                parts = data[start + 1:stop].split(b",", 10)
                elapsed, shard, height, block_hash, length = (parts[i] for i in columns)
                receipt, shard_id, stages = int(elapsed), int(shard), int(length)
                require(receipt >= 0 and 0 <= shard_id < shards and stages >= 0,
                        f"invalid block receipt: {path}")
                key = (shard_id, int(height), block_hash)
                previous = blocks.get(key)
                require(previous is None or previous[1] == stages, f"conflicting block replay: {path}")
                if previous is None or receipt < previous[0]:
                    blocks[key] = (receipt, stages)
                counts["block_rows"] += 1
                start = data.find(b"\nblock,", stop)
        require(not carry, f"event ledger has an incomplete last line: {path}")

    per_shard = [{"last": -1, "stages": 0} for _ in range(shards)]
    for (shard_id, _, _), (receipt, stages) in blocks.items():
        if stages:
            row = per_shard[shard_id]
            row["last"] = max(row["last"], receipt)
            row["stages"] += stages
    return digest.hexdigest(), counts, per_shard


def derive(run, record, archive):
    condition = record["condition"]
    state = read_json(run / STATE)
    if archive:
        hashes = record["archived_files_sha256"]
        state = verified_json(run, STATE, hashes)
        analysis = record["analysis_directory"]
        require(state["status"] == record["archived_runner_status"], f"{condition}: archive status differs")
        require(state["status"] == "completed" or analysis == "analysis_retry_20261104_210529",
                f"{condition}: archive run did not complete or recover analysis")
    else:
        require(state["status"] == "completed", f"{condition}: rerun did not complete")
        hashes = state["artifacts_sha256"]
        analysis = "analysis"

    summary_file = f"{analysis}/summary.json"
    runtime = verified_json(run, RUNTIME, hashes)
    meta = verified_json(run, METADATA, hashes)
    summary = verified_json(run, summary_file, hashes)
    validation = verified_json(run, f"{analysis}/validation.json", hashes)
    checkpoint = verified_json(run, CHECKPOINT, hashes)
    settings, fingerprint = meta["config"], state["fingerprint"]
    full = summary["full_run"]

    label = condition.split("_")[1]
    method = "glas" if label == "glas13" else label
    shards, rate, count = (record[key] for key in (
        "shard_count", "inject_tps", "recorded_expected_transactions",
    ))
    pbft = record["pbft_view_change_ms"]
    require((settings["shard_count"], settings["inject_speed"], settings["expected_transactions"],
             settings["consensus_method"]) == (shards, rate, count, METHODS[method])
            and (runtime["InjectSpeed"], runtime["TotalDataSize"], runtime["ConsensusMethod"],
                 runtime["PbftViewChangeTimeOut"], runtime["FormalMapCheckpoint"])
            == (rate, count, METHODS[method], pbft, count // 2),
            f"{condition}: run settings differ from the paper configuration")
    require(checkpoint["source_position"] == count // 2 and checkpoint["shards"] == shards
            and checkpoint["method"] == METHODS[method], f"{condition}: E1 checkpoint differs")
    require(state["run_id"] == record["archived_run_id"]
            and fingerprint["data_sha256"] == record["input_sha256"]
            and settings["config_sha256"] == fingerprint["config_sha256"] == hashes[RUNTIME],
            f"{condition}: run ID or input/configuration fingerprint differs")
    if archive:
        require(hashes[RUNTIME] == record["archived_config_sha256"]
                and fingerprint["binary_sha256"] == record["binary_sha256"]
                and int(meta["first_injection_unix_ns"]) == int(record["first_injection_unix_ns"]),
                f"{condition}: archive configuration, binary or dispatch identity differs")
    if method == "glas":
        require(runtime["GLASPendingFullWindowEarly"] is True
                and runtime["GLASPendingWindowRatio"] == Decimal("1.3")
                and runtime["ExperimentBlocksPerEpoch"] == 6, f"{condition}: wrong GlasChain variant")
    require(meta["status"] == "closed" and meta.get("completion_scope", "full") == "full"
            and meta["clock"] == "supervisor_receipt_monotonic"
            and validation["valid_for_publication"] and summary["valid_for_publication"]
            and full["complete"] and full["outstanding"] == 0
            and full["completed_unique"] == full["dispatched_unique"] == count
            and not any(summary["duplicate_replay_rows"].get(key, 0) for key in ("stage", "inject")),
            f"{condition}: full-run completion did not validate")
    for metric in CROSSING:
        require(math.isclose(full[f"{metric}_completed"] / count, full[f"{metric}_ratio"],
                             rel_tol=0, abs_tol=1e-12), f"{condition}: invalid {metric} ratio")
    top, call, union = (full[f"{metric}_completed"] for metric in CROSSING)
    require(0 <= max(top, call) <= union <= min(count, top + call),
            f"{condition}: invalid crossing counts")

    require(sha(run / AVERAGE) == hashes[AVERAGE], f"{condition}: epoch report checksum differs")
    rows = list(epochs(run).values())
    adjusted = sum(row["adjusted_stage_count"] for row in rows)
    relay1, relay2 = (sum(row[stage] for row in rows) for stage in ("relay1", "relay2"))
    require(adjusted == count and relay1 == relay2 == top, f"{condition}: epoch stage totals differ")
    span = math.fsum(row["nonempty_block_span_seconds"] or 0 for row in rows)
    zero_span = sum(row["status"] == "positive_count_zero_span" for row in rows)
    require(span > 0 and zero_span == 0, f"{condition}: weighted epoch throughput is undefined")

    events_hash, counts, per_shard = scan_blocks(run / EVENTS, shards)
    require(events_hash == hashes[EVENTS], f"{condition}: raw event checksum differs")
    require(all(counts[key] == meta[key] for key in counts)
            and counts["injection_rows"] == count
            and counts["stage_rows"] == count + top == sum(row["stages"] for row in per_shard)
            and all(0 <= row["last"] <= meta["observed_duration_ns"] for row in per_shard),
            f"{condition}: event and recorder counts or shard receipts disagree")
    last_final = int(full["last_completion_seconds"] * NS)
    last_stage = max(row["last"] for row in per_shard)
    require(last_stage >= last_final and (not archive or last_stage == last_final),
            f"{condition}: unexpected final and last-stage receipt times")

    common = dict(condition=condition, run_id=state["run_id"], method=method,
                  shards=shards, inject_tps=rate, source_transactions=count, independent_runs=1,
                  pbft_view_change_ms=pbft, runner_status=state["status"], analysis_directory=analysis)
    observed = dict(**common, measured_epochs=sum(row["status"] == "measured" for row in rows),
                    empty_epochs=sum(row["status"] == "no_nonempty_block" for row in rows),
                    nonempty_zero_span_epochs=zero_span, adjusted_stage_count=adjusted,
                    measured_span_seconds=span, weighted_epoch_stage_tps=adjusted / span,
                    **{key: full[key] for key in FULL_FIELDS}, whole_run_finalization_tps=full["tps"])
    maximum = dict(
        condition=condition, run_id=state["run_id"], shards=shards, inject_tps=rate,
        source_transactions=count, method=method, pbft_view_change_ms=pbft,
        analysis_directory=analysis, events_path=f"{record['canonical_run_path']}/{EVENTS}",
        events_sha256=events_hash, summary_sha256=hashes[summary_file],
        max_shard_processing_time_ns=last_stage,
        max_shard_processing_time_seconds=Decimal(last_stage) / NS,
        max_shard_ids=" ".join(str(i) for i, row in enumerate(per_shard) if row["last"] == last_stage),
        last_final_elapsed_ns=last_final, stage_minus_final_ns=last_stage - last_final,
    )
    return observed, [dict(**common, **row) for row in rows], maximum


def select_runs(root, conditions, archive):
    records = [r for r in read_json(PROVENANCE)["runs"] if "E1" in r["paper_experiments"]]
    require(len(records) == 40 and len({r["condition"] for r in records}) == 40
            and len({r["archived_run_id"] for r in records}) == 40, "expected 40 unique formal E1 runs")
    if conditions:
        require(conditions <= {r["condition"] for r in records}, "unknown E1 condition selected")
    for record in records:
        if conditions and record["condition"] not in conditions:
            continue
        candidates = [root / record["canonical_run_path"]]
        if archive:
            candidates.append(root / record["archive_path"])
        else:
            candidates.append(root / record["archived_run_id"].removesuffix("_r01") / "r01")
        existing = [p for p in dict.fromkeys(candidates) if (p / STATE).is_file()]
        require(bool(existing), f"{record['condition']}: missing run directory, checked {candidates}")
        if not archive:
            require(len(existing) == 1, f"{record['condition']}: multiple matching reruns under {root}")
        yield existing[0], record


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True, help="canonical archive or rerun root")
    parser.add_argument("--archive", action="store_true", help="verify against the formal archive fingerprints")
    parser.add_argument("--out", type=Path, required=True, help="root for generated figure-source tables")
    parser.add_argument("--condition", nargs="+", help="verify a subset of E1 runs for inspection")
    args = parser.parse_args()
    try:
        root, output = args.runs.resolve(), args.out.resolve()
        require(not output.is_relative_to(root), "use an output directory outside the input runs")
        selected = list(select_runs(root, set(args.condition or ()), args.archive))
        observed, epoch_rows, maxima = [], [], []
        for run, record in selected:
            row, epochs_for_run, maximum = derive(run, record, args.archive)
            observed.append(row)
            epoch_rows.extend(epochs_for_run)
            maxima.append(maximum)
            print(f"Validated {record['condition']}", flush=True)
        target = output / "e1/e1_statistics"
        target.mkdir(parents=True, exist_ok=True)
        for name, rows in (("observed_run_summary.csv", observed), ("epoch_block_span_tps.csv", epoch_rows),
                           ("max_shard_processing_time.csv", maxima)):
            write_csv(target / name, rows)
        print(f"Wrote {len(observed)} runs and {len(epoch_rows)} epochs to {target}")
    except (OSError, ValueError, KeyError, csv.Error) as exc:
        parser.exit(2, f"E1 analysis failed: {exc}\n")


if __name__ == "__main__":
    main()
