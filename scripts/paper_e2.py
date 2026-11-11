#!/usr/bin/env python3
"""Rebuild Figure 8 from local E2 run directories and the Ethereum input order."""
import argparse
import csv
import hashlib
from itertools import islice
import json
from pathlib import Path

from unified_metrics.summarize import COLUMNS, NS

ROOT = Path(__file__).resolve().parents[1]
PROVENANCE = ROOT / "provenance/formal-runs.json"
TRACE = ROOT / "datasets/icde_e2_1000001_4000000/selection.json"
META = "result/unified_metrics/run.json"
EVENTS = "result/unified_metrics/events.csv"
WINDOW = 600 * NS
HEADER = ",".join(COLUMNS).encode("ascii")
METHODS = ("glas", "clpa", "plouvain")
SAMPLE_SIZE = {4: 50_000, 8: 100_000, 16: 200_000, 24: 300_000, 32: 400_000}


def load(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def sha256(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def scan_cohort(path, meta, expected_sha):
    """Verify the full event ledger while retaining receipts in [0, 600 s)."""
    digest = hashlib.sha256()
    injections, finals = {}, {}
    counts = {b"inject": 0, b"stage": 0, b"block": 0}
    duration = meta["observed_duration_ns"]
    with path.open("rb", buffering=2 * 1024 * 1024) as file:
        header = file.readline()
        if header.rstrip(b"\r\n") != HEADER:
            raise ValueError(f"{path}: unexpected event header")
        digest.update(header)
        for line in file:
            digest.update(line)
            if line.startswith(b"block,"):
                counts[b"block"] += 1
                continue
            event, source, stage, elapsed_text, _ = line.split(b",", 4)
            elapsed = int(elapsed_text)
            if event not in (b"inject", b"stage") or not 0 <= elapsed <= duration:
                raise ValueError(f"{path}: invalid event or time")
            counts[event] += 1
            if event == b"inject" and stage == b"TOP":
                if elapsed < WINDOW:
                    injections[source] = min(elapsed, injections.get(source, WINDOW))
            elif event == b"stage" and stage in (b"LOCAL", b"RELAY1", b"RELAY2"):
                if elapsed < WINDOW and stage != b"RELAY1":
                    old_stage, old_elapsed = finals.get(source, (stage, WINDOW))
                    if stage != old_stage:
                        raise ValueError(f"{path}: conflicting final stages")
                    finals[source] = (stage, min(elapsed, old_elapsed))
            else:
                raise ValueError(f"{path}: invalid stage")
    actual_sha = digest.hexdigest()
    if actual_sha != expected_sha:
        raise ValueError(f"{path}: raw event SHA-256 mismatch")
    for event, field in ((b"inject", "injection_rows"), (b"stage", "stage_rows"), (b"block", "block_rows")):
        if counts[event] != meta[field]:
            raise ValueError(f"{path}: {field} disagrees with recorder metadata")
    if not injections or min(injections.values()) != 0:
        raise ValueError(f"{path}: missing first actual dispatch")
    delays = {}
    for source, (_, elapsed) in finals.items():
        if source not in injections or injections[source] > elapsed:
            raise ValueError(f"{path}: confirmation lacks a preceding dispatch")
        delays[source] = elapsed - injections[source]
    return len(injections), delays, actual_sha


def scan_run(folder, entry, archived):
    state, meta = load(folder / "run_state.json"), load(folder / META)
    recorded = state["artifacts_sha256"]
    if archived:
        recorded = entry["archived_files_sha256"]
    config = meta["config"]
    if (
        state["status"] != "completed" or state["run_id"] != entry["archived_run_id"]
        or meta["status"] != "closed" or meta.get("completion_failure")
        or meta["clock"] != "supervisor_receipt_monotonic" or meta["observed_duration_ns"] < WINDOW
        or (config["shard_count"], config["inject_speed"]) != (entry["shard_count"], entry["inject_tps"])
        or state["fingerprint"]["data_sha256"] != entry["input_sha256"]
        or sha256(folder / META) != recorded[META]
        or sha256(folder / "runtime_config.json") != recorded["runtime_config.json"]
    ):
        raise ValueError(f"{folder}: wrong run, input or clock")
    return scan_cohort(folder / EVENTS, meta, recorded[EVENTS])


def input_order(transactions, trace):
    with transactions.open(newline="", encoding="utf-8-sig") as file:
        rows = csv.DictReader(file)
        if "transactionHash" not in (rows.fieldnames or ()):
            raise ValueError("expected Ethereum transactionHash input column")
        yield from (row["transactionHash"].lower().encode("ascii") for row in islice(
            rows, trace[0], trace[1]))


def input_range(path, metadata):
    digest = sha256(path)
    if digest == metadata["files"]["transactions.csv"]["sha256"]:
        return 0, metadata["files"]["transactions.csv"]["data_rows"]
    if digest == metadata["source_sha256"]["transactions.csv"]:
        return metadata["first_source_position"] - 1, metadata["last_source_position"]
    raise ValueError("transaction input SHA-256 differs from both verified source versions")


def metrics(scanned, order, size):
    shared = set.intersection(*(set(per_tx) for _, per_tx, _ in scanned.values()))
    sample = list(islice((source for source in order if source in shared), size))
    if len(sample) != size or len(set(sample)) != size:
        raise ValueError(f"the common confirmed sample needs {size:,} ordered distinct transactions")
    sample_sha = hashlib.sha256(b"source_tx_hash\n" + b"\n".join(sample) + b"\n").hexdigest()
    p95_index = (95 * size + 99) // 100 - 1
    for method, (sent, delays, event_sha) in scanned.items():
        values = sorted(delays[source] for source in sample)
        yield method, dict(
            window_start_s=0, window_end_s=600, dispatched=sent, confirmed=len(delays),
            throughput_tps=len(delays) / 600, confirmation_rate=len(delays) / sent,
            matched_sample_size=size, independent_runs=1, mean_latency_s=sum(values) / size / NS,
            p95_latency_s=values[p95_index] / NS, matched_sample_sha256=sample_sha,
            source_events_sha256=event_sha,
        )


def collect(root, transactions, entries, selection, archived, settings):
    indices = input_range(transactions, selection)
    entries = {r["condition"]: r for r in entries if r["archive_path"].startswith("e2/")}
    folders = {}
    if not archived:
        for state in root.rglob("run_state.json"):
            key = state.parent.parent.name
            if key in entries and state.parent.name == "r01":
                if key in folders:
                    raise ValueError(f"multiple run directories for {key}; choose a single output root")
                folders[key] = state.parent
    for shards in settings:
        rate = {4: 1000, 8: 1500, 16: 2000, 24: 2500, 32: 3000}[shards]
        matching = {m: entries[f"e2_{'glas13' if m == 'glas' else m}_s{shards}_r{rate}"] for m in METHODS}
        scanned = {}
        for method, entry in matching.items():
            path = root / entry["archive_path"] if archived else folders.get(entry["condition"])
            if path is None:
                raise ValueError(f"no run found for {entry['condition']}")
            scanned[method] = scan_run(path, entry, archived)
        for method, row in metrics(scanned, input_order(transactions, indices), SAMPLE_SIZE[shards]):
            yield dict(shards=shards, inject_tps=rate, method=method,
                       run_id=matching[method]["archived_run_id"], **row)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=Path, required=True, help="archive or rerun root")
    parser.add_argument("--archive", action="store_true", help="verify the archived hashes and canonical paths")
    parser.add_argument("--transactions", type=Path, default=TRACE.with_name("transactions.csv"))
    parser.add_argument("--shards", type=int, choices=SAMPLE_SIZE, nargs="+", default=list(SAMPLE_SIZE))
    parser.add_argument("--out", type=Path, required=True, help="separate destination for regenerated summary tables")
    args = parser.parse_args()
    root, out = args.runs.resolve(strict=True), args.out.resolve()
    if out.is_relative_to(root) or root.is_relative_to(out):
        parser.error("use a summary directory separate from the input runs")
    rows = list(collect(root, args.transactions, load(PROVENANCE)["runs"], load(TRACE), args.archive, args.shards))
    table = out / "e2/e2_overview/figure_data.csv"
    table.parent.mkdir(parents=True, exist_ok=True)
    with table.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved {len(rows)} method-setting rows to {table}")


if __name__ == "__main__":
    main()
