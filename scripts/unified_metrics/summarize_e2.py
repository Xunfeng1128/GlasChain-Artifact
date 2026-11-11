#!/usr/bin/env python3
"""Validate an E2 observation and report its TPS windows and cumulative input-hash cohorts."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

if __package__:
    from . import summarize as ledger
else:
    import summarize as ledger


def load_prefix(db, path, count):
    """Bind the supplied input order and its exact bytes in the same pass."""
    digest = hashlib.sha256()
    db.execute("CREATE TABLE prefix (position INTEGER PRIMARY KEY, source TEXT UNIQUE NOT NULL)")
    with path.open("rb") as handle:
        header = handle.readline()
        digest.update(header)
        if header.rstrip(b"\r\n") != b"source_tx_hash":
            raise ledger.InvalidRun("prefix CSV must have one column named source_tx_hash")
        size = 0
        for size, line in enumerate(handle, 1):
            digest.update(line)
            source = line.rstrip(b"\r\n")
            if size > count or not re.fullmatch(rb"0x[0-9a-f]{64}", source):
                raise ledger.InvalidRun(f"unexpected or invalid hash at prefix position {size}")
            try:
                db.execute("INSERT INTO prefix VALUES (?,?)", (size, source.decode("ascii")))
            except sqlite3.IntegrityError as exc:
                raise ledger.InvalidRun(f"duplicate prefix hash at position {size}") from exc
    if size != count:
        raise ledger.InvalidRun(f"prefix contains {size} hashes, expected {count}")
    return digest.hexdigest()


def ordered_positive(values, name):
    if not values or list(values) != sorted(set(values)) or values[0] <= 0:
        raise ledger.InvalidRun(f"{name} must be positive and strictly increasing")


def report_rows(db, windows, prefixes):
    """Run only against an integrity-checked ledger with a complete prefix."""
    throughput = []
    for window in windows:
        n, = db.execute("SELECT count(*) FROM stages WHERE stage IN ('LOCAL','RELAY2') AND elapsed<?",
                        (window,)).fetchone()
        injected, = db.execute("SELECT count(*) FROM injected WHERE elapsed<?", (window,)).fetchone()
        t = ledger.seconds(window)
        throughput.append({"start_seconds": 0, "end_seconds": t, "completed": n, "tps": n / t,
                           "injected": injected, "outstanding_at_end": injected - n})

    db.execute("CREATE INDEX e2_latency_order ON sample(latency,position)")
    latency = []
    for size in prefixes:
        total, last_send, last_final, at_end = db.execute("""
            SELECT exact_sum(latency), max(sent), max(finished), sum(finished < ?)
            FROM sample WHERE position<=?
        """, (windows[-1], size)).fetchone()
        rank = (size * 95 + 99) // 100
        p95, = db.execute("SELECT latency FROM sample WHERE position<=? ORDER BY latency LIMIT 1 OFFSET ?",
                          (size, rank - 1)).fetchone()
        latency.append({"prefix_count": size, "injected": size, "completed": size,
                        "mean_latency_seconds": int(total) / (size * ledger.NS),
                        "p95_latency_seconds": ledger.seconds(p95),
                        "confirmed_before_observation_end": at_end,
                        "last_dispatch_seconds": ledger.seconds(last_send),
                        "last_confirmation_seconds": ledger.seconds(last_final)})
    return throughput, latency


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize_e2(run_dir, output_dir, prefix_file, windows, prefixes):
    """windows uses integer nanoseconds. prefixes uses one-based input ranks."""
    run_dir, output_dir, prefix_file = (Path(p).resolve() for p in (run_dir, output_dir, prefix_file))
    if output_dir == run_dir or run_dir in output_dir.parents:
        raise ledger.InvalidRun("E2 output must be outside the raw logs directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    validation = output_dir / "e2_validation.json"
    table_names = ("e2_throughput.csv", "e2_latency.csv")
    if any((output_dir / name).exists() for name in (*table_names, validation.name)):
        raise ledger.InvalidRun("E2 results already exist; choose a fresh output directory")
    # Exclusive reservation prevents simultaneous writers. Publish validation last,
    # so interrupted work cannot look successful or overwrite a prior publication.
    with validation.open("x", encoding="utf-8") as f:
        json.dump({"valid_for_e2": False, "status": "analyzing"}, f)
    info = {"schema_version": 1, "valid_for_e2": False, "all_input_complete": False,
            "clock": "supervisor_receipt_monotonic", "run_dir": str(run_dir)}
    issues = ledger.Issues()
    with tempfile.TemporaryDirectory(dir=output_dir, prefix=".e2-") as work:
        tmp = Path(work)
        db = None
        try:
            meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
            scope = meta.get("completion_scope", "full")
            info["completion_scope"] = scope
            if scope not in ("full", "e2") or meta.get("schema_version") != 1 or meta.get("clock") != info["clock"]:
                raise ledger.InvalidRun("unsupported completion scope, clock or schema")
            if meta.get("status") != "closed" or meta.get("completion_failure"):
                raise ledger.InvalidRun("E2 requires a normally closed, successful recorder")
            ordered_positive(windows, "windows")
            ordered_positive(prefixes, "prefixes")
            cfg = meta["config"]
            expected = ledger.integer(cfg["expected_transactions"], "expected_transactions", 1)
            covered = ledger.integer(meta["observed_duration_ns"], "observed_duration_ns")
            ledger.integer(meta["first_injection_unix_ns"], "first_injection_unix_ns", 1)
            if expected < prefixes[-1] or covered < windows[-1]:
                raise ledger.InvalidRun("configured input cap or observed duration does not cover E2 targets")

            db = sqlite3.connect(tmp / "ledger.sqlite3")
            ledger.initialize(db)
            digest = load_prefix(db, prefix_file, prefixes[-1])
            info.update({"expected_transactions": expected, "prefix_file_sha256": digest,
                         "prefix_bound_by_recorder": scope == "e2", "prefixes": list(prefixes),
                         "windows_seconds": [ledger.seconds(w) for w in windows],
                         "observed_duration_seconds": ledger.seconds(covered), "percentile_method": "nearest_rank"})
            if scope == "e2":
                target_count = ledger.integer(meta["e2_prefix_count"], "e2_prefix_count", 1)
                target_ns = ledger.seconds_ns(meta["e2_observation_seconds"], "e2_observation_seconds", True)
                if target_count != prefixes[-1] or target_ns != windows[-1] or meta["e2_prefix_sha256"] != digest:
                    raise ledger.InvalidRun("recorder E2 targets or prefix digest differ from this analysis")
            raw, replays = ledger.ingest(db, run_dir / "events.csv", meta, issues)
            ledger.query_issues(db, issues, require_all_completed=(scope == "full"))
            ledger.check_block_gas(db, ledger.integer(cfg["block_gas_limit"], "block_gas_limit", 1), issues)
            noncanonical = db.execute("""
                SELECT source FROM injected WHERE length(source)!=66 OR substr(source,1,2)!='0x'
                   OR substr(source,3) GLOB '*[^0-9a-f]*' LIMIT 1
            """).fetchone()
            if noncanonical is not None:
                issues.add("noncanonical_source", repr(noncanonical))
            injected, first = db.execute("SELECT count(*),min(elapsed) FROM injected").fetchone()
            finals, = db.execute("SELECT count(*) FROM stages WHERE stage IN ('LOCAL','RELAY2')").fetchone()
            if first != 0:
                issues.add("invalid_origin", "first injection must have zero elapsed_ns")
            if injected > expected or (scope == "full" and (injected != expected or finals != injected)):
                issues.add("count_mismatch", f"injected={injected}, completed={finals}, expected={expected}")
            db.execute("""CREATE TABLE sample AS
                SELECT p.position,p.source,i.elapsed AS sent,s.elapsed AS finished,s.elapsed-i.elapsed AS latency
                FROM prefix p LEFT JOIN injected i USING(source)
                LEFT JOIN stages s ON s.source=p.source AND s.stage IN ('LOCAL','RELAY2')""")
            sample_injected, sample_final = db.execute("SELECT count(sent),count(finished) FROM sample").fetchone()
            if sample_injected != prefixes[-1] or sample_final != prefixes[-1]:
                issues.add("incomplete_prefix", f"{sample_injected} injected, {sample_final} confirmed, expected {prefixes[-1]}")
            if scope == "e2":
                online = meta["completion_check"]
                for key, offline in (("expected_transactions", expected), ("unique_injected", injected),
                                     ("unique_completed", finals), ("unique_final_sources", finals),
                                     ("e2_prefix_count", prefixes[-1]), ("e2_prefix_injected", sample_injected),
                                     ("e2_prefix_completed", sample_final)):
                    if ledger.integer(online[key], key) != offline:
                        issues.add("online_offline_count_mismatch", f"{key} differs from raw events")
            info.update({"raw_rows": raw, "duplicate_replay_rows": replays, "unique_injected": injected,
                         "unique_completed": finals, "outstanding": injected - finals,
                         "unissued": expected - injected, "sample_injected": sample_injected,
                         "sample_completed": sample_final})
            if issues.counts:
                raise ledger.InvalidRun("E2 integrity or completeness checks failed")
            t_rows, l_rows = report_rows(db, windows, prefixes)
            for name, rows in zip(table_names, (t_rows, l_rows)):
                write_csv(tmp / name, rows)
                os.link(tmp / name, output_dir / name)
            info.update({"valid_for_e2": True, "all_input_complete": injected == finals == expected})
        except (OSError, ValueError, KeyError, TypeError, csv.Error, sqlite3.Error, ledger.InvalidRun) as exc:
            if not issues.counts:
                issues.add("invalid_e2_run", str(exc))
            for name in table_names:
                (output_dir / name).unlink(missing_ok=True)
            raise ledger.InvalidRun(f"{exc}; see {validation}") from exc
        finally:
            if db is not None:
                db.close()
            detail = issues.report()
            # Avoid implying an E2-only ledger has full-run publication validity.
            detail.pop("valid_for_publication")
            ledger.write_json(tmp / validation.name, {**info, **detail})
            os.replace(tmp / validation.name, validation)
    return info


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--prefix-file", required=True, type=Path,
                        help="Canonical valid-input-order source_tx_hash CSV, one row per source")
    parser.add_argument("--windows", nargs="+", default=["600", "960"])
    parser.add_argument("--prefixes", nargs="+", type=int, default=[300_000, 500_000, 700_000, 900_000])
    args = parser.parse_args(argv)
    try:
        windows = [ledger.seconds_ns(w, "windows", positive=True) for w in args.windows]
        summarize_e2(args.run_dir, args.output_dir, args.prefix_file, windows, args.prefixes)
    except (OSError, ValueError, ledger.InvalidRun) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(f"Wrote E2 throughput, latency and validation to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
