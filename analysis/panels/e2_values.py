"""Read the verified E2 600-second counts and common-hash latencies."""
import csv
import math
from pathlib import Path

SETTINGS = ((4, 1000), (8, 1500), (16, 2000), (24, 2500), (32, 3000))
METHODS = ("glas", "clpa", "plouvain")


def settings_rows(metric, methods=METHODS):
    source = Path("outputs/e2/e2_overview/figure_data.csv")
    with source.open(newline="", encoding="utf-8-sig") as stream:
        rows = list(csv.DictReader(stream))
    by_run = {(int(r["shards"]), int(r["inject_tps"]), r["method"]): r for r in rows}
    if len(rows) != 15 or set(by_run) != {(*s, m) for s in SETTINGS for m in METHODS}:
        raise ValueError("Expected the 15 plotted E2 runs")
    for setting in SETTINGS:
        comparable = [by_run[(*setting, method)] for method in METHODS]
        sample = {int(r["matched_sample_size"]) for r in comparable}
        if len(sample) != 1 or next(iter(sample)) <= 0:
            raise ValueError(f"Inconsistent common sample for {setting}")
        if "matched_sample_sha256" in comparable[0] and len({r["matched_sample_sha256"] for r in comparable}) != 1:
            raise ValueError(f"Inconsistent common sample hash for {setting}")
        for row in comparable:
            sent, done = int(row["dispatched"]), int(row["confirmed"])
            if (int(row["window_start_s"]), int(row["window_end_s"])) != (0, 600):
                raise ValueError(f"Expected a 600-second window for {row['run_id']}")
            if (not 0 < done <= sent or not math.isclose(done / 600, float(row["throughput_tps"]))
                    or not math.isclose(done / sent, float(row["confirmation_rate"]))):
                raise ValueError(f"Inconsistent E2 counts for {row['run_id']}")
    parse = int if metric == "confirmed" else float
    return [(*s, *(parse(by_run[(*s, m)][metric]) for m in methods)) for s in SETTINGS]
