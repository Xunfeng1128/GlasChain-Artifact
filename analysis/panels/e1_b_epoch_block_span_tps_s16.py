"""Panel code from e1/e1_b_epoch_block_span_tps_s16.ipynb."""

import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator

project = next(p for p in (Path.cwd(), *Path.cwd().parents)
               if (p / "styles/paper.mplstyle").is_file())
source = project / "outputs/e1/e1_statistics/observed_run_summary.csv"
epoch_source = project / "outputs/e1/e1_statistics/epoch_block_span_tps.csv"
out = project / "outputs/e1/e1_b_epoch_block_span_tps_s16"
out.mkdir(parents=True, exist_ok=True)

shards, inject_tps = 16, 2000
sizes = [500_000, 1_000_000, 1_500_000, 2_000_000]
methods = [
    ("glas", "GlasChain", "#E78A7B", "/"),
    ("clpa", "CLPA", "#76A8C9", "+"),
    ("plouvain", "Contribchain-P", "#69BBA6", "x"),
    ("txallo", "TxAllo", "#B5A3D3", ".."),
    ("garet", "GARET-FS", "#F0C877", "\\\\"),
]

with source.open(newline="", encoding="utf-8-sig") as file:
    reader = csv.DictReader(file)
    fields = reader.fieldnames
    all_rows = list(reader)
epoch_groups = defaultdict(list)
with epoch_source.open(newline="", encoding="utf-8-sig") as file:
    for epoch in csv.DictReader(file):
        epoch_groups[epoch["run_id"]].append(epoch)

if len(epoch_groups) != len(all_rows) or set(epoch_groups) != {r["run_id"] for r in all_rows}:
    raise ValueError("Epoch data and run summary cover different runs.")

for row in all_rows:
    run_id = row["run_id"]
    epochs = [e for e in epoch_groups[run_id] if e["status"] == "measured"]
    if len(epochs) != int(row["measured_epochs"]):
        raise ValueError(f"Unexpected measurement coverage for {run_id}.")
    if len({e["epoch"] for e in epochs}) != len(epochs):
        raise ValueError(f"Repeated epoch for {run_id}.")
    amounts, spans = [], []
    for epoch in epochs:
        count = int(epoch["local"]) + (int(epoch["relay1"]) + int(epoch["relay2"])) / 2
        span = float(epoch["nonempty_block_span_seconds"])
        if (span <= 0 or not math.isclose(count, float(epoch["adjusted_stage_count"]))
                or not math.isclose(count / span, float(epoch["epoch_stage_tps"]), rel_tol=1e-12)):
            raise ValueError(f"Inconsistent stage TPS for {run_id}, epoch {epoch['epoch']}.")
        amounts.append(count)
        spans.append(span)
    total_count = math.fsum(amounts)
    total_span = math.fsum(spans)
    tps = total_count / total_span
    if (not math.isclose(total_count, float(row["adjusted_stage_count"]))
            or not math.isclose(total_span, float(row["measured_span_seconds"]), rel_tol=1e-12)
            or not math.isclose(tps, float(row["weighted_epoch_stage_tps"]), rel_tol=1e-12)):
        raise ValueError(f"Inconsistent weighted TPS for {run_id}.")

rows = [row for row in all_rows if int(row["shards"]) == shards
        and int(row["inject_tps"]) == inject_tps]
data = {(int(row["source_transactions"]), row["method"]): row for row in rows}
expected = {(n, key) for n in sizes for key, *_ in methods}
if len(rows) != len(expected) or data.keys() != expected:
    raise ValueError(f"Expected 20 unique conditions for {shards} shards.")

with (out / "figure_data.csv").open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(data[n, key] for n in sizes for key, *_ in methods)

x = np.arange(len(sizes))
width = 0.145
with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.24, 3.68))
    fig.subplots_adjust(left=0.194656, right=0.958018, bottom=0.202989, top=0.951359)
    for i, (key, label, color, hatch) in enumerate(methods):
        heights = [float(data[n, key]["weighted_epoch_stage_tps"]) for n in sizes]
        ax.bar(x + (i - 2) * width, heights, width, label=label,
               color=color, hatch=hatch, edgecolor="#252525", linewidth=0.7)
    ax.set(xlim=(-0.47, len(x) - 0.53), xlabel="Transaction Number (million)",
           ylabel="Throughput (TPS)")
    ax.set_xticks(x, [f"{n / 1_000_000:g}" for n in sizes])
    peak = max(float(row["weighted_epoch_stage_tps"]) for row in all_rows)
    ax.set_ylim(0, np.ceil(peak * 1.10 / 100) * 100)
    ax.yaxis.set_major_locator(MultipleLocator(100))
    ax.tick_params(direction="out")
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)

    ax.legend(handles=ax.containers, loc="upper left", fontsize=8.9, ncols=2,
              frameon=True, facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
              borderpad=0.35, labelspacing=0.3, handlelength=1.25, handleheight=0.85,
              handletextpad=0.45, columnspacing=0.75, borderaxespad=0.4)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e1_b_epoch_block_span_tps_s16.{ext}", dpi=600, facecolor="white")


validation = {
    "source": str(source.relative_to(project)),
    "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
    "epoch_source": str(epoch_source.relative_to(project)),
    "epoch_source_sha256": hashlib.sha256(epoch_source.read_bytes()).hexdigest(),
    "figure_data_sha256": hashlib.sha256((out / "figure_data.csv").read_bytes()).hexdigest(),
    "source_metric": "weighted_epoch_stage_tps",
    "method": "sum(LOCAL + (RELAY1 + RELAY2) / 2) / sum(nonempty block spans)",
    "shards": shards,
    "inject_tps": inject_tps,
    "rows": len(rows),
    "audited_runs": len(all_rows),
    "audited_epochs": sum(map(len, epoch_groups.values())),
    "garet_pbft_view_change_ms_by_source_transactions": {
        str(n): int(data[n, "garet"]["pbft_view_change_ms"]) for n in sizes},
    "ylim": list(ax.get_ylim()),
    "export_sha256": {ext: hashlib.sha256((out / f"{out.name}.{ext}").read_bytes()).hexdigest()
                       for ext in ("pdf", "svg", "png")},
}
(out / "validation.json").write_text(json.dumps(validation, indent=2) + "\n", encoding="utf-8")
