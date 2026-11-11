"""Panel code from e1/e1_c_cross_shard_ratio_s8.ipynb."""

import csv
import hashlib
import json
from math import ceil, isclose, isfinite
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from colorsys import hsv_to_rgb, rgb_to_hsv

from matplotlib.colors import to_rgb

project = next(p for p in (Path.cwd(), *Path.cwd().parents)
               if (p / "styles/paper.mplstyle").is_file())
stats = project / "outputs/e1/e1_statistics"
ratio_source = stats / "observed_run_summary.csv"
time_source = stats / "max_shard_processing_time.csv"
out = project / "outputs/e1/e1_c_cross_shard_ratio_s8"
out.mkdir(parents=True, exist_ok=True)

shards, inject_tps = 8, 1500
sizes = [500_000, 1_000_000, 1_500_000, 2_000_000]
methods = [
    ("glas", "GlasChain", "#E78A7B", "/"),
    ("clpa", "CLPA", "#76A8C9", "+"),
    ("plouvain", "Contribchain-P", "#69BBA6", "x"),
    ("txallo", "TxAllo", "#B5A3D3", ".."),
    ("garet", "GARET-FS", "#F0C877", "\\"),
]
labels = {
    "x": "Transaction Number (million)",
    "left": "Cross-shard TX Ratio (%)",
    "right": "Max Shard Processing Time (sec)",
}
time_field = "max_shard_processing_time_seconds"

def read_rows(path):
    with path.open(newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))

def run_key(row):
    return (row["condition"], row["run_id"], int(row["shards"]), int(row["inject_tps"]),
            int(row["source_transactions"]), row["method"])

ratio_rows = read_rows(ratio_source)
time_rows = read_rows(time_source)
times = {run_key(row): float(row[time_field]) for row in time_rows}
if len(ratio_rows) != 40 or len(times) != len(time_rows) or set(times) != {run_key(r) for r in ratio_rows}:
    raise ValueError("Expected the same 40 unique runs in the cross-shard and processing-time tables.")
if any(not isfinite(t) or t <= 0 for t in times.values()):
    raise ValueError("Processing times must be positive and finite.")

rows = [row for row in ratio_rows if (int(row["shards"]), int(row["inject_tps"])) == (shards, inject_tps)]
data = {(int(row["source_transactions"]), row["method"]): row for row in rows}
if len(rows) != 20 or set(data) != {(n, key) for n in sizes for key, *_ in methods}:
    raise ValueError(f"Expected 20 unique conditions for {shards} shards.")

figure_data = []
for n in sizes:
    for key, *_ in methods:
        row = data[n, key]
        completed, crossed = int(row["completed_unique"]), int(row["union_cross_completed"])
        if completed != n or int(row["outstanding"]) != 0 or not 0 <= crossed <= completed:
            raise ValueError(f"Invalid completed counts for {n}, {key}.")
        if not isclose(crossed / completed, float(row["union_cross_ratio"]), rel_tol=1e-12):
            raise ValueError(f"Inconsistent cross-shard ratio for {n}, {key}.")
        figure_data.append({**row, time_field: times[run_key(row)]})

with (out / "figure_data.csv").open("w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=list(figure_data[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(figure_data)

def vivid(hex_color, saturation=1.3, value=1.12):
    """Keep the palette hue but push saturation/brightness up, so the lines read
    brighter than the hatched bars without leaving the colour scheme."""
    hue, sat, val = rgb_to_hsv(*to_rgb(hex_color))
    return hsv_to_rgb(hue, min(sat * saturation, 1.0), min(val * value, 1.0))


x = np.arange(len(sizes))
width = 0.145
time_tick = 5_000
time_limit = time_tick * ceil(max(times.values()) * 1.04 / time_tick)
with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.99, 3.68))
    fig.subplots_adjust(left=0.165276, right=0.833057, bottom=0.202989, top=0.951359)
    right = ax.twinx()
    right.set_zorder(ax.get_zorder() + 1)
    right.patch.set_visible(False)

    for i, (key, label, color, hatch) in enumerate(methods):
        ratios = [100 * int(data[n, key]["union_cross_completed"]) /
                  int(data[n, key]["completed_unique"]) for n in sizes]
        ax.bar(x + (i - 2) * (width + 0.007), ratios, width, label=label,
               color=color, hatch=hatch, edgecolor="#252525", linewidth=0.7)
        durations = [times[run_key(data[n, key])] for n in sizes]
        line_color = vivid(color)
        right.plot(x, durations, color=line_color, linewidth=1.45, linestyle="-",
                   marker="o", markersize=3.6, markerfacecolor=line_color,
                   markeredgecolor="white", markeredgewidth=0.45,
                   solid_capstyle="round")

    ax.set(xlim=(-0.47, len(x) - 0.53), xlabel=labels["x"], ylabel=labels["left"], ylim=(0, 118))
    right.set(ylabel=labels["right"], ylim=(0, time_limit))
    right.yaxis.label.set_fontsize(11)   # 右轴标题较长，字号收一档以免超出版面
    ax.set_xticks(x, [f"{n / 1_000_000:g}" for n in sizes])
    ax.set_yticks(range(0, 101, 20))
    right.set_yticks(range(0, time_limit + time_tick, time_tick))
    for axes in (ax, right):
        axes.tick_params(direction="out")
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)
    right.grid(False)
    ax.spines["right"].set_visible(False)
    for side in ("left", "bottom", "top"):
        right.spines[side].set_visible(False)


    ax.legend(handles=ax.containers, loc="upper left", fontsize=8.9, ncols=2,
              frameon=True, facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
              borderpad=0.35, labelspacing=0.3, handlelength=1.25, handleheight=0.85,
              handletextpad=0.45, columnspacing=0.75, borderaxespad=0.4)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e1_c_cross_shard_ratio_s8.{ext}", dpi=600, facecolor="white")


files = ["figure_data.csv", *(f"e1_c_cross_shard_ratio_s8.{ext}" for ext in ("pdf", "svg", "png"))]
validation = {
    "sources_sha256": {str(path.relative_to(project)): hashlib.sha256(path.read_bytes()).hexdigest()
                        for path in (ratio_source, time_source)},
    "outputs_sha256": {name: hashlib.sha256((out / name).read_bytes()).hexdigest() for name in files},
    "rows": len(figure_data),
    "axis_labels": labels,
    "y_limits": {"cross_shard_ratio_percent": [0, 100], "max_shard_processing_time_seconds": [0, time_limit]},
    "left_metric": "100 * union_cross_completed / completed_unique, full run",
    "right_metric": "maximum elapsed time across shards from first actual dispatch to last valid execution-stage receipt",
}
(out / "validation.json").write_text(json.dumps(validation, indent=2) + "\n", encoding="utf-8")
