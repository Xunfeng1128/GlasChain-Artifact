"""Panel code from e3/e3_b_cross_shard_composition.ipynb."""

import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator

project = next(p for p in (Path.cwd(), *Path.cwd().parents)
               if (p / "styles/paper.mplstyle").is_file())
out = project / "outputs/e3/e3_b_cross_shard_composition"
with (out / "figure_data.csv").open(newline="", encoding="utf-8") as f:
    data = {(int(row["shards"]), int(row["inject_tps"]), row["method"]): row
            for row in csv.DictReader(f)}

settings = [(8, 1000), (8, 2000), (16, 1000), (16, 2000)]
methods = [
    ("glas", "GlasChain", "#E8747E", "/"),
    ("clpa", "CLPA", "#6FCBB8", "+"),
    ("plouvain", "Contribchain-P", "#A874D8", "x"),
    ("hash", "Hash", "#E9B85C", "\\"),
]

x = np.arange(len(settings))
width = 0.18
ink = "#252525"
with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.214, 3.68))
    fig.subplots_adjust(left=0.191404, right=0.958570, bottom=0.198370, top=0.951082)
    for i, (key, label, color, hatch) in enumerate(methods):
        positions = x + (i - 1.5) * width
        top_only = [float(data[s, r, key]["top_only_pct"]) for s, r in settings]
        calls = [float(data[s, r, key]["call_cross_pct"]) for s, r in settings]
        ax.bar(positions, top_only, width, label=label, color=color,
               edgecolor=ink, linewidth=0.7)
        ax.bar(positions, calls, width, bottom=top_only, color=color,
               hatch="///", edgecolor=ink, linewidth=0.7)
    ax.set_xticks(x, [f"[{rate}, {shards}]" for shards, rate in settings])
    ax.set_xlabel("[Tx arrival rate, shard count]", fontsize=13.5)
    ax.set_ylabel("Cross-shard TX ratio (%)", fontweight="bold")
    ax.set(xlim=(-0.46, len(settings) - 0.54), ylim=(0, 118))
    ax.yaxis.set_major_locator(MultipleLocator(20))
    ax.tick_params(direction="out")
    ax.tick_params(axis="x", labelsize=11)
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)


    ax.legend(handles=[Patch(facecolor=m[2], edgecolor="#252525", linewidth=0.7, label=m[1])
                       for m in methods]
                   + [Patch(facecolor="0.85", edgecolor="#252525", label="Top-level only"),
                      Patch(facecolor="0.85", edgecolor="#252525", hatch="///", label="Internal call")], loc="upper left", ncols=3, fontsize=8.4,
              frameon=True, facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
              borderpad=0.35, labelspacing=0.3, handlelength=1.25, handleheight=0.85,
              handletextpad=0.45, columnspacing=0.75, borderaxespad=0.4)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e3_b_cross_shard_composition.{ext}", dpi=600, facecolor="white")
