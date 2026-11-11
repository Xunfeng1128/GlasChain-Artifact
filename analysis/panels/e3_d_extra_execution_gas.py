"""Panel code from e3/e3_d_extra_execution_gas.ipynb."""

import csv
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
import numpy as np

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")
out = project / "outputs/e3/e3_d_extra_execution_gas"
with (out / "figure_data.csv").open(newline="", encoding="utf-8") as stream:
    rows = list(csv.DictReader(stream))
data = {(int(row["shards"]), int(row["inject_tps"]), row["method"]): row for row in rows}

methods = [
    ("glas", "GlasChain", "#E8747E"),
    ("clpa", "CLPA", "#6FCBB8"),
    ("plouvain", "Contribchain-P", "#A874D8"),
    ("hash", "Hash", "#E9B85C"),
]
settings = [(8, 1000), (8, 2000), (16, 1000), (16, 2000)]
assert len(rows) == len(data)
assert set(data) == {(k, r, key) for k, r in settings for key, _, _ in methods}

x = np.arange(len(settings))
ink = "#252525"
stack_hatch = "////"
with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.081, 3.674))
    fig.subplots_adjust(left=0.178901, right=0.966150, bottom=0.197063, top=0.951004)
    for index, (key, label, color) in enumerate(methods):
        group = [data[k, r, key] for k, r in settings]
        relay = [int(row["relay1_base_gas"]) / 1e9 for row in group]
        attached = [int(row["attached_gas"]) / 1e9 for row in group]
        centers = x + (index - 1.5) * 0.217
        bar_style = dict(width=0.217, color=color, edgecolor=ink, linewidth=0.7)
        ax.bar(centers, relay, **bar_style)
        ax.bar(centers, attached, bottom=relay, hatch=stack_hatch, **bar_style)

    ax.set_xticks(x, [f"{k} shards\n{r:,} tx/s" for k, r in settings])
    ax.set(xlim=(-0.48, 3.48), ylim=(0, 50))
    ax.set_ylabel("Extra Execution Gas")
    ax.text(0.0, 1.012, "1e9", transform=ax.transAxes, ha="left", va="bottom",
            fontsize=12.2, color="#252525")
    ax.yaxis.set_major_locator(MultipleLocator(10))
    ax.tick_params(axis="x", length=0, pad=5)
    ax.tick_params(axis="y", direction="out")
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)

    method_handles = [Patch(facecolor=color, edgecolor=ink, linewidth=0.7, label=label)
                      for _, label, color in methods]
    part_handles = [
        Patch(facecolor="white", edgecolor=ink, linewidth=0.7, label="Repeated Relay Base Gas"),
        Patch(facecolor="white", edgecolor=ink, linewidth=0.7, hatch=stack_hatch,
              label="Internal Cross-shard Call Gas"),
    ]

    ax.legend(handles=method_handles + part_handles, loc="upper left", ncols=3, fontsize=8.4,
              frameon=True, facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
              borderpad=0.35, labelspacing=0.3, handlelength=1.25, handleheight=0.85,
              handletextpad=0.45, columnspacing=0.75, borderaxespad=0.4)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e3_d_extra_execution_gas.{ext}", dpi=600, facecolor="white")
