"""Panel code from e5/e5_b_max_shard_execution_gas.ipynb."""

import csv
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb
from matplotlib.ticker import MultipleLocator
import numpy as np

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")
out = project / "outputs/e5/e5_b_max_shard_execution_gas"
maxima = {}
with (out / "figure_data.csv").open(newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        setting = (int(row["shards"]), int(row["inject_tps"]), row["method"])
        assert setting not in maxima, setting
        maxima[setting] = int(row["max_shard_gas"])

# Shared palette for E4-a through E4-d.
methods = [
    ("glas", "GlasChain", "#F88F7E"),
    ("clpa", "CLPA", "#7AB4DA"),
    ("plouvain", "Contribchain-P", "#6CCDB4"),
    ("hash", "Hash", "#F5D95C"),
]
settings = [(8, 1000), (8, 2000), (16, 1000), (16, 2000)]
assert set(maxima) == {(k, r, key) for k, r in settings for key, _, _ in methods}


def darken(hex_color, factor=0.55):
    return tuple(c * factor for c in to_rgb(hex_color))


x = np.arange(len(settings))
label_ink = "#333333"
with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.12, 3.43))
    fig.subplots_adjust(left=0.179688, right=0.960938, bottom=0.180758, top=0.947522)
    for index, (key, label, color) in enumerate(methods):
        ax.bar(
            x + (index - 1.5) * 0.217,
            [maxima[k, r, key] / 1e9 for k, r in settings],
            width=0.217, color=color, edgecolor="none", linewidth=0.0, label=label,
        )
    
    ax.set_xticks(x, [f"[{k},{r}]" for k, r in settings])
    ax.set(xlim=(-0.48, 3.48), ylim=(0, 16.4))
    ax.set_ylabel("Max Shard Execution Gas")
    ax.text(0.0, 1.012, "1e9", transform=ax.transAxes, ha="left", va="bottom",
            fontsize=12.2, color="#252525")
    ax.yaxis.set_major_locator(MultipleLocator(2))
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(axis="x", length=0, pad=5)
    ax.tick_params(axis="y", direction="out")

    ax.legend(loc="upper left", ncols=2, fontsize=8.9,
              frameon=True, facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
              borderpad=0.35, labelspacing=0.3, handlelength=1.4, handleheight=0.85,
              handletextpad=0.45, columnspacing=0.75, borderaxespad=0.4)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e5_b_max_shard_execution_gas.{ext}", dpi=600, facecolor="white")
