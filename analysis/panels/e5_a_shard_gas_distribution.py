"""Panel code from e5/e5_a_shard_gas_distribution.ipynb."""

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.cbook import boxplot_stats
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator, StrMethodFormatter
import numpy as np

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")
out = project / "outputs/e5/e5_a_shard_gas_distribution"
loads = defaultdict(dict)
with (out / "figure_data.csv").open(newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        setting = (int(row["shards"]), int(row["inject_tps"]), row["method"])
        shard_id = int(row["shard_id"])
        assert shard_id not in loads[setting], (setting, shard_id)
        loads[setting][shard_id] = int(row["execution_gas"])

methods = [
    ("glas", "GlasChain", "#F88F7E"),
    ("clpa", "CLPA", "#7AB4DA"),
    ("plouvain", "Contribchain-P", "#6CCDB4"),
    ("hash", "Hash", "#F5D95C"),
]
settings = [(8, 1000), (8, 2000), (16, 1000), (16, 2000)]
assert set(loads) == {(shards, rate, key) for shards, rate in settings for key, _, _ in methods}

ink = "#252525"
box_width = 0.173
group_positions = np.arange(len(settings)) * 1.38
box_style = {
    "patch_artist": True,
    "manage_ticks": False,
    "medianprops": {"color": ink, "linewidth": 1.0},
    "whiskerprops": {"color": ink, "linewidth": 0.65},
    "capprops": {"color": ink, "linewidth": 0.65},
    "flierprops": {"marker": "o", "markerfacecolor": "none", "markeredgecolor": ink,
                   "markersize": 3, "markeredgewidth": 0.6},
}
glas_boxes = {}

with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.12, 3.43))
    fig.subplots_adjust(left=0.179688, right=0.960938, bottom=0.180758, top=0.947522)
    for group, (shards, rate) in enumerate(settings):
        for index, (key, _, color) in enumerate(methods):
            by_shard = loads[shards, rate, key]
            assert set(by_shard) == set(range(shards)), (shards, rate, key)
            gas = np.array([by_shard[s] for s in range(shards)]) / 1e9
            center = group_positions[group] + (index - 1.5) * 0.217
            stats = boxplot_stats(gas, whis=1.5)[0]
            ax.bxp(
                [stats], positions=[center], widths=box_width, showfliers=True,
                boxprops={"facecolor": color, "edgecolor": ink, "linewidth": 0.7},
                **box_style,
            )
            if key == "glas" and shards == 16:
                glas_boxes[rate] = (center, stats, color)


    ax.set_xticks(group_positions, [f"[{k},{r}]" for k, r in settings])
    ax.set(xlim=(-0.48, group_positions[-1] + 0.48), ylim=(0, 15.2))
    ax.set_ylabel("Cumulative Execution Gas")
    ax.text(0.0, 1.012, "1e9", transform=ax.transAxes, ha="left", va="bottom",
            fontsize=12.2, color="#252525")
    ax.yaxis.set_major_locator(MultipleLocator(2))
    ax.tick_params(axis="x", length=0, pad=5)
    ax.tick_params(axis="y", direction="out")
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)

    # Compact local windows beside the corresponding GLAS boxes.
    # Units match the main axis; y ranges differ to resolve each local box.
    zooms = [
        (1000, (2.55, 2.63), (2.55, 2.59, 2.63)),
        (2000, (2.10, 2.90), (2.10, 2.50, 2.90)),
    ]
    for rate, limits, ticks in zooms:
        center, stats, color = glas_boxes[rate]
        inset = ax.inset_axes([center - 0.40, 0.75, 0.235, 1.62], transform=ax.transData)
        inset.bxp(
            [stats], positions=[0], widths=0.66, showfliers=True,
            boxprops={"facecolor": color, "edgecolor": ink, "linewidth": 0.45},
            medianprops={"color": ink, "linewidth": 0.55},
            whiskerprops={"color": ink, "linewidth": 0.45},
            capprops={"color": ink, "linewidth": 0.45},
            flierprops={"marker": "o", "markerfacecolor": "none", "markeredgecolor": ink,
                        "markersize": 2.4, "markeredgewidth": 0.45},
            patch_artist=True, manage_ticks=False,
        )
        inset.set(xlim=(-0.65, 0.65), ylim=limits, xticks=[0], yticks=ticks)
        inset.yaxis.set_major_formatter(StrMethodFormatter("{x:.2f}"))
        inset.tick_params(axis="y", direction="in", length=2.0, pad=1.4, labelsize=6.6)
        inset.tick_params(axis="x", direction="in", length=0, pad=0, labelbottom=False)
        inset.grid(False)

    handles = [Patch(facecolor=color, edgecolor=ink, linewidth=0.7, label=label)
               for _, label, color in methods]

    ax.legend(handles=handles, loc="upper left", ncols=2, fontsize=8.9,
              frameon=True, facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
              borderpad=0.35, labelspacing=0.3, handlelength=1.4, handleheight=0.85,
              handletextpad=0.45, columnspacing=0.75, borderaxespad=0.4)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e5_a_shard_gas_distribution.{ext}", dpi=600, facecolor="white")
