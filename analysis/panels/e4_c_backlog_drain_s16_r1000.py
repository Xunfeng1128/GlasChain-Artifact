"""Panel code from e4/e4_c_backlog_drain_s16_r1000.ipynb."""

from collections import defaultdict
import csv
from math import ceil
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, StrMethodFormatter
import numpy as np
from scipy.interpolate import PchipInterpolator

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")
data_dir = project / "outputs/e4/e4_backlog_drain"
STEM = "e4_c_backlog_drain_s16_r1000"
out = project / "outputs/e4" / STEM
out.mkdir(parents=True, exist_ok=True)

SHARDS, RATE = 16, 1000

curves = defaultdict(list)
with (data_dir / "figure_data.csv").open(newline="", encoding="utf-8") as source:
    for row in csv.DictReader(source):
        curves[(int(row["shards"]), int(row["inject_tps"]), row["method"])].append(row)

with (data_dir / "peak_and_drain.csv").open(newline="", encoding="utf-8") as source:
    metrics = {
        (int(row["shards"]), int(row["inject_tps"]), row["method"]): row
        for row in csv.DictReader(source)
    }

methods = [
    ("glas", "GlasChain", "#F36958", "-"),
    ("clpa", "CLPA", "#2570CC", (0, (5.5, 2.7))),
    ("plouvain", "Contribchain-P", "#00B995", (1, (4.3, 1.6, 1.1, 1.6))),
    ("hash", "Hash", "#E0A526", (0, (1.6, 2.1))),
]

LEGEND = dict(fontsize=8.9, frameon=True, facecolor="white", edgecolor="#9a9a9a",
              framealpha=1.0, borderpad=0.35, labelspacing=0.3, handlelength=1.6,
              handletextpad=0.45, borderaxespad=0.4)

with plt.style.context(project / "styles/paper.mplstyle"):
    fig = plt.figure(figsize=(5.493, 3.68))
    panels = fig.add_gridspec(
        2, 1, left=0.21118, right=0.93938, top=0.95136, bottom=0.17989,
        height_ratios=(2.2, 0.82), hspace=0.20,
    )
    ax = fig.add_subplot(panels[0])
    drain_ax = fig.add_subplot(panels[1], sharex=ax)
    for i, (key, label, color, linestyle) in enumerate(methods):
        curve = curves[SHARDS, RATE, key]
        seconds = np.array([float(row["elapsed_seconds"]) for row in curve])
        backlog = np.array([int(row["logical_backlog"]) for row in curve]) / 1000
        exact = np.array([
            row["exact_peak"] == "1" or row["last_injection"] == "1"
            or row["last_confirmation"] == "1"
            for row in curve
        ])
        knots = (seconds % 24 == 0) | exact
        trend = PchipInterpolator(seconds[knots], backlog[knots])
        plot_time = np.union1d(np.arange(0, seconds[-1], 2), seconds[knots])
        values = np.maximum(trend(plot_time), 0)
        if key == "glas":

            ax.fill_between(plot_time, values, color=color, alpha=0.18,
                            linewidth=0, zorder=1)
        ax.plot(plot_time, values, label=label,
                color=color, linestyle=linestyle, linewidth=1.2)

        stats = metrics[SHARDS, RATE, key]
        start = float(stats["last_dispatch_seconds"])
        end = float(stats["last_completion_seconds"])
        duration = float(stats["drain_seconds"])
        drain_ax.plot([start, end], [i, i], color=color, linewidth=1.2,
                      solid_capstyle="round")
        drain_ax.plot(start, i, marker="o", markersize=3.5, linestyle="none",
                      markerfacecolor="white", markeredgecolor=color, markeredgewidth=0.9)
        drain_ax.plot(end, i, marker="o", markersize=3.5, linestyle="none", color=color)
        drain_ax.annotate(f"{duration:,.0f} s", ((start + end) / 2, i),
                          xytext=(0, 2.2), textcoords="offset points",
                          ha="center", va="bottom", fontsize=9.5)

    latest = max(float(metrics[SHARDS, RATE, key]["last_completion_seconds"])
                 for key, *_ in methods)
    x_max = ceil(latest * 1.035 / 250) * 250
    ax.set(xlim=(0, x_max), ylim=(0, 450))
    ax.set_ylabel("Queue Size (tx)")
    ax.text(0.0, 1.012, "1e3", transform=ax.transAxes, ha="left", va="bottom",
            fontsize=12.2, color="#252525")
    ax.yaxis.set_major_locator(MultipleLocator(100))
    ax.tick_params(axis="x", bottom=False, labelbottom=False)
    ax.spines[["top", "right"]].set_visible(False)

    drain_ax.set_ylim(3.28, -0.42)
    drain_ax.set_yticks([])   # 方法名已由图例给出，排空行用颜色区分
    drain_ax.tick_params(axis="y", length=0, pad=5, labelsize=10.8)
    drain_ax.spines[["top", "right", "left"]].set_visible(False)
    drain_ax.set_xlabel("Time since first dispatch (sec)")
    drain_ax.xaxis.set_major_locator(MultipleLocator(1000 if x_max > 3500 else 500))
    drain_ax.xaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    for panel in (ax, drain_ax):
        panel.tick_params(direction="out")
        panel.grid(False)
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc="upper right", **LEGEND)


    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"{STEM}.{ext}", dpi=600, facecolor="white")
