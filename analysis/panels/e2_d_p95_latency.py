"""Panel code from e2/e2_d_p95_latency.ipynb."""

from pathlib import Path

from e2_values import settings_rows

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator

data = settings_rows("p95_latency_s")
latency = np.array([row[2:] for row in data])
methods = [
    ("GlasChain", "#EE6677", "o", "-"),
    ("CLPA", "#0072B2", "s", "--"),
    ("Contribchain-P", "#009E73", "^", ":"),
]

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")

with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.005, 3.28))
    fig.subplots_adjust(left=0.171828, right=0.971029, bottom=0.198170, top=0.954268)
    x = np.arange(len(data))
    for i, (method, color, marker, linestyle) in enumerate(methods):
        ax.plot(x, latency[:, i], label=method, color=color,
                linestyle=linestyle, marker=marker, markersize=4.5,
                linewidth=1.5, markerfacecolor="white", markeredgewidth=1.1)

    ax.set_xticks(x, [f"[{rate}, {shards}]" for shards, rate, *_ in data])
    ax.set_xlabel("[Tx arrival rate, shard count]", fontsize=13.5)
    ax.set_ylabel("P95 confirmation latency (sec)", fontweight="bold")
    ax.set_ylim(0, 500)
    ax.yaxis.set_major_locator(MultipleLocator(100))
    ax.tick_params(direction="out")
    ax.tick_params(axis="x", labelsize=11)
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)
    ax.legend(loc="upper right", facecolor="white", edgecolor="0.8", framealpha=1)

    out = project / "outputs/e2/e2_d_p95_latency"
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e2_d_p95_latency.{ext}", dpi=600, facecolor="white")
