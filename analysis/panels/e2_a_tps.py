"""Panel code from e2/e2_a_tps.ipynb."""

from pathlib import Path

from e2_values import settings_rows

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator


data = np.array(settings_rows("confirmed", ("clpa", "plouvain", "glas")))
tps = data[:, 2:] / 600
methods = [
    ("GlasChain", "#EE6677", "/", 2),
    ("CLPA", "#0072B2", "+", 0),
    ("Contribchain-P", "#009E73", "x", 1),
]

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")

with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.005, 3.28))
    fig.subplots_adjust(left=0.171828, right=0.971029, bottom=0.198170, top=0.954268)
    x = np.arange(len(data))
    width = 0.22
    for i, (method, color, hatch, tps_col) in enumerate(methods):
        ax.bar(x + (i - 1) * width, tps[:, tps_col], width, label=method,
               color=color, edgecolor="black", linewidth=0.7, hatch=hatch)

    ax.set_xticks(x, [f"[{rate}, {shards}]" for shards, rate in data[:, :2]])
    ax.set_xlabel("[Tx arrival rate, shard count]", fontsize=13.5)
    ax.set_ylabel("Throughput (TPS)", fontweight="bold")
    ax.set_ylim(0, 1500)
    ax.yaxis.set_major_locator(MultipleLocator(250))
    ax.tick_params(direction="out")
    ax.tick_params(axis="x", labelsize=11)
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", facecolor="white", edgecolor="0.8", framealpha=1)

    out = project / "outputs/e2/e2_a_tps"
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e2_a_tps.{ext}", dpi=600, facecolor="white")
