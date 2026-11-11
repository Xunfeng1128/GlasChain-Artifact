"""Panel code from e2/e2_b_confirmation_rate.ipynb."""

from pathlib import Path

from e2_values import settings_rows

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import MultipleLocator, PercentFormatter

data = settings_rows("confirmation_rate")
confirmation_rate = np.array([row[2:] for row in data])
methods = [
    ("GlasChain", "#EE6677", "/"),
    ("CLPA", "#0072B2", "+"),
    ("Contribchain-P", "#009E73", "x"),
]

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")

with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.005, 3.28))
    fig.subplots_adjust(left=0.171828, right=0.971029, bottom=0.198170, top=0.954268)
    x = np.arange(len(data))
    width = 0.22
    for i, (method, color, hatch) in enumerate(methods):
        ax.bar(x + (i - 1) * width, confirmation_rate[:, i], width, label=method,
               color=color, edgecolor="black", linewidth=0.7, hatch=hatch)

    ax.set_xticks(x, [f"[{rate}, {shards}]" for shards, rate, *_ in data])
    ax.set_xlim(-0.55, len(data) - 0.45)
    ax.set_xlabel("[Tx arrival rate, shard count]", fontsize=13.5)
    ax.set_ylabel("Confirmation rate at 600 s (%)", fontweight="bold")
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_locator(MultipleLocator(0.2))
    ax.yaxis.set_major_formatter(PercentFormatter(1, decimals=0, symbol=""))
    ax.tick_params(direction="out")
    ax.tick_params(axis="x", labelsize=11)
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)
    ax.legend(loc="upper left", facecolor="white", edgecolor="0.8", framealpha=1)

    out = project / "outputs/e2/e2_b_confirmation_rate"
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"e2_b_confirmation_rate.{ext}", dpi=600, facecolor="white")
