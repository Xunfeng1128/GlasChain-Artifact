"""Panel code from e5/e5_d_max_shard_extra_gas.ipynb."""

from hashlib import sha256
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
import numpy as np
import pandas as pd

project = Path(".") if Path("styles/paper.mplstyle").is_file() else Path("..")
reference = project / "outputs/e5/e5_a_shard_gas_distribution"
source = reference / "figure_data.csv"
validation = json.loads((reference / "validation.json").read_text(encoding="utf-8"))
assert sha256(source.read_bytes()).hexdigest() == validation["figure_data_sha256"]

shards = pd.read_csv(source)
shards["base_gas"] = shards.execution_gas - shards.relay1_base_gas - shards.attached_gas
assert len(shards) == 192

methods = {
    "glas": ("GlasChain", "#F88F7E"),
    "clpa": ("CLPA", "#7AB4DA"),
    "plouvain": ("Contribchain-P", "#6CCDB4"),
    "hash": ("Hash", "#F5D95C"),
}
settings = [(8, 1000), (8, 2000), (16, 1000), (16, 2000)]
groups = shards.groupby(["shards", "inject_tps", "method"])
ink = "#252525"
colors = [colour for _, colour in methods.values()]
x = np.array([0.0, 0.96, 1.96, 3.02])
positions = (x[:, None] + 0.192 * (np.arange(4) - 1.5)).ravel()
legend_style = dict(loc="upper left", ncols=2, fontsize=8.9, frameon=True,
                    facecolor="white", edgecolor="#9a9a9a", framealpha=1.0,
                    borderpad=0.35, labelspacing=0.3, handlelength=1.25,
                    handleheight=0.85, handletextpad=0.45, columnspacing=0.75,
                    borderaxespad=0.4)
method_handles = [Patch(facecolor=colour, edgecolor="none", linewidth=0.0, label=label)
                  for label, colour in methods.values()]

out = project / "outputs/e5" / "e5_d_max_shard_extra_gas"
out.mkdir(parents=True, exist_ok=True)

rows = []
for k, rate in settings:
    for method in methods:
        run = groups.get_group((k, rate, method))
        base = run.base_gas.to_numpy()
        extra = (run.relay1_base_gas + run.attached_gas).to_numpy()
        highest = extra.max()
        heaviest = run.loc[extra == highest].sort_values("shard_id").iloc[0]
        rows.append({
            "shards": k,
            "inject_tps": rate,
            "method": method,
            "base_gas_cv_pct": 100 * base.std(ddof=0) / base.mean(),
            "max_shard_extra_gas": highest,
            "relay1_base_gas": int(heaviest.relay1_base_gas),
            "attached_gas": int(heaviest.attached_gas),
            "heaviest_shard": int(heaviest.shard_id),
        })
table = pd.DataFrame(rows)
assert len(table) == 16

with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.12, 3.43))
    fig.subplots_adjust(left=0.179688, right=0.960938, bottom=0.180758, top=0.947522)
    relay = table.relay1_base_gas.to_numpy() / 1e9
    calls = table.attached_gas.to_numpy() / 1e9
    ax.bar(positions, relay, width=0.192, color=colors * len(settings),
           edgecolor="none", linewidth=0.0)
    ax.bar(positions, calls, bottom=relay, width=0.192, color=colors * len(settings),
           edgecolor=ink, linewidth=0.0, hatch="////")
    ax.set_ylabel("Extra Execution Gas")
    ax.text(0.0, 1.012, "1e9", transform=ax.transAxes, ha="left", va="bottom",
            fontsize=12.2, color="#252525")
    ax.set_xlim(-0.43, 3.45)
    ax.set_xticks(x, [f"[{k},{rate}]" for k, rate in settings])
    ax.tick_params(axis="both", length=2.2, pad=2.1, direction="out")
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)
    ax.set_axisbelow(True)
    ax.set_ylim(0, 10.6)
    ax.yaxis.set_major_locator(MultipleLocator(2))
    part_handles = [
        Patch(facecolor="white", edgecolor=ink, linewidth=0.6, label="Relay repeat"),
        Patch(facecolor="white", edgecolor=ink, linewidth=0.6, hatch="////",
              label="Internal call"),
    ]
    merged = dict(legend_style)
    merged["ncols"] = 3          # 四种方法 + 两种组成，一行三个
    ax.legend(handles=method_handles + part_handles, **merged)
    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"{"e5_d_max_shard_extra_gas"}.{ext}", dpi=600, facecolor="white")


table.to_csv(out / "figure_data.csv", index=False)
