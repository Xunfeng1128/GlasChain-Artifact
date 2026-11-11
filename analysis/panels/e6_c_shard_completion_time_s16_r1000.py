"""Panel code from e6/e6_c_shard_completion_time_s16_r1000.ipynb."""

import csv
import hashlib
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
from matplotlib.lines import Line2D
from matplotlib.ticker import StrMethodFormatter

cwd = Path.cwd()
project = next(p for p in (cwd, *cwd.parents) if (p / "styles/paper.mplstyle").is_file())
data_dir = project / "outputs/e6/e6_shard_completion_time"
STEM = "e6_c_shard_completion_time_s16_r1000"
out = project / "outputs/e6" / STEM
out.mkdir(parents=True, exist_ok=True)

source = data_dir / "figure_data.csv"
source_validation = data_dir / "source_validation.json"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


provenance = json.loads(source_validation.read_text(encoding="utf-8"))
assert provenance["valid"]
assert sha256(source) == provenance["figure_data_sha256"]
assert provenance["runs_checked"] == 16
assert provenance["shard_rows"] == 192
assert provenance["independent_runs_per_method_per_setting"] == 1
assert provenance["stage_filter"] == ["LOCAL", "RELAY1", "RELAY2"]

with source.open(newline="", encoding="utf-8") as stream:
    rows = list(csv.DictReader(stream))

methods = [
    ("glas", "GlasChain", "#0072B2", "o"),
    ("clpa", "CLPA", "#D55E00", "^"),
    ("plouvain", "Contribchain-P", "#009E73", "D"),
    ("hash", "Hash", "#E69F00", "s"),
]
settings = [(8, 1000), (8, 2000), (16, 1000), (16, 2000)]
SHARDS, RATE = 16, 1000
data = {
    (int(row["shards"]), int(row["inject_tps"]), row["method"], int(row["shard_id"])): row
    for row in rows
}
expected = {
    (shards, rate, method, shard_id)
    for shards, rate in settings
    for method, *_ in methods
    for shard_id in range(shards)
}
assert len(data) == len(rows) == provenance["shard_rows"] and set(data) == expected

times = {}
for key, row in data.items():
    assert int(row["physical_stages"]) > 0
    assert int(row["last_stage_elapsed_ns"]) >= int(row["last_final_elapsed_ns"]) >= 0
    times[key] = int(row["last_stage_elapsed_ns"]) / 1e9

ink = "#252525"
upper = max(4500, 500 * (int(max(times.values())) // 500 + 1))

with plt.style.context(project / "styles/paper.mplstyle"):
    fig, ax = plt.subplots(figsize=(5.12, 3.43))
    fig.subplots_adjust(left=0.179688, right=0.960938, bottom=0.180758, top=0.947522)

    for method, _, color, marker in methods:
        positions = list(range(SHARDS))
        values = [times[SHARDS, RATE, method, shard_id] for shard_id in positions]
        # Larger Hash squares sit behind nearby marks on the same shard line.
        ax.scatter(positions, values, s=26 if method == "hash" else 16,
                   marker=marker, facecolors=to_rgba(color, 0.78), edgecolors=ink,
                   linewidths=0.4, zorder={"hash": 3, "clpa": 5}.get(method, 4))

    ax.set_xlabel("Shard ID", fontsize=12.2, labelpad=3)
    ax.set_ylabel("Shard completion time (sec)")
    ax.set_xticks(range(SHARDS), minor=True)
    ax.set_xticks(range(SHARDS) if SHARDS == 8 else range(0, SHARDS, 2))
    ax.set_xlim(-0.55, SHARDS - 0.45)
    ax.set_ylim(0, upper)
    ax.set_yticks(range(0, 4001, 1000))
    ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    ax.tick_params(axis="both", direction="out", length=2.4, labelsize=10.8, pad=2.5)
    ax.tick_params(axis="x", which="minor", length=0)
    ax.set_axisbelow(True)
    ax.grid(axis="both", color="#DCDCDC", linewidth=0.6, linestyle='-', zorder=0)

    handles = [
        Line2D([], [], color=to_rgba(color, 0.78), marker=marker, markeredgecolor=ink,
               markeredgewidth=0.4, markersize=4.8, linestyle="None", label=label)
        for _, label, color, marker in methods
    ]
    ax.legend(handles=handles, loc="upper right", ncols=2, fontsize=8.9, frameon=True,
              framealpha=1, facecolor="white", edgecolor="#C9C9C9", handlelength=0.8,
              handletextpad=0.45, borderpad=0.3, labelspacing=0.22, columnspacing=0.85,
              borderaxespad=0.4)

    for ext in ("pdf", "svg", "png"):
        fig.savefig(out / f"{STEM}.{ext}", dpi=600, facecolor="white")
