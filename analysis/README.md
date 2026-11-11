# Section VI analysis and figures

The reference tables support local figure reproduction with Python 3.13.
They contain aggregate measurements, run identifiers and provenance hashes.
The plotting programs are the code cells of the 24 original panel notebooks.
They use the reference tables, generate the original panel types and assemble
the paper figures. The E2 panels read the verified table directly. Jupyter is
optional because the artifact contains headless Python entry points.

From the repository root:

```bash
python3.13 -m venv .venv
.venv/bin/python -m pip install -r analysis/requirements.txt
.venv/bin/python analysis/render_figures.py --out results/figures
```

To render a subset, pass `--experiments e1 e2`. The command uses Times New Roman
if installed and DejaVu Serif otherwise. The optional `--font` selects another
installed font family. All figure values use the same source data whichever
font is selected. PDFs, SVGs and 600 dpi PNGs are generated for the figures and
individual panels. The generated `figure_sources.json` records panel hashes.
PDF IDs and font output can vary across environments.

## Mapping to the paper

Table II in Section VI describes the experimental matrix. The main README
lists the run configurations and historical variant details. The figure names
below are the same as in the English manuscript.

| Experiment | Generated PDF | Input under `analysis/reference/` |
| --- | --- | --- |
| E1 | `fig_e1_allocation.pdf` | `e1/e1_statistics/` contains 40 run summaries, 4,272 epoch rows and 40 maximum shard processing times. |
| E2 | `fig_e2_end_to_end.pdf` | `e2/e2_overview/figure_data.csv` contains 15 method-by-setting rows, including sample sizes and hashes for the matched transactions. |
| E3 | `fig_e3_cross_shard_and_gas.pdf` | `e3/e3_[a-d]_*/figure_data.csv` contains cross-shard counts, category composition, Gas-weighted crossing and extra Gas. |
| E4 | `fig_e4_backlog_and_drain.pdf` | `e4/e4_backlog_drain/{figure_data,peak_and_drain}.csv` contains 44,554 curve samples and 16 drain records. |
| E5 | `fig_e5_shard_gas.pdf` | `e5/e5_a_shard_gas_distribution/figure_data.csv` contains 192 shard records. `e5/e5_b_max_shard_execution_gas/figure_data.csv` contains 16 maxima. Panels (c) and (d) derive from the 192 shard records. |
| E6 | `fig_e6_shard_completion.pdf` | `e6/e6_shard_completion_time/figure_data.csv` contains 192 shard records, with the last execution-stage receipt as the plotted value. |

E3–E6 use the same 16 complete, 500,000-transaction runs. E4 samples the
logical backlog on a one-second grid and also records the first exact peak,
last dispatch and final confirmation. Its published lines use shape-preserving
interpolation between 24-second grid points and those three event points.
The four panels use the published x-axis limits per setting. All have the
same y-axis scale.

E1 uses `LOCAL + (RELAY1 + RELAY2)/2` per epoch. The weighted TPS divides
the total count by the sum of nonempty block spans. E2 throughput counts final
confirmations in the first 600 seconds since actual dispatch began.
Confirmation rate divides by the number actually dispatched in that window.
For the latency panels, the sample is the earliest hashes in input order
among the transactions confirmed by all three methods within their respective
600-second windows. P95 uses the nearest rank.

## Derive new summaries from run logs

The formal runner emits one directory per condition and repeat, containing
`run_state.json`, `result/unified_metrics/{run.json,events.csv}` and its
validation and summary files. Use the E1/E2 statistics commands in the main
README for those two cohorts. For the common E3–E6 cohort:

```bash
python3.13 analysis/rebuild_e3_e6.py --runs runs/e3 --out results/tables
.venv/bin/python analysis/render_figures.py --sources results/tables \
    --experiments e3 e4 e5 e6 --out results/figures
```

Use `runs/e3` for the default E3 runner output. A copy of the historical
archive can use its `e3` directory with the `<method>/<condition>/r01`
layout. Discovery uses the E3 run IDs and their configuration metadata.
The extractor requires the sixteen validated runs with one repeat per
condition. It checks artifact hashes, builds a temporary SQLite ledger,
deduplicates stages, and checks that all runs completed the same source
cohort. E6 uses the last valid LOCAL, RELAY1 or RELAY2 receipt for each shard.
The temporary ledger is deleted when processing a run finishes. For an
alternate scratch location, pass `--scratch` with an existing directory.

`reference/provenance.json` gives original and distributed table hashes,
the original notebook and panel export for each panel, and the corresponding
manuscript PDF digest. Paths to historical local files were removed or made
relative. The main figure command verifies all distributed reference hashes
before it plots. The original manuscript asset names are used for the
generated PDFs. The manuscript uses `GlasChain` in the legends, and the
archived source notebooks use `GLAS` for the same method.
