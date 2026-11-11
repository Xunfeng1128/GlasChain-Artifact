# GlasChain Artifact

This repository accompanies **GlasChain: Gas-Load-Aware Blockchain Sharding with Cross-Shard Cost Optimization**, prepared for ICDE 2027. It contains the Section VI emulator, formal configurations, the programs that generate the paper metrics and figures, and verified reference tables.

## Quick start

Use a POSIX environment such as Ubuntu or macOS, Git, Go, and Python 3.13. The emulator uses the pinned modules in `go.mod` and `go.sum`. The artifact was built and tested locally with Go 1.26.1. The following commands download Go dependencies and run the self-contained synthetic smoke test on two shards with four replicas per shard.

```sh
go mod download
go build -mod=readonly -trimpath -buildvcs=false -o /tmp/glaschain-emulator .
go test -mod=readonly ./...

python3 scripts/run_experiments.py --manifest configs/smoke/manifest.json --check
python3 scripts/run_experiments.py --manifest configs/smoke/manifest.json \
  --execute --cleanup-database --run-timeout 65
```

The smoke test checks the packaged input hashes and Gas bounds, runs the early full-window GlasChain mode with six-block rounds, and confirms all 480 source transactions. Check `runs/smoke/smoke_glas/r01/analysis/validation.json` and `analysis/summary.json` within that run directory. The formal runner stores database state locally during execution. The cleanup flag removes that run's database after validation. Use `--resume` with `--execute` to reuse a verified local result.

## Recreate the paper figures

The figure inputs under [analysis/reference](analysis/reference/) come from the validated formal runs. They support figure reproduction immediately after cloning.

```sh
python3.13 -m venv .venv
.venv/bin/python -m pip install -r analysis/requirements.txt
.venv/bin/python analysis/render_figures.py --out results/figures
```

The output directory contains the six PDFs below, the corresponding SVG and PNG files, and an index of panel hashes. The renderer reads the original plotting cells and supports an installed Times New Roman or Matplotlib’s bundled DejaVu Serif. See the [figure recipes and reference checksums](analysis/README.md).

| Paper item | Figure or configuration source |
| --- | --- |
| Table II | [Formal configuration matrix and run inventory](provenance/README.md) |
| Figure 7 · E1 allocation performance | `fig_e1_allocation.pdf` from `analysis/reference/e1/e1_statistics/` |
| Figure 8 · E2 end-to-end performance | `fig_e2_end_to_end.pdf` from `analysis/reference/e2/e2_overview/figure_data.csv` |
| Figure 9 · E3 cross-shard transactions and extra Gas | `fig_e3_cross_shard_and_gas.pdf` from `analysis/reference/e3/` |
| Figure 10 · E4 backlog and drain | `fig_e4_backlog_and_drain.pdf` from `analysis/reference/e4/e4_backlog_drain/` |
| Figure 11 · E5 shard Gas | `fig_e5_shard_gas.pdf` from `analysis/reference/e5/` |
| Figure 12 · E6 shard completion | `fig_e6_shard_completion.pdf` from `analysis/reference/e6/e6_shard_completion_time/` |

## Obtain the formal input and run experiments

Use [the data instructions](data/README.md) to obtain and verify both CSVs before launching the formal matrix. All methods use ordered prefixes of the same 3,000,000 transactions. They occupy about 5.43 GB together. The trace spans Ethereum mainnet blocks 25,757,553 through 25,766,189, from transaction index 60 through index 77 inclusive at the respective endpoints. The call file contains 16,043,041 records, including 3,000,000 root calls and 13,043,041 internal calls. The manuscript's current count of 16,043,041 refers to all call records.

```sh
python3 data/trace.py verify
python3 scripts/run_paper_experiments.py
python3 scripts/run_paper_experiments.py --condition e3_glas13_s8_r1000_n500000 --check
python3 scripts/run_paper_experiments.py --condition e3_glas13_s8_r1000_n500000 \
  --execute --resume --cleanup-database
```

The first runner command lists all conditions. Use `--experiment E1`, `--experiment E2`, or `--experiment E3` with `--execute --resume --cleanup-database` to launch a complete group. `--experiment E4`, `E5`, and `E6` select the same runs as E3. Run E3 once to obtain the shared logs. The runner uses local loopback ports and writes to `runs/<group>/<method>/<condition>/r01/`. Allow sufficient disk space for live databases and event logs and enough host resources for 32 shards with four replicas each.

| Group | Formal runs and stop policy |
| --- | --- |
| E1 | 40 results. Five methods at 8 shards and 1,500 tx/s or 16 shards and 2,000 tx/s. Each setting uses 0.5, 1.0, 1.5, or 2.0 million inputs and completes the full batch. |
| E2 | 20 archived runs across 4, 8, 16, 24 and 32 shards at 1,000, 1,500, 2,000, 2,500 and 3,000 tx/s. Figure 8 uses 15 runs from GlasChain, CLPA and ContribChain-P. Hash runs are available in the configuration matrix. Fixed-window and prefix-drain stop policies are recorded per condition. The paper panels use the first 600 seconds after actual dispatch. |
| E3 | 16 results. GlasChain, Hash, CLPA and ContribChain-P at 8 or 16 shards and 1,000 or 2,000 tx/s, each completing the same 500,000 inputs. |
| E4–E6 | Offline statistics from the 16 E3 runs. The 16-shard, 2,000 tx/s GlasChain run is also used in E1. |

The [run inventory](provenance/formal-runs.json) binds each condition to the archive's executable, configuration, input and event fingerprints. The [provenance notes](provenance/README.md) describe the source profiles, the 30/120-second PBFT timeouts, the historical E2 stop policies and the accepted TxAllo statistics recovery. The artifact exposes one GlasChain implementation, based on revision `9961f01299f500450fe24ac5db92723976b52dde`. After the fifth ordinary block, each shard selects a local window with multiplier 1.3 and forecasts the sixth block. The supervisor builds and scores the placement from the complete windows while the shards produce that block. Workers stage the placement, activate it at the six-block boundary, migrate and resume after installation. E1 and E3 drain the workload. E2 seals the observation at 960 seconds after first dispatch.

### Recompute figure inputs from the run logs

After collecting the required runs, use these commands from the repository root. The paths for the E1 and E2 programs can also point to a local copy of the historical archive when passed `--archive`.

```sh
python3 scripts/paper_e1.py --runs runs --out results/tables
python3 scripts/paper_e2.py --runs runs/e2 --out results/tables
python3 analysis/rebuild_e3_e6.py --runs runs/e3 --out results/tables
.venv/bin/python analysis/render_figures.py --sources results/tables --out results/figures
```

E1 weights LOCAL as one and each relay stage as one half. The weighted block-span TPS divides summed stage counts by summed nonempty block spans. Cross-shard ratio uses the union of top-level and effective internal crossings. E2 latency uses the earliest 50k, 100k, 200k, 300k or 400k hashes in input order from the three-method confirmation intersection within the 600-second window. The script computes first dispatch to first LOCAL or RELAY2 receipt, with nearest-rank P95. A rerun needs at least the specified sample size at each operating point. E3–E6 share one validated transaction cohort and use the ledger and stage receipts described in the analysis guide. Timing varies with hardware, system scheduling and the Go toolchain. See [local verification](provenance/verification.md) for the exact checks performed.

## Baseline source

| Method | Artifact implementation |
| --- | --- |
| Hash | Static address partitioning on BlockEmulator's unified relay execution path, mode 3. |
| CLPA | BlockEmulator account allocation with transaction-count weights, mode 1. |
| ContribChain-P | Vendored count-based P-Louvain from the ContribChain implementation, mode 9. Integrated in source revision `5b59d2537e191fd22c0e6f51ab271b51a78c02f9`. |
| TxAllo | Vendored ContribChain TxAllo branch, mode 8. Integrated in source revision `78b54fac4073c26c41546cc27ed324020b3a1799`. |
| GARET-FS | Fixed-account-group implementation in `partition/garet/` and the supervisor, mode 10. The runner builds the group catalog from the exact workload prefix. |
| GlasChain | `GLAS_Pending` mode 12 with the full-window early configuration. The trace parser shared by the emulator is in `internal/tracecsv/`. |

The executable framework is adapted from BlockEmulator. The two ContribChain implementations are included with their original licenses and oracle fixtures that record exact upstream file hashes. The upstream archive lacked a commit ID. This repository uses the vendored sources directly. [BlockEmulator upstream](https://github.com/HuangLab-SYSU/block-emulator) · [ContribChain paper](https://arxiv.org/abs/2505.06899)

## Availability and known gaps

- An immutable public download URL or DOI for the exact 5.43 GB input pair is pending. The public BigQuery extraction recipes use live historical tables. The input hashes identify the exact archived bytes, and the selector can verify and reproduce them from the pinned five-million-transaction parent trace.
- The archived formal results use five executable fingerprints. Exact source attribution is still open for the historical baseline executable identified by `82eb90172f94` and the GARET-FS executable identified by `f2f1fd1a51ac`. Their recorded parameters, artifact hashes and source evidence are in the run inventory. The shared emulator runs the recorded baseline settings. An isolated compatibility patch reproduces the recorded fixed-window E2 baseline stop policy.
- The bundled reference tables support Figure 7–12 reproduction. The archived per-run event logs used to verify those tables are held separately. Local verification covered configuration and data preflight, a synthetic multi-process run, and figure reproduction.

The original BlockEmulator license is in [LICENSE](LICENSE). The vendored ContribChain components retain their [P-Louvain license](partition/plouvain_contrib/LICENSE) and [TxAllo license](partition/txallo/LICENSE).
