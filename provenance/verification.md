# Local verification

| Check | Result |
| --- | --- |
| Go 1.26.1 on macOS ARM64 | Emulator build and retained package tests passed. The historical fixed-window E2 baseline profile also built locally. |
| Python standard-library tests | Unified metrics, workload preflight, and launch suites passed. |
| Synthetic run | Two shards with four replicas each. All 480 source transactions confirmed and three reconfigurations finished in the drain run. A separate synthetic fixed-window run sealed exactly at its eight-second cutoff and passed E2 validation. |
| Formal planner after cleanup | Six before/after fixtures agreed on candidate order, selected placement, migrated accounts and Gas components. |
| Formal configuration plan | 75 distinct conditions. All 76 archive routes and the shared E1/E3 run are in the inventory. |
| Preflight against the actual trace | Input hashes and executable workload checks passed for early GlasChain with 500,000 transactions, GARET-FS with 500,000 transactions and the historical fixed-window Hash profile with 3,000,000 transactions. |
| Exact input extraction | Streamed the local five-million-transaction source and verified both the source and the derived three-million-transaction slice against their recorded SHA-256 values. The BigQuery SQL and exporter were exercised offline. |
| E1 metrics from the archived runs | Spot-checked CLPA at 16 shards/2M, the shared GlasChain run at 16 shards/500k, and the recovered TxAllo analysis at 8 shards/2M. Recomputed values matched the packaged figure tables. |
| E2 metrics from the archived runs | Recomputed all three methods at 4 shards. Dispatch and completion counts, throughput, confirmation rate, mean, P95 and the selected 50,000-transaction hash list matched the packaged figure table. |
| E3–E6 log processing | The extractor was exercised with the synthetic ledger and the completed smoke-run ledger. Its formal cohort check uses 500,000 transactions per run. |
| Figures 7–12 | Rendered all six headlessly using the pinned dependency versions and both supported font paths. With Times New Roman, page dimensions and extracted text matched the manuscript assets. The reference-table file checksums were validated before plotting. |

Formal performance workloads were inspected through their archived records and preflighted locally. The large formal matrix was not rerun during artifact preparation. A full dependency download in an empty Python environment remains unverified. The renderer was exercised with the exact versions in the pinned requirements.
