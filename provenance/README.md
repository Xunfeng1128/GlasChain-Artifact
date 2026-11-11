# Formal experiment provenance

The run inventory in [formal-runs.json](formal-runs.json) describes 76 archive entries representing 75 distinct runs. It records the original configuration and executable digests, normalized configuration paths and digests, input digests, recorder settings and outcome, and hashes of the archived metric inputs. The source archive index has SHA-256 `112193ed2f8ff752399c091ba0031481841d6553f4e971b2f30ce275fff42284`. The original configurations included machine-specific paths, so the portable copies have different digests. A logged run is the independent unit. Each method and setting has one run.

| Paper section | Run matrix | Source of published metrics |
| --- | --- | --- |
| E1 | CLPA, ContribChain-P, TxAllo, GARET-FS and GlasChain. At 8 shards use 1,500 tx/s and at 16 shards use 2,000 tx/s. At both sizes use 0.5, 1.0, 1.5 and 2.0 million transactions. | Full drain, block-span TPS and cross-shard metrics. The archived midpoint mapping also supports the fixed 50,000-transaction evaluation. |
| E2 | Hash, CLPA, ContribChain-P and GlasChain. The five shard count/rate pairs are 4/1,000, 8/1,500, 16/2,000, 24/2,500 and 32/3,000. Each run offers the common 3-million-transaction input. | First-dispatch-relative throughput and confirmation statistics at the windows specified for each paper panel. The stop policy varied historically as detailed below. |
| E3–E6 | Hash, CLPA, ContribChain-P and GlasChain at 8 and 16 shards, each at 1,000 and 2,000 tx/s. All 16 runs drain the same 500,000-transaction prefix. | E3 uses cross-shard transactions and extra execution gas. E4 uses backlog and drain. E5 uses per-shard gas. E6 uses per-shard last stage completion. |

The GlasChain run at 16 shards, 2,000 tx/s and 500,000 transactions serves both E1 and E3–E6. It lives at `runs/e3/glas/e3_glas13_s16_r2000_n500000/r01` and is selected by either experiment. The inventory retains both archive routes and the same source run ID. The wrapper also accepts the E1 alias `e1_glas13_s16_r2000_n500000`.

All configurations use four nodes per shard, a 30,000,000 gas block limit, an 8-second nominal block interval, a 48-second reconfiguration interval and a shared 3,000-gas charge per cross-shard call. The transaction batch size matches the target injection rate. E1 and E3–E6 complete the input. The 48-second runner summary for those groups is an auxiliary validation window.

## Executable versions and parameters

| Archived executable SHA-256 prefix | Archive entries | Source evidence and artifact profile |
| --- | ---: | --- |
| `8a506136318f` | 17 GlasChain | The formal deployment pins `9961f01299f500450fe24ac5db92723976b52dde`. The archived runner and E2 analyzer hashes match that revision. The artifact builds the filtered source as profile `formal`. |
| `14e357714d6a` | 6 E2 baselines | The historical fixed-window snapshot has the archived runner and E2 analyzer hashes. Profile `archived-e2-fixed` ports its baseline stop and log-validation behavior into an isolated copy of the shared runtime. |
| `2e8f98534324` | 4 E1 baselines | The CLPA and ContribChain-P rerun source pins `e87a012f472959ebe48f2e60a5d1510e327814cd`, whose runner digest matches. The two GARET-FS entries have the same archived executable fingerprint. Profile `archived-baseline` selects their recorded configurations on the artifact's shared runtime. |
| `82eb90172f94` | 44 baselines | The archived runner SHA-256 matches `e87a012f`. The executable has a separate digest and exact source attribution remains open. Profile `archived-baseline` uses their recorded configurations on the shared runtime. |
| `f2f1fd1a51ac` | 5 GARET-FS | The archive records a different executable and runner digest. Exact source attribution remains open. Profile `archived-baseline` retains the recorded 120-second PBFT timeout. |

The archived executable fingerprints identify the original results. Local reruns build the artifact's shared source and use the recorded runtime configurations. The E2 compatibility profile applies a SHA-256-verified patch for cooperative shutdown after 960 seconds and accepts logs sealed after that point. Its TPS calculation still uses the exact 600- and 960-second windows.

- GlasChain uses mode 12, early full-window reporting, local queue-gas windows, a window ratio of 1.3 and immediate migration on activation. Its ID-only early-report variant and local-search option are disabled. The configuration uses six blocks per epoch with no experiment-wide epoch quota. Weighted placement uses objective weight 0.5, Fennel gamma 1.5 and two restarts.
- Hash uses mode 3, the unified relay path. CLPA uses mode 1, transaction-count weights, and the poll/install-clock reconfiguration profile.
- ContribChain-P uses mode 9 and TxAllo uses mode 8. Both use the vendored ContribChain account-partitioning implementations with equal modeled shard performance, beta 2 and at most 70 refinement iterations.
- GARET-FS uses mode 10, 100 equal-size account groups, 8-second gas samples and a 600-second reconfiguration timeout. The PBFT view-change timeout is 30 seconds for the two 500,000-transaction GARET-FS runs and 120 seconds for its six other runs. All other methods use 30 seconds.
- The TxAllo 8-shard, 2-million-transaction recorder closed with all 2 million completions. Its original runner status was failed and the archive selected a validated statistics retry. The inventory identifies the accepted analysis directory and its output digests.

## Historical E2 stop policies

| Shards | Hash | CLPA | ContribChain-P | GlasChain |
| --- | --- | --- | --- | --- |
| 4 | 900,000 prefix | 900,000 prefix | Fixed window | Fixed window |
| 8 | Fixed window | Fixed window | Fixed window | Fixed window |
| 16 | 400,000 prefix | 400,000 prefix | 400,000 prefix | Fixed window |
| 24 | 400,000 prefix | 400,000 prefix | 400,000 prefix | Fixed window |
| 32 | 900,000 prefix | Fixed window | Fixed window | Fixed window |

A prefix run waited for the named leading cohort to complete and for the observation window to be covered. Injection continued according to the common runner. The six historical fixed-window baseline logs sealed at about 961–962 seconds. The five GlasChain logs sealed at exactly 960 seconds. All throughput comparisons use events inside the stated measurement window. See the figure-specific statistical recipes for the latency cohorts.

## Launch commands

From the artifact root, run `python3 scripts/run_paper_experiments.py` to list the full matrix. Choose `--experiment E1` through `--experiment E6` or pass a condition ID. The E3, E4, E5 and E6 selections all resolve to the same 16 run directories. For example

```sh
python3 scripts/run_paper_experiments.py --experiment E2
python3 scripts/run_paper_experiments.py --condition e2_hash_s8_r1500 --check
python3 scripts/run_paper_experiments.py --condition e2_hash_s8_r1500 --execute --cleanup-database
python3 scripts/run_paper_experiments.py --experiment E3 --execute --resume --cleanup-database
```

The check verifies the expected SHA-256 of both trace files and validates the workload through the emulator reader. The execute option writes run metadata, unified event logs and initial summaries below `runs/`. The historical fixed-window and prefix analyzers are selected by their corresponding runtime profile.

The full plan resolved all 75 distinct runs. The input preflight passed for `e3_glas13_s8_r1000_n500000`, `e1_garet_s8_r1500_n500000` and `e2_hash_s8_r1500` using the original trace files. The first two scanned 500,000 admitted transactions each. The fixed-window preflight scanned all 3 million. The fixed-window compatibility build passed with Go 1.26.1 on macOS arm64. Synthetic Go and Python tests exercised cooperative sealing after the cutoff and the historical latency cohort.
