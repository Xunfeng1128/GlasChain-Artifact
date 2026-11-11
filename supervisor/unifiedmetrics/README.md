# Unified reconfiguration observations

`reconfiguration.csv` is created beside `events.csv` for every unified-metrics
run. Hash has a header-only file. This sidecar does not change the existing
transaction event schema or the first-actual-dispatch time origin.

Each control attempt has a unique `reconfiguration_id`; `target_epoch` is the
epoch the existing algorithm attempts to enter, not a count of successful
reconfigurations. A skipped GLAS attempt can share a target epoch with a later
attempt. The rows are:

- `trigger`: the supervisor enters its existing synchronous reconfiguration
  path after the due-condition check; it is the actual check time, not the
  nominal 48-second deadline.
- `injection_pause_begin`: the same supervisor control path stops dispatching
  new top-level input while more input remains. This does **not** assert that
  workers are frozen or that earlier asynchronous sends have reached workers.
- `finish`: that control attempt returns, with `completed`, `skipped` (GLAS
  work drained during the delivery barrier), or `failed` status.
- `injection_pause_end`: the supervisor can continue input dispatch after a
  completed/skipped control attempt. The next actual send can occur later;
  its timestamp remains in `events.csv`. A failure has no resume event.

`phase=draining` means no new input remains; these attempts have trigger/finish
rows only and must not be added to injection-pause totals. Abrupt termination or
panic can leave an unfinished attempt and unclosed run metadata. Do not invent
a finish timestamp for it.

`scope=supervisor_control` applies to every row. `completion_basis` makes the
existing methods' distinct completion conditions explicit:

- `observed_new_epoch_block`: CLPA/CLPA-Gas/P-Louvain/P-Louvain-Gas/TxAllo resume
  when their existing `curEpoch` wait is satisfied by a new-epoch block receipt.
  That is not proof that every shard or replica has resumed.
- `observed_new_epoch_block_all_shards`: GARET resumes only after the supervisor
  observes a block receipt at or beyond the target epoch from every shard.
  This describes the supervisor's completion condition, not a simultaneous
  resume timestamp for all worker replicas.
- `glas_start_barrier`: GLAS returns after its existing start acknowledgments,
  running-phase transition and round-metric writes.

After first injection, `elapsed_ns` uses the same supervisor monotonic clock as
`events.csv`. Before the origin is available, it is **empty**,
`clock_phase=before_first_injection`, and only `observed_unix_ns` is populated.
These diagnostic rows cannot be treated as zero-time running-window events.
They never start/reset the transaction clock, including when a run closes before
any injection. If a timestamp captured before injection is written after the
origin is established, its relative elapsed can be negative.

For a retrospective control-epoch cutoff, select the successful `finish` row
for the chosen target epoch and use its elapsed time. This is a supervisor
control boundary, not a global worker execution-epoch boundary. Hash has no
dynamic control epochs; compare all methods at a common elapsed-time cutoff.

Additive `run.json` fields `reconfiguration_schema_version=1` and
`reconfiguration_rows` describe this sidecar. Old run files without it remain
valid inputs to existing transaction analysis. All control writes are checked
and flushed; any failure prevents a clean `closed` metadata status.
