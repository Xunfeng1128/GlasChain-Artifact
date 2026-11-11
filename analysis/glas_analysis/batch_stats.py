"""Backlog and Gas aggregation from the validated full-run ledger.

Adapted from the formal E3/E4 extended statistics implementation.
"""
import hashlib

NS = 1_000_000_000


def required(condition, message):
    if not condition:
        raise ValueError(message)


def source_digest(db):
    # Cross-method evidence independent of dispatch order, shard and stage.
    h = hashlib.sha256()
    for (source,) in db.execute("SELECT source FROM injected ORDER BY source"):
        h.update(source.encode("ascii") + b"\n")
    return h.hexdigest()


def logical_backlog(db, count, last_send, last_final):
    """Stream merged event times; memory depends on elapsed seconds, not N.

    A grid timestamp coinciding with events receives the state after every event
    at that timestamp. Only the final maximum's first timestamp is retained as
    an extra point, so a long sequence of increasing backlogs stays bounded.
    """
    required(count > 0 and 0 <= last_send <= last_final, "invalid logical-backlog cohort or endpoints")
    sql = """
        SELECT elapsed, sum(sent), sum(finished) FROM (
            SELECT elapsed, 1 AS sent, 0 AS finished FROM injected
            UNION ALL
            SELECT elapsed, 0 AS sent, 1 AS finished FROM stages
            WHERE stage IN ('LOCAL','RELAY2')
        ) GROUP BY elapsed ORDER BY elapsed
    """
    samples = {}
    injected = completed = next_grid = 0
    peak = -1
    peak_row = None
    first_time = last_time = None
    observed_last_send = observed_last_final = None

    def snapshot(t):
        return (t, injected, completed, injected - completed)

    for t, sent, finished in db.execute(sql):
        required(0 <= t <= last_final, "logical event outside the complete-run interval")
        if first_time is None:
            first_time = t
        # Points strictly before t hold the previous post-event state.
        while next_grid < t:
            samples[next_grid] = snapshot(next_grid)
            next_grid += NS
        injected += sent
        completed += finished
        backlog = injected - completed
        required(0 <= completed <= injected <= count, "negative backlog or source count overflow")
        if sent:
            observed_last_send = t
        if finished:
            observed_last_final = t
        current = snapshot(t)
        if backlog > peak:
            peak = backlog
            peak_row = current
        if next_grid == t:
            samples[t] = current
            next_grid += NS
        if t in (last_send, last_final):
            samples[t] = current
        last_time = t

    required(first_time == 0, "logical backlog must start at the first actual dispatch at zero")
    required(observed_last_send == last_send and observed_last_final == last_final and last_time == last_final,
             "logical event endpoints differ from the full-run summary")
    required(injected == completed == count and injected - completed == 0,
             "logical backlog requires every configured source to finish")
    required(peak_row is not None, "no logical backlog events")
    samples[peak_row[0]] = peak_row
    rows = [dict(elapsed_seconds=t / NS, injected_unique=sent,
                 completed_unique=finished, logical_backlog=backlog,
                 last_injection=int(t == last_send), last_confirmation=int(t == last_final),
                 exact_peak=int(t == peak_row[0]))
            for t, sent, finished, backlog in (samples[t] for t in sorted(samples))]
    required(rows[-1]["logical_backlog"] == 0, "final logical backlog is not zero")
    return rows, peak, peak_row[0]


def gas_tables(db, n_shards, count, base_gas):
    by_shard = {}
    sql = """
        SELECT shard, count(*), exact_sum(gas), exact_sum(attached),
               exact_sum(CASE WHEN stage='RELAY1' THEN base ELSE 0 END)
        FROM stages GROUP BY shard
    """
    for shard, stages, gas, attached, relay in db.execute(sql):
        by_shard[shard] = stages, int(gas), int(attached), int(relay)
    total = sum(values[1] for values in by_shard.values())
    extra = total - base_gas
    # Cost conserved: RELAY1 is the duplicate physical base charge of a
    # cross-shard source; attached Gas is already contained in stage_gas.
    required(extra == sum(attached + relay for _, _, attached, relay in by_shard.values()),
             "extra Gas differs from repeated relay base plus attached Gas")
    rows = []
    for shard in range(n_shards):
        stages, gas, attached, relay = by_shard.get(shard, (0, 0, 0, 0))
        rows.append(dict(shard_id=shard, physical_stages=stages, execution_gas=gas,
                         attached_gas=attached, relay1_base_gas=relay, share=gas/total if total else None))
    maximum = max(row["execution_gas"] for row in rows)
    system = dict(completed=count, base_gas=base_gas, total_execution_gas=total, extra_execution_gas=extra,
                  extra_gas_per_completed=extra/count, mean_shard_gas=total/n_shards, max_shard_gas=maximum,
                  max_to_mean_shard_gas=maximum*n_shards/total if total else None)
    return rows, system
