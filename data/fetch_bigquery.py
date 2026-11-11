#!/usr/bin/env python3
"""Materialize the original 5M SQL pipeline and stream ordered query results."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from trace import CALLS, SOURCE, TRANSACTIONS, Fingerprint

HERE = Path(__file__).resolve().parent
LOCATION = "US"
STEPS = {
    "selection": "01_create_selection_table.sql",
    "account-classification": "11_repair_account_classification.sql",
    "calls": "02_create_contract_calls_table.sql",
    "transactions": "03_create_final_transactions.sql",
    "export-transactions": "13_export_transaction_csv_lines.sql",
    "export-calls": "12_export_call_csv_lines.sql",
}
EXPORTS = {
    "export-transactions": TRANSACTIONS,
    "export-calls": CALLS,
}
HEADERS = {
    TRANSACTIONS: (
        "blockNumber,timestamp,transactionHash,from,to,toCreate,fromIsContract,toIsContract,"
        "value,gasLimit,gasPrice,gasUsed,callingFunction,isError,eip2718type,baseFeePerGas,"
        "maxFeePerGas,maxPriorityFeePerGas,reserved,transactionIndex,receiptStatus,txCategory,"
        "epochId,internalCallCount,contractCallCount,maxCallDepth,ccgVisibleGas,hasCCGEdge,"
        "sumCCGEdgeGasInclusive"
    ),
    CALLS: (
        "epochId,blockNumber,transactionIndex,transactionHash,traceAddress,parentTraceAddress,"
        "tracePathSortKey,callDepth,subtraceCount,caller,callee,callType,functionSelector,"
        "callValue,callGasLimit,callGasUsed,error,txStatus,transactionGasUsed,callerIsContract,"
        "calleeIsContract,isContractToContract,isCCGEdge"
    ),
}


class BigQuery:
    """Small wrapper around the same paginated REST interface used for the original export."""

    def __init__(self, project):
        self.project = project
        self.token = ""
        self.token_time = 0

    def auth(self):
        if not self.token or time.monotonic() - self.token_time > 2400:
            self.token = subprocess.check_output(
                ["gcloud", "auth", "print-access-token"], text=True
            ).strip()
            self.token_time = time.monotonic()

    def api(self, method, route, body=None, **params):
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project}/{route}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = json.dumps(body).encode("utf-8") if body is not None else None
        for attempt in range(6):
            self.auth()
            request = urllib.request.Request(
                url, data=data, method=method,
                headers={
                    "Authorization": "Bearer " + self.token,
                    "Content-Type": "application/json",
                    "Accept-Encoding": "gzip",
                    "User-Agent": "GlasChain artifact (gzip)",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    raw = response.read()
                    if response.headers.get("Content-Encoding") == "gzip":
                        raw = gzip.decompress(raw)
                    return json.loads(raw)
            except urllib.error.HTTPError as error:
                if error.code == 409 and method == "POST" and not body["configuration"]["dryRun"]:
                    return self.get_job(body["jobReference"]["jobId"])
                if error.code == 401:
                    self.token = ""
                elif error.code not in (408, 429, 500, 502, 503, 504) or attempt == 5:
                    message = error.read().decode("utf-8", errors="replace")
                    raise RuntimeError(f"BigQuery HTTP {error.code}: {message}") from error
            except (urllib.error.URLError, TimeoutError) as error:
                if attempt == 5:
                    raise RuntimeError("BigQuery connection failed after retries") from error
            time.sleep(min(2**attempt, 20))
        raise RuntimeError("BigQuery authentication failed after retries")

    def get_job(self, job_id):
        return self.api("GET", f"jobs/{job_id}", location=LOCATION)

    def submit(self, sql, job_id, limit, dry_run):
        config = {
            "dryRun": dry_run,
            "query": {
                "query": sql,
                "useLegacySql": False,
                "maximumBytesBilled": str(limit),
            },
        }
        job = self.api("POST", "jobs", {
            "jobReference": {"projectId": self.project, "jobId": job_id, "location": LOCATION},
            "configuration": config,
        })
        if dry_run:
            return self.check(job)
        print(f"Submitted BigQuery job {job_id}", flush=True)
        while job["status"]["state"] != "DONE":
            time.sleep(4)
            job = self.get_job(job_id)
        return self.check(job)

    @staticmethod
    def check(job):
        status = job.get("status", {})
        if status.get("errorResult"):
            raise RuntimeError(json.dumps(status["errorResult"], ensure_ascii=False))
        return job

    def result_pages(self, job_id):
        params = {"location": LOCATION, "maxResults": 50000}
        while True:
            result = self.api("GET", f"queries/{job_id}", **params)
            if not result.get("jobComplete"):
                time.sleep(2)
                continue
            yield result
            token = result.get("pageToken")
            if not token:
                return
            params["pageToken"] = token


def query(step, staging):
    sql = (HERE / "sql" / STEPS[step]).read_text(encoding="utf-8")
    return sql.replace("@@STAGING@@", staging)


def export(client, job_id, output, name):
    expected = SOURCE["files"][name]
    target = output / name
    part = output / (name + ".part")
    if target.exists():
        raise FileExistsError(f"Refusing to overwrite {target}")
    fingerprint = Fingerprint()
    with part.open("xb") as stream:
        header = (HEADERS[name] + "\n").encode("utf-8")
        stream.write(header)
        fingerprint.add(header)
        last_progress = time.monotonic()
        for result in client.result_pages(job_id):
            if int(result["totalRows"]) != expected["data_rows"]:
                raise ValueError(f"{name}: BigQuery returned {result['totalRows']} rows, "
                                 f"expected {expected['data_rows']}")
            fields = result.get("schema", {}).get("fields")
            if fields and [field["name"] for field in fields] != ["csv_line"]:
                raise ValueError("Unexpected BigQuery export schema")
            for row in result.get("rows", []):
                line = (row["f"][0]["v"] + "\n").encode("utf-8")
                stream.write(line)
                fingerprint.add(line)
            if time.monotonic() - last_progress >= 30:
                print(f"{name}: {fingerprint.rows:,} rows downloaded", flush=True)
                last_progress = time.monotonic()
        stream.flush()
        os.fsync(stream.fileno())
    fingerprint.check(name, expected)
    target.hardlink_to(part)
    part.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="your BigQuery billing project ID")
    parser.add_argument("--staging", required=True, help="your dedicated US dataset as PROJECT.DATASET")
    parser.add_argument("--step", required=True, choices=["all", *STEPS])
    parser.add_argument("--output", type=Path, default=HERE / "work" / "trace-5m")
    parser.add_argument("--maximum-bytes-billed", type=int, default=600_000_000_000,
                        help="BigQuery billing byte cap per query job")
    parser.add_argument("--dry-run", action="store_true", help="estimate one existing SQL step")
    parser.add_argument("--print-sql", action="store_true", help="print rendered SQL without authentication")
    parser.add_argument("--result-job", help="re-download an existing successful export job")
    args = parser.parse_args()

    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", args.project):
        parser.error("invalid Google Cloud project ID")
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]\.[A-Za-z0-9_]+", args.staging):
        parser.error("staging must be a project ID and dataset name separated by a period")
    if args.maximum_bytes_billed <= 0:
        parser.error("maximum bytes billed must be positive")
    if args.step == "all" and (args.dry_run or args.print_sql):
        parser.error("choose an individual step with --dry-run or --print-sql")
    if args.result_job and (args.step not in EXPORTS or args.dry_run or args.print_sql
                            or not re.fullmatch(r"[A-Za-z0-9_-]+", args.result_job)):
        parser.error("--result-job requires an export step and a BigQuery job ID")
    if args.print_sql:
        print(query(args.step, args.staging), end="")
        return

    client = BigQuery(args.project)
    steps = tuple(STEPS) if args.step == "all" else (args.step,)
    if not args.dry_run:
        for step in steps:
            name = EXPORTS.get(step)
            if name and any((args.output / x).exists() for x in (name, name + ".part")):
                parser.error(f"{name} or its .part file already exists in {args.output}")
    try:
        for step in steps:
            name = EXPORTS.get(step)
            if name and not args.dry_run:
                args.output.mkdir(parents=True, exist_ok=True)
            if args.result_job:
                job_id = args.result_job
                client.check(client.get_job(job_id))
            else:
                job_id = f"glaschain_{step.replace('-', '_')}_{time.time_ns()}"
                job = client.submit(query(step, args.staging), job_id,
                                    args.maximum_bytes_billed, args.dry_run)
                stats = job.get("statistics", {})
                print(f"{step}: {stats.get('totalBytesProcessed', 'unknown')} processed bytes", flush=True)
            if name and not args.dry_run:
                export(client, job_id, args.output, name)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
