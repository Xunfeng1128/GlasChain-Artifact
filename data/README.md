# Ethereum trace for Section VI

E1–E6 use ordered prefixes of the same two CSVs. The experiment runner expects these files under `datasets/icde_e2_1000001_4000000/` by default.

| File | Data rows | Bytes | SHA-256 |
| --- | ---: | ---: | --- |
| `transactions.csv` | 3,000,000 | 852,791,912 | `0bb7125e6776b8a2e66da951338a05bea075b7bfd9d0017f816bdaea49665949` |
| `contract_calls.csv` | 16,043,041 | 4,575,175,173 | `d9dd96caeb98da7d36274489e3407e512942878eb9520aa138b251d5f9ebd92b` |

The call CSV includes 3,000,000 root calls and 13,043,041 internal calls. The CSV pair occupies about 5.43 GB uncompressed. The [selection manifest](../datasets/icde_e2_1000001_4000000/selection.json) records counts, hashes and boundaries.

## Exact input

Use an archived copy of the formal CSV pair if available. Preserve the CSV bytes and run the following from the repository root. An immutable public URL or DOI for the archived files is still pending.

```sh
python3 data/trace.py verify
```

An archived copy of the original five-million-transaction trace also suffices. Use the source filenames shown above, either as plain CSV or as `.csv.gz`. The [source manifest](source_5m.json) pins the plain CSV bytes. The selector verifies the source and outputs before placing the formal pair at the default path.

```sh
python3 data/trace.py select --source /path/to/trace-5m
python3 data/trace.py verify
```

Add `--check-only` to validate the full source and derived slice without writing output. Add `--directory /path/to/formal-trace` to `verify` to check a pair in place. Verification and selection stream the CSVs with Python's standard library. The binary-preserving selector copies the original headers and LF-terminated records in order. It does not rewrite fields or renumber the source trace's 50,000-transaction `epochId` windows. These are dataset labels and are independent of emulator reconfiguration epochs.

The parent trace takes the first 5,000,000 eligible transactions in ascending `(block_number, transaction_index, transaction_hash)` order. Eligibility requires a successful receipt, non-null endpoints, a non-creation transaction and distinct sender and recipient. Candidate blocks are 25,755,293 through 25,775,292 inclusive. The parent actually spans blocks 25,755,293 through 25,769,799. The starting point was chosen after inspecting historical relationship reuse.

The formal trace takes parent positions **1,000,001 through 4,000,000**, inclusive and one-based with the header excluded. The first transaction is block **25,757,553**, transaction index **60** at **2026-08-15 02:47:35 UTC**, hash `0xf69137ef6690394952ff9a6aec8cc03c14ea07a436e3d154a6dfac6dcd35a7a4`. The last is block **25,766,189**, index **77** at **2026-08-16 07:41:47 UTC**, hash `0xd9cc7213134020539c52a6ffd42490fd032dfeb5f217d3645fddbe633cc6326d`. The call selection uses the same inclusive block and index boundaries. The query joins calls by selected transaction hash and block and orders them by transaction position, call depth and numeric trace path. It includes root calls and local trace failures with valid endpoints. The successful internal contract-to-contract subset is marked with `isCCGEdge`. Top-level Gas comes from receipts.

## Reconstruct from public BigQuery tables

The source is Google's US Ethereum mainnet dataset `bigquery-public-data.goog_blockchain_ethereum_mainnet_us`. The included SQL uses its `transactions`, `receipts`, `blocks`, `traces` and `accounts` tables. The dataset reference and REST paging behavior are documented by Google. [Ethereum Mainnet schemas](https://docs.cloud.google.com/blockchain-analytics/docs/schema#ethereum-mainnet) · [BigQuery query result pagination](https://docs.cloud.google.com/bigquery/docs/paging-results)

Use a billing-enabled Google Cloud project, access to the public tables, the BigQuery API, the `gcloud` and `bq` command-line tools, Python 3.10 or later, and a new US staging dataset that you can write to. The query cap is per job. Use an appropriate cap for your billing project. The historical materialization and audit together processed approximately 437 GB. Allow about 15 GB of local space for both plain CSV pairs.

```sh
gcloud auth login
PROJECT=your-billing-project
STAGING=${PROJECT}.glaschain_trace
bq --location=US mk --dataset "$STAGING"

# Estimate the first query, without creating a table.
python3 data/fetch_bigquery.py --project "$PROJECT" --staging "$STAGING" \
  --step selection --dry-run

# Run all materialization and ordered export steps.
python3 data/fetch_bigquery.py --project "$PROJECT" --staging "$STAGING" \
  --step all --maximum-bytes-billed 600000000000 --output data/work/trace-5m
python3 data/trace.py select --source data/work/trace-5m
python3 data/trace.py verify
```

The SQL runs selection, account-classification repair, calls, final transactions, and the two ordered exports, in that order. Staging tables expire after seven days. The two exporters stream explicitly ordered BigQuery query results using REST pagination, one header and LF endings. Use `--step STEP --print-sql` to inspect the rendered SQL offline. For an interrupted export, move its partial file out of the output directory, then rerun the corresponding `--step export-transactions` or `--step export-calls` with `--result-job JOB_ID` if the BigQuery result is still available. The tool prints job IDs and stores no credentials.

The BigQuery rebuild depends on the provider's live tables. The extraction normalized empty account bytecode, used the current account directory for account classification, and retained provider trace error text. Provider updates can change those fields even for the same historical blocks. The exporter checks archived bytes, counts and hashes and reports any mismatch. The archived CSV pair is needed for a byte-identical replay if the public query produces a different snapshot. Local verification matched both archived source files and both exact derived files. Live BigQuery jobs were outside the local verification.
