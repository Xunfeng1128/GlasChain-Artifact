#!/usr/bin/env python3
"""Verify the formal CSV pair or derive it from the ordered five-million trace."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory

DATA = Path(__file__).resolve().parent
FORMAL = DATA.parent / "datasets" / "icde_e2_1000001_4000000"
SOURCE = json.loads((DATA / "source_5m.json").read_text(encoding="utf-8"))
SELECTION = json.loads((FORMAL / "selection.json").read_text(encoding="utf-8"))
TRANSACTIONS = "transactions.csv"
CALLS = "contract_calls.csv"
FILES = (TRANSACTIONS, CALLS)


class Fingerprint:
    def __init__(self):
        self.digest = hashlib.sha256()
        self.rows = -1  # The header is included in the digest but excluded from the count.
        self.size = 0

    def add(self, line: bytes):
        if not line.endswith(b"\n"):
            raise ValueError("Expected an LF-terminated CSV record")
        self.digest.update(line)
        self.rows += 1
        self.size += len(line)

    def check(self, name, expected):
        actual = {"bytes": self.size, "data_rows": self.rows, "sha256": self.digest.hexdigest()}
        if actual != expected:
            raise ValueError(f"{name} fingerprint mismatch\nexpected {expected}\nobserved {actual}")
        print(f"{name}: {self.rows:,} rows, {self.size:,} bytes, SHA-256 {actual['sha256']}", flush=True)


def verify(folder: Path):
    for name in FILES:
        observed = Fingerprint()
        with (folder / name).open("rb") as stream:
            for line in stream:
                observed.add(line)
        observed.check(name, SELECTION["files"][name])


def open_source(folder: Path, name: str):
    csv_path = folder / name
    if csv_path.is_file():
        return csv_path.open("rb")
    return gzip.open(folder / (name + ".gz"), "rb")


def transaction_location(line: bytes):
    fields = next(csv.reader([line.decode("utf-8")]))
    return int(fields[0]), int(fields[19])


def call_location(line: bytes):
    _, block, index, _ = line.split(b",", 3)
    return int(block), int(index)


def select_file(source: Path, name: str, destination: Path | None):
    start = SELECTION["first_source_position"]
    stop = SELECTION["last_source_position"]
    first, last = (tuple(x) for x in SELECTION["boundary_block_and_txindex"])
    source_fp, selected_fp = Fingerprint(), Fingerprint()
    output = (destination / name).open("xb") if destination else nullcontext()
    with open_source(source, name) as stream, output as writer:
        for position, line in enumerate(stream):
            source_fp.add(line)
            if position == 0:
                selected = True
            elif name == TRANSACTIONS:
                selected = start <= position <= stop
                if position == start and transaction_location(line) != first:
                    raise ValueError("Unexpected first transaction boundary")
                if position == stop and transaction_location(line) != last:
                    raise ValueError("Unexpected last transaction boundary")
            else:
                selected = first <= call_location(line) <= last
            if selected:
                selected_fp.add(line)
                if writer is not None:
                    writer.write(line)
    source_fp.check("5M source " + name, SOURCE["files"][name])
    selected_fp.check("formal " + name, SELECTION["files"][name])


def select(source: Path, output: Path, check_only: bool):
    if check_only:
        for name in FILES:
            select_file(source, name, None)
        return
    output.mkdir(parents=True, exist_ok=True)
    if any((output / name).exists() for name in FILES):
        raise FileExistsError(f"An output CSV already exists in {output}")
    with TemporaryDirectory(prefix=".trace-", dir=output) as temp:
        stage = Path(temp)
        for name in FILES:
            select_file(source, name, stage)
        published = []
        try:
            for name in FILES:
                # The hard link publishes a fully verified file without overwriting an existing one.
                target = output / name
                target.hardlink_to(stage / name)
                published.append(target)
        except OSError:
            for target in published:
                target.unlink()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("verify", help="check the plain formal CSV files")
    check.add_argument("--directory", type=Path, default=FORMAL)
    extract = commands.add_parser("select", help="derive the formal trace from the original 5M pair")
    extract.add_argument("--source", type=Path, required=True, help="directory with the 5M CSV or CSV.gz files")
    extract.add_argument("--output", type=Path, default=FORMAL)
    extract.add_argument("--check-only", action="store_true", help="hash the slice without creating files")
    args = parser.parse_args()
    try:
        if args.command == "verify":
            verify(args.directory)
        else:
            select(args.source, args.output, args.check_only)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
