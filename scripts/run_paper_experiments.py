#!/usr/bin/env python3
"""List, preflight or run the paper configurations with their corresponding engines."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ('formal', 'archived-baseline', 'archived-e2-fixed')
GROUP = {'E1': 'E1_overall', 'E2': 'E2_system', 'E3': 'E3_drain',
         'E4': 'E3_drain', 'E5': 'E3_drain', 'E6': 'E3_drain'}
SHARED_E1 = 'e1_glas13_s16_r2000_n500000'
SHARED_RUN = 'e3_glas13_s16_r2000_n500000'


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def materialize_engine(profile: str, destination: Path) -> Path:
    """Copy the filtered runtime, apply the archived delta and verify its bytes."""
    overlay = ROOT / 'provenance' / 'engines' / profile
    files = read_json(overlay / 'files.json')
    destination.mkdir()
    for relative, item in files.items():
        if item['base_sha256'] is None:
            continue
        source = ROOT / relative
        raw = source.read_bytes()
        if sha256(raw).hexdigest() != item['base_sha256']:
            raise ValueError(f'Archive patch input has changed: {relative}')
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    # Run Git outside the artifact's Git index. The temporary engine is a
    # separate source tree and the patch must never change files in the root.
    env = dict(os.environ, GIT_CEILING_DIRECTORIES=str(destination.parent))
    subprocess.run(['git', 'apply', '--whitespace=nowarn', str(overlay / 'source.patch')],
                   cwd=destination, env=env, check=True)
    for relative, item in files.items():
        if sha256((destination / relative).read_bytes()).hexdigest() != item['profile_sha256']:
            raise ValueError(f'Archive patch output differs: {relative}')
    (destination / 'datasets').symlink_to(ROOT / 'datasets', target_is_directory=True)
    (destination / 'runs').symlink_to(ROOT / 'runs', target_is_directory=True)
    return destination


def selected_conditions(manifest: Path, experiment: str, conditions: set[str]) -> list[str]:
    group = GROUP.get(experiment)
    return [
        item['condition_id'] for item in read_json(manifest)['conditions']
        if (not group or group in item['used_in'])
        and (not conditions or item['condition_id'] in conditions)
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--experiment', choices=('all', *GROUP), default='all')
    parser.add_argument('--condition', nargs='+', help='One or more condition IDs')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--execute', action='store_true', help='Build and launch local nodes')
    mode.add_argument('--check', action='store_true', help='Check input hashes and scan data')
    parser.add_argument('--resume', action='store_true', help='Reuse verified local runs')
    parser.add_argument('--cleanup-database', action='store_true',
                        help='Delete only this run database after successful completion')
    args = parser.parse_args()
    conditions = {SHARED_RUN if c == SHARED_E1 else c for c in args.condition or []}
    jobs = []
    for profile in PROFILES:
        manifest = ROOT / 'configs' / f'{profile}.json'
        chosen = selected_conditions(manifest, args.experiment, conditions)
        if chosen:
            jobs.append((profile, manifest, chosen))
    selected = {condition for _, _, chosen in jobs for condition in chosen}
    if not selected or not conditions <= selected:
        parser.error('No matching runs for: ' + ', '.join(sorted(conditions - selected)))

    output = ROOT / 'runs'
    output.mkdir(exist_ok=True)
    with ExitStack() as resources:
        if args.execute:
            import fcntl

            lock = resources.enter_context((output / '.paper-runner.lock').open('a'))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                parser.error('Another local paper run is using the loopback topology')
        temporary = resources.enter_context(TemporaryDirectory(prefix='.engines-', dir=output))
        for profile, manifest, chosen in jobs:
            print(f'{profile}: {len(chosen)} run(s)', flush=True)
            engine = ROOT
            if profile == 'archived-e2-fixed':
                engine = materialize_engine(profile, Path(temporary) / profile)
            command = [sys.executable, '-u', str(engine / 'scripts/run_experiments.py'),
                       '--manifest', str(manifest), '--condition', *chosen]
            for enabled, flag in ((args.execute, '--execute'), (args.check, '--check'),
                                  (args.resume, '--resume'), (args.cleanup_database, '--cleanup-database')):
                if enabled:
                    command.append(flag)
            subprocess.run(command, cwd=engine, check=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
