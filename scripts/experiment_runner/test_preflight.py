"""Runner checks with a mocked read-only checker; no experiment is started."""
import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import preflight as p


class WorkloadPreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'tx.csv').write_text('fixture')
        (self.root / 'calls.csv').write_text('fixture')
        self.job = {'config': {'ConsensusMethod': 3, 'TotalDataSize': 2,
                              'BlockGasLimit': 100, 'UnifiedCrossCallGas': 3,
                              'UnifiedTransactionsFile': 'tx.csv', 'UnifiedCallsFile': 'calls.csv'}}
        self.report = {'schema_version': 1, 'valid': True, 'requested_transactions': 2,
                       'checked_transactions': 2, 'block_gas_limit': 100, 'cross_call_gas': 3,
                       'max_stage_gas_bound': 99, 'invalid_count': 0}
        self.scans = 0
        self.commands = []
        self.returncode = 0

    def fake_run(self, cmd, **kwargs):
        if cmd[0] != 'go':
            self.scans += 1
            self.commands.append(cmd)
            return subprocess.CompletedProcess(cmd, self.returncode, json.dumps(self.report), '')
        return subprocess.CompletedProcess(cmd, 0)

    def run_check(self, jobs):
        with patch.object(p.shutil, 'which', return_value='/fake/go'), patch.object(p.subprocess, 'run', side_effect=self.fake_run), contextlib.redirect_stdout(io.StringIO()):
            p.check_workloads(self.root, jobs)

    def test_deduplicates_methods_shards_repeats_but_rechecks_each_invocation(self):
        jobs = [copy.deepcopy(self.job) for _ in range(5)]
        self.run_check(jobs)
        self.assertEqual(self.scans, 1)
        for j in jobs:
            p.assert_preflight_current(j)
            self.assertEqual(j['workload_preflight'], self.report)
        self.run_check(jobs)
        self.assertEqual(self.scans, 2)

    def test_rejects_failed_short_or_inconsistent_report(self):
        original = self.report.copy()
        for field, value in [('valid', False), ('checked_transactions', 1), ('requested_transactions', 1),
                             ('block_gas_limit', 200), ('cross_call_gas', 5), ('invalid_count', 1),
                             ('max_stage_gas_bound', 101)]:
            with self.subTest(field=field):
                self.report = dict(original, **{field: value})
                self.assertRaises(p.PreflightError, self.run_check, [self.job])
        self.report = original
        self.returncode = 2
        self.assertRaises(p.PreflightError, self.run_check, [self.job])

    def test_input_change_invalidates_success(self):
        self.run_check([self.job])
        (self.root / 'calls.csv').write_text('new data after preflight')
        self.assertRaises(p.PreflightError, p.assert_preflight_current, self.job)

    def test_shared_preflight_does_not_require_call_gas(self):
        self.run_check([self.job])
        self.assertNotIn('--require-call-gas', self.commands[0])


if __name__ == '__main__':
    unittest.main()
