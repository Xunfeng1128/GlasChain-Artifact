"""Read-only canonical Go workload check; never starts emulator nodes."""
from pathlib import Path
import json
import hashlib
import shutil
import subprocess
import tempfile
if __package__:
    from .glas_pending import validate_glas_pending_config
    from .clpa_scheduler import validate_clpa_scheduler_config
else:  # Existing direct-module tests and standalone callers.
    from glas_pending import validate_glas_pending_config
    from clpa_scheduler import validate_clpa_scheduler_config


class PreflightError(ValueError):
    pass


def input_signature(path):
    st = path.stat()
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def check_workloads(root, jobs):
    """Scan once per distinct input/prefix/pricing tuple in this invocation.

    No persistent success cache: each execution rechecks its current input.
    The upper bound assumes every eligible attached call crosses shards, so
    its guarantee covers every mapping, including future reconfigurations.
    """
    root = Path(root)
    grouped = {}
    for job in jobs:
        cfg = job['config']
        validate_clpa_scheduler_config(cfg)
        if cfg.get('ConsensusMethod') == 12:
            validate_glas_pending_config(cfg)
        tx = (root / cfg['UnifiedTransactionsFile']).resolve()
        calls = (root / cfg['UnifiedCallsFile']).resolve()
        count, limit, unit = (cfg[k] for k in ('TotalDataSize', 'BlockGasLimit', 'UnifiedCrossCallGas'))
        if any(type(v) is not int for v in (count, limit, unit)) or count <= 0 or limit <= 0 or unit < 0:
            raise PreflightError('预检要求正交易数、正区块容量及非负整数调用费用')
        grouped.setdefault((tx, calls, count, limit, unit, cfg.get('E2PrefixCount', 0)), []).append(job)
    if not grouped:
        return
    if shutil.which('go') is None:
        raise PreflightError('未找到 go；完整可执行性预检需要项目的 Go 工具链')
    with tempfile.TemporaryDirectory(prefix='block-emulator-preflight-') as folder:
        binary = Path(folder) / 'workload_preflight'
        print('编译只读数据预检工具（不启动实验节点）...', flush=True)
        subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(binary),
                        './cmd/workload_preflight'], cwd=root, check=True)
        for group_no, ((tx, calls, count, limit, unit, prefix_count), members) in enumerate(grouped.items()):
            print(f'核验前 {count:,} 笔有效交易及内部调用：调用费用={unit}, 区块上限={limit:,}；'
                  f'本批 {len(members)} 个运行共用此次扫描...', flush=True)
            before = {path: input_signature(path) for path in (tx, calls)}
            command = [str(binary), '--transactions', str(tx), '--calls', str(calls),
                       '--count', str(count), '--block-gas-limit', str(limit),
                       '--cross-call-gas', str(unit)]
            if prefix_count:
                prefix_file = Path(folder) / f'prefix-{group_no}.csv'
                command += ['--e2-prefix-count', str(prefix_count), '--e2-prefix-file', str(prefix_file)]
            result = subprocess.run(command, cwd=root, capture_output=True, text=True)
            if any(input_signature(path) != sig for path, sig in before.items()):
                raise PreflightError('扫描期间输入文件发生变化，拒绝使用预检结果')
            try:
                report = json.loads(result.stdout)
            except (TypeError, ValueError) as exc:
                raise PreflightError('预检工具未返回有效 JSON: ' + (result.stderr or '')[-2000:]) from exc
            if (not isinstance(report, dict) or result.returncode != 0 or report.get('valid') is not True
                    or report.get('checked_transactions') != count or report.get('requested_transactions') != count
                    or report.get('block_gas_limit') != limit or report.get('cross_call_gas') != unit
                    or report.get('invalid_count') != 0):
                raise PreflightError('单笔可执行性/数据前缀预检未通过；没有启动实验节点。\n'
                                     + json.dumps(report, ensure_ascii=False, indent=2))
            if type(report.get('max_stage_gas_bound')) is not int or not 0 <= report['max_stage_gas_bound'] <= limit:
                raise PreflightError('预检报告的最大单笔费用无效')
            if prefix_count:
                prefix_bytes = prefix_file.read_bytes()
                if prefix_bytes.count(b'\n') != prefix_count + 1 or not prefix_bytes.startswith(b'source_tx_hash\n'):
                    raise PreflightError('E2 有效输入前缀记录数量或表头不匹配')
                prefix_hash = hashlib.sha256(prefix_bytes).hexdigest()
                print(f'  E2 固定样本 {prefix_count:,} 笔；SHA-256 {prefix_hash}', flush=True)
            for job in members:
                if prefix_count:
                    job['e2_prefix_bytes'] = prefix_bytes
                    job['e2_prefix_sha256'] = prefix_hash
                job['workload_preflight'] = report
                job['preflight_input_signatures'] = before
            print(f"  通过：{count:,} 笔；最大单笔保守上界 {report['max_stage_gas_bound']:,} Gas。", flush=True)


def assert_preflight_current(job):
    """Do not let input changes between scanning, hashing and launch bypass it."""
    signatures = job.get('preflight_input_signatures')
    if not signatures or not job.get('workload_preflight', {}).get('valid'):
        raise PreflightError('缺少本次运行的可执行性预检')
    if any(input_signature(path) != sig for path, sig in signatures.items()):
        raise PreflightError('预检后输入文件发生变化，请重新预检；未启动本次实验')
