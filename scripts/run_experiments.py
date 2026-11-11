#!/usr/bin/env python3
"""Sequential ICDE runs. Default: list only. --execute explicitly launches nodes."""
from __future__ import annotations

import argparse
import csv
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from contextlib import contextmanager
from experiment_runner.launcher import LaunchError
from experiment_runner.preflight import check_workloads, assert_preflight_current
from experiment_runner.garet_catalog import prepare_catalogs, validate_garet_config
from experiment_runner.glas_pending import formal_glas_enabled, validate_glas_pending_config
from experiment_runner.clpa_scheduler import validate_clpa_scheduler_config
from unified_metrics.summarize import InvalidRun
from unified_metrics.summarize_e2_window import latency_policy, validate_windows

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / 'configs/formal.json'
METHODS = {'hash': 3, 'clpa': 1, 'clpa-gas': 4, 'plouvain': 9,
           'plouvain-gas': 7, 'txallo': 8, 'garet': 10, 'glas': 12}
GROUPS = {'E1': 'E1_overall', 'E2': 'E2_heterogeneity',
          'E3': 'E3_drain', 'E3a': 'E3a_simple_gas_weight', 'E3b': 'E3b_internal_call_ablation',
          'E4': 'E4_injection_rate'}
FLAGS = {'GLAS_LOCAL_REFINE': 'off', 'GLAS_LOCAL_REFINE_DIAG': '0', 'GLAS_COMPARE_PLANNERS': '0'}
RUNTIME_ENV = ('GOMAXPROCS', 'GOGC', 'GOMEMLIMIT', 'GODEBUG', 'TZ')
ARTIFACTS = ('runtime_config.json', 'runtime_ip_table.json', 'workload_preflight.json', 'result/unified_metrics/run.json',
             'result/unified_metrics/events.csv', 'analysis/windows.csv', 'analysis/summary.json', 'analysis/validation.json')
ADDITIONAL_ARTIFACTS = ('result/unified_metrics/reconfiguration.csv',)
E2_ARTIFACTS = tuple(name for name in ARTIFACTS if not name.startswith('analysis/')) + (
    'e2_prefix.csv', 'analysis/e2_validation.json', 'analysis/e2_throughput.csv', 'analysis/e2_latency.csv')
E2_WINDOW_ARTIFACTS = tuple(name for name in E2_ARTIFACTS if name != 'e2_prefix.csv')
PENDING_ARTIFACTS = ('result/glas_pending_plans.jsonl',)
TX_HEADERS = set('blockNumber transactionIndex transactionHash from to toCreate value gasUsed receiptStatus fromIsContract toIsContract txCategory'.split())
CALL_HEADERS = set('blockNumber transactionIndex transactionHash traceAddress callDepth caller callee callType callGasUsed error txStatus callerIsContract calleeIsContract isCCGEdge'.split())


class RunnerError(Exception):
    pass


def artifacts_for(j):
    method = j.get('config', {}).get('ConsensusMethod', j.get('condition', {}).get('consensus_method'))
    allowed_extra = ADDITIONAL_ARTIFACTS + (PENDING_ARTIFACTS if method == 12 else ())
    c = j.get('condition', {})
    if 'E1_overall' in c.get('used_in', ()) and c.get('required_config', {}).get('FormalMapCheckpoint'):
        allowed_extra += ('result/supervisor_measureOutput/Average_TPS.csv', 'result/e1_checkpoint.json')
        allowed_extra += tuple(f"result/pbft_shardNum={c['shards']}/Shard{s}{c['shards']}.csv" for s in range(c['shards']))
    if 'E3_drain' in c.get('used_in', ()):
        allowed_extra += tuple(f"result/pbft_shardNum={c['shards']}/Shard{s}{c['shards']}.csv" for s in range(c['shards']))
    extra = j['run'].get('required_artifacts', [])
    if (not isinstance(extra, list) or any(name not in allowed_extra for name in extra)
            or len(extra) != len(set(extra))):
        raise RunnerError('清单包含不支持或重复的必需产物')
    garet = ('garet_group_catalog.json',) if method == 10 else ()
    cfg = j.get('config', {})
    outputs = ARTIFACTS
    if fixed_e2(cfg):
        outputs = E2_WINDOW_ARTIFACTS
    elif cfg.get('E2PrefixCount'):
        outputs = E2_ARTIFACTS
    return outputs + tuple(extra) + garet


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def resolve(root, path):
    p = Path(path)
    return (p if p.is_absolute() else root / p).resolve()


def write_json(path, value):
    path = Path(path)
    temp = path.with_name(path.name + '.tmp')
    temp.write_bytes(encoded(value))
    os.replace(temp, path)


def select_runs(manifest, methods=('all',), group='all', repeats=(1, 2, 3), shards=None, rates=None, conditions=None):
    if group == 'E3b':
        raise RunnerError('E3b 的规划端消融尚未实现，不能把完整 GLAS 控制组当作整个实验。')
    chosen_modes = set(METHODS.values()) if 'all' in methods else {METHODS[m] for m in methods}
    by_id = {c['condition_id']: c for c in manifest['conditions']}
    result, seen = [], set()
    for run in manifest['runs']:
        c = by_id[run['condition_id']]
        if c['consensus_method'] not in chosen_modes or run['repeat'] not in repeats:
            continue
        if shards and c['shards'] not in shards or rates and c['inject_speed_tps'] not in rates:
            continue
        if conditions and c['condition_id'] not in conditions:
            continue
        if group != 'all':
            prefix = GROUPS[group]
            if not (group == 'E2' and 'E2_system' in c['used_in']) and not any(v == prefix or v.startswith(prefix + '_') for v in c['used_in']):
                continue
        if run['run_id'] in seen:
            raise RunnerError('清单中存在重复 run_id: ' + run['run_id'])
        seen.add(run['run_id'])
        result.append({'run': run, 'condition': c})
    if not result:
        raise RunnerError('筛选条件没有匹配到运行配置。')
    return result


def ip_addresses(table, shards, nodes):
    # build.initConfig validates every shard, including unused table entries.
    if not isinstance(table, dict) or '2147483647' not in table:
        raise RunnerError('IP 表缺少监督端 2147483647/0')
    shard_keys = {k for k in table if k != '2147483647'}
    if shard_keys != {str(i) for i in range(len(shard_keys))} or len(shard_keys) < shards:
        raise RunnerError('IP 表分片编号必须从 0 连续排列并覆盖本次规模')
    for sid in shard_keys:
        if any(str(n) not in table[sid] for n in range(nodes)):
            raise RunnerError('IP 表节点数不足: shard ' + sid)
    addresses = [table[str(s)][str(n)] for s in range(shards) for n in range(nodes)]
    addresses.append(table['2147483647']['0'])
    normalized = []
    for address in addresses:
        host, port = address.rsplit(':', 1)
        ip = ipaddress.ip_address(host)
        if ip.version != 4 or not ip.is_loopback or not 0 < int(port) < 65536:
            raise RunnerError('当前批量入口只支持单台机器的 IPv4 回环地址: ' + address)
        normalized.append((str(ip), int(port)))
    if len(set(normalized)) != len(normalized):
        raise RunnerError('IP 表包含重复地址/端口')
    return addresses



def fixed_e2(cfg):
    seconds = cfg.get('E2ObservationSeconds', 0)
    return formal_glas_enabled(cfg) and type(seconds) is int and seconds > 0


def e2_policy(cfg, run):
    windows = run.get('e2_windows_seconds', [])
    prefixes = run.get('e2_prefix_counts', [])
    duration, count = cfg.get('E2ObservationSeconds', 0), cfg.get('E2PrefixCount', 0)
    if not any((duration, count, windows, prefixes, cfg.get('E2PrefixFile'))):
        return
    if fixed_e2(cfg):
        try:
            validate_windows(windows)
        except InvalidRun as exc:
            raise RunnerError(str(exc)) from exc
        if count or prefixes or cfg.get('E2PrefixFile') or windows[-1] != duration or run['observation_seconds'] != duration:
            raise RunnerError('fixed E2 requires matching observation duration and no source prefix')
        return
    for label, values, largest in (('window', windows, duration), ('prefix', prefixes, count)):
        if (type(largest) is not int or largest <= 0 or not isinstance(values, list) or not values
                or any(type(v) is not int or v <= 0 for v in values)
                or sorted(set(values)) != values or values[-1] != largest):
            raise RunnerError(f'invalid E2 {label} list or termination target')
    if (cfg['ConsensusMethod'] not in (1, 3, 9, 12) or count > cfg['TotalDataSize']
            or run['observation_seconds'] != duration):
        raise RunnerError('E2 requires a supported method, a valid source prefix and matching observation duration')



def epoch_quota_policy(cfg):
    epochs, blocks = cfg.get('ExperimentEpochCount', 0), cfg.get('ExperimentBlocksPerEpoch', 0)
    if type(epochs) is not int or type(blocks) is not int:
        raise RunnerError('epoch counts must be integers')
    if (epochs or blocks) and not formal_glas_enabled(cfg):
        raise RunnerError('six-block epoch boundaries are available only for formal GlasChain')


def make_jobs(root, manifest_path, selected, output_base=None):
    jobs, outputs = [], set()
    manifest = read_json(manifest_path)
    verify_source = manifest.get('data_integrity_policy', {}).get('require_expected_file_sha256', False)
    datasets = {d['dataset_id']: d for d in manifest.get('datasets', [])} if verify_source else {}
    for item in selected:
        c, run = item['condition'], item['run']
        config_path = (manifest_path.parent / run['config_file']).resolve()
        cfg = read_json(config_path)
        for option in ('GLASPendingEarlyReport', 'GLASPendingFullWindowEarly'):
            enabled = cfg.get(option, False)
            if type(enabled) is not bool or (enabled and c['consensus_method'] != 12):
                raise RunnerError(f'{option} 必须为布尔值且只适用于 GLAS_Pending')
        checks = {'ConsensusMethod': c['consensus_method'], 'InjectSpeed': c['inject_speed_tps'],
                  'TotalDataSize': c['total_transactions'], 'UnifiedExecution': True, 'UnifiedMetrics': True,
                  'ExpDataRootDir': run['output_root']}
        if any(cfg.get(k) != v for k, v in checks.items()):
            raise RunnerError('配置与实验清单不一致: ' + str(config_path))
        required = c.get('required_config', {})
        if not isinstance(required, dict) or any(cfg.get(k) != v for k, v in required.items()):
            raise RunnerError('配置不满足本实验声明的公共参数/输入路径: ' + str(config_path))
        e2_policy(cfg, run)
        epoch_quota_policy(cfg)
        artifacts_for(dict(item, config=cfg))
        validate_clpa_scheduler_config(cfg)
        if c['consensus_method'] == 10:
            validate_garet_config(cfg)
        if c['consensus_method'] == 12:
            validate_glas_pending_config(cfg)
        args = ['-S', str(c['shards']), '-N', str(c['nodes_per_shard'])]
        if run['shared_process_arguments'] != args:
            raise RunnerError('拓扑参数不一致: ' + run['run_id'])
        if cfg.get('BlockCapacityMode') != 'gas' or cfg.get('BlockGasLimit', 0) <= 0:
            raise RunnerError('统一实验要求硬 Gas 容量')
        out = (output_base / c['condition_id'] / ('r%02d' % run['repeat'])).resolve() if output_base else resolve(root, cfg['ExpDataRootDir'])
        if out == root or root.is_relative_to(out) or out in outputs or any(p.is_relative_to(out) or out.is_relative_to(p) for p in outputs):
            raise RunnerError('不安全或重叠的输出目录: ' + str(out))
        outputs.add(out)
        cfg = dict(cfg)
        cfg['ExpDataRootDir'] = str(out)  # Runtime snapshot only; originals untouched.
        if cfg.get('E2PrefixCount'):
            cfg['E2PrefixFile'] = str(out / 'e2_prefix.csv')
        if c['consensus_method'] == 10:
            cfg['GARETGroupCatalogFile'] = str(out / 'garet_group_catalog.json')
        table_path = resolve(root, cfg['IpTableFile'])
        table_bytes = table_path.read_bytes()
        table = json.loads(table_bytes)
        cfg['IpTableFile'] = str(out / 'runtime_ip_table.json')
        addresses = ip_addresses(table, c['shards'], c['nodes_per_shard'])
        flags = dict(FLAGS)
        flags.update(c.get('required_environment', {}))
        if any(flags[k] != v for k, v in FLAGS.items()):
            raise RunnerError('清单意外启用了主方法以外的 GLAS 规划环境变量')
        jobs.append(dict(item, config=cfg, source_config=config_path, output=out, addresses=addresses,
                         ip_table=table_path, ip_table_bytes=table_bytes, flags=flags, config_sha=sha_bytes(encoded(cfg))))
        if verify_source:
            evidence = datasets.get(c.get('dataset_id'), {}).get('files', {})
            declared = {resolve(root, info['path']): info.get('sha256') for info in evidence.values()}
            expected = {}
            for key in ('UnifiedTransactionsFile', 'UnifiedCallsFile'):
                path = resolve(root, cfg[key])
                digest = declared.get(path)
                if (not isinstance(digest, str) or len(digest) != 64
                        or any(ch not in '0123456789abcdef' for ch in digest)):
                    raise RunnerError('清单缺少输入的有效 SHA256: ' + str(path))
                expected[path] = digest
            jobs[-1]['expected_data_sha256'] = expected
    return jobs


def dataset_preflight(root, jobs):
    files = {}
    for j in jobs:
        for key, required in [('UnifiedTransactionsFile', TX_HEADERS), ('UnifiedCallsFile', CALL_HEADERS)]:
            path = resolve(root, j['config'][key])
            files.setdefault(path, set()).update(required)
    errors = []
    for path, required in files.items():
        if not path.is_file():
            errors.append('缺少数据文件: ' + str(path))
            continue
        try:
            with path.open(newline='', encoding='utf-8') as f:
                header = [s.strip() for s in next(csv.reader(f))]
            missing = required - set(header)
            if missing or len(header) != len(set(header)):
                errors.append(f'数据表头不正确: {path}; 缺少列={sorted(missing)}')
        except (OSError, UnicodeError, StopIteration, csv.Error) as exc:
            errors.append(f'不能读取表头: {path}: {exc}')
    return files, errors


def print_plan(jobs):
    grouped = defaultdict(list)
    for j in jobs:
        grouped[j['condition']['condition_id']].append(j)
    print(f'选中 {len(grouped)} 个设置、{len(jobs)} 次运行；按顺序执行，不并行跑不同实验。')
    print('P-Louvain=ContribChain 计数版；P-Louvain-Gas 为单独扩展。E3b 待实现，不在可运行队列中。')
    if any(j['config'].get('E2PrefixCount') for j in jobs):
        print('E2：固定窗口和固定哈希样本共用一次运行；覆盖最长窗口且最大前缀全部最终确认后正常收尾。')
    if any(fixed_e2(j['config']) for j in jobs):
        print('新版 GLAS E2：从首次派发开始计时，到固定截止时刻停止观测并统计。')
    for cid, js in grouped.items():
        c = js[0]['condition']
        repeats = ','.join(str(j['run']['repeat']) for j in js)
        print(f"  {cid}  K={c['shards']}  R={c['inject_speed_tps']}/s  N={c['total_transactions']}  观察={c['observation_seconds']}s  重复={repeats}")


def fingerprint(root, j, binary_sha, data):
    result = {'config_sha256': j['config_sha'], 'binary_sha256': binary_sha,
            'ip_table_sha256': sha_bytes(j['ip_table_bytes']),
            'data_sha256': {key: data[resolve(root, j['config'][key])]['sha256'] for key in ('UnifiedTransactionsFile', 'UnifiedCallsFile')},
            'environment': dict(j['flags'], **{k: os.environ.get(k) for k in RUNTIME_ENV}),
            'summary_script_sha256': file_hash(root / 'scripts/unified_metrics/summarize.py'),
            'runner_sha256': file_hash(Path(__file__)),
            'launcher_sha256': file_hash(Path(__file__).parent / 'experiment_runner/launcher.py'),
            'preflight_script_sha256': file_hash(Path(__file__).parent / 'experiment_runner/preflight.py'),
            'clpa_scheduler_validator_sha256': file_hash(Path(__file__).parent / 'experiment_runner/clpa_scheduler.py'),
            'shards': j['condition']['shards'], 'nodes': j['condition']['nodes_per_shard'],
            'observation_seconds': j['run']['observation_seconds'], 'bin_seconds': j['run']['metric_bin_seconds']}
    if fixed_e2(j['config']):
        windows = j['run']['e2_windows_seconds']
        result['e2'] = {'stop_policy': 'fixed_window', 'windows': windows, 'latency': latency_policy(windows),
                        'analyzer_sha256': file_hash(root / 'scripts/unified_metrics/summarize_e2_window.py')}
    elif j['config'].get('E2PrefixCount'):
        result['e2'] = {'windows': j['run']['e2_windows_seconds'], 'prefixes': j['run']['e2_prefix_counts'],
                        'prefix_sha256': j['e2_prefix_sha256'],
                        'analyzer_sha256': file_hash(root / 'scripts/unified_metrics/summarize_e2.py')}
    if j['run'].get('required_artifacts'):
        result['required_artifacts'] = list(j['run']['required_artifacts'])
    if j['config'].get('ConsensusMethod') == 12:
        result['glas_pending_validator_sha256'] = file_hash(Path(__file__).parent / 'experiment_runner/glas_pending.py')
    if j['config'].get('ConsensusMethod') == 10:
        if 'garet_catalog' not in j:
            raise RunnerError('GARET 缺少已验证的账户分组目录')
        result['garet_catalog'] = j['garet_catalog']
        result['garet_catalog_script_sha256'] = file_hash(Path(__file__).parent / 'experiment_runner/garet_catalog.py')
    return result


def can_resume(j, expected):
    state = read_json(j['output'] / 'run_state.json')
    if state.get('status') != 'completed' or state.get('run_id') != j['run']['run_id'] or state.get('fingerprint') != expected:
        raise RunnerError('已有目录不是同版本成功运行，拒绝复用/覆盖: ' + str(j['output']))
    artifacts = artifacts_for(j)
    if set(state.get('artifacts_sha256', {})) != set(artifacts):
        raise RunnerError('已有结果缺少完整的输出文件指纹: ' + str(j['output']))
    for name in artifacts:
        path = j['output'] / name
        if not path.is_file() or file_hash(path) != state['artifacts_sha256'][name]:
            raise RunnerError('已有结果缺失或被修改，拒绝复用: ' + str(path))
    if j['config'].get('ConsensusMethod') == 10 and state['artifacts_sha256']['garet_group_catalog.json'] != expected['garet_catalog']['file_sha256']:
        raise RunnerError('归档 GARET 分组目录与本次运行身份不一致: ' + str(j['output']))
    if fixed_e2(j['config']) or j['config'].get('E2PrefixCount'):
        return e2_passed(j)
    validation = read_json(j['output'] / 'analysis/validation.json')
    meta = read_json(j['output'] / 'result/unified_metrics/run.json')
    summary = read_json(j['output'] / 'analysis/summary.json')
    if meta.get('status') != 'closed' or not summary.get('valid_for_publication') or not validation.get('valid_for_publication'):
        raise RunnerError('已有结果未通过统一统计检查: ' + str(j['output']))
    return True


def e2_passed(job):
    out = job['output']
    meta = read_json(out / 'result/unified_metrics/run.json')
    evidence = read_json(out / 'analysis/e2_validation.json')
    if fixed_e2(job['config']):
        windows = job['run']['e2_windows_seconds']
        if (meta.get('status') != 'closed' or meta.get('completion_scope') != 'e2'
                or meta.get('e2_stop_policy') != 'fixed_window' or meta.get('e2_prefix_count')
                or meta.get('e2_prefix_sha256') or meta.get('e2_observation_seconds') != windows[-1]
                or meta.get('observed_duration_ns') != windows[-1] * 10**9
                or evidence.get('valid_for_e2') is not True or evidence.get('completion_scope') != 'e2'
                or evidence.get('e2_stop_policy') != 'fixed_window' or evidence.get('windows_seconds') != windows
                or evidence.get('latency_policy') != latency_policy(windows)):
            raise RunnerError('新版 GLAS E2 固定窗口或日志校验未通过: ' + str(out))
        return True
    expected_hash = job['e2_prefix_sha256']
    if (meta.get('status') != 'closed' or meta.get('completion_scope') != 'e2'
            or meta.get('e2_prefix_sha256') != expected_hash or file_hash(out / 'e2_prefix.csv') != expected_hash
            or evidence.get('valid_for_e2') is not True or evidence.get('completion_scope') != 'e2'
            or evidence.get('prefix_file_sha256') != expected_hash
            or evidence.get('prefixes') != job['run']['e2_prefix_counts']
            or evidence.get('windows_seconds') != job['run']['e2_windows_seconds']):
        raise RunnerError('E2 区间、固定交易样本或日志校验未通过: ' + str(out))
    return True


def analyze_run(root, job):
    out = job['output']
    is_fixed_e2 = fixed_e2(job['config'])
    is_prefix_e2 = bool(job['config'].get('E2PrefixCount'))
    if is_fixed_e2:
        script = 'summarize_e2_window.py'
    elif is_prefix_e2:
        script = 'summarize_e2.py'
    else:
        script = 'summarize.py'
    cmd = [sys.executable, str(root / 'scripts/unified_metrics' / script),
           '--run-dir', str(out / 'result/unified_metrics'), '--output-dir', str(out / 'analysis')]
    if is_fixed_e2:
        cmd += ['--windows', *map(str, job['run']['e2_windows_seconds'])]
    elif is_prefix_e2:
        cmd += ['--prefix-file', str(out / 'e2_prefix.csv'), '--windows', *map(str, job['run']['e2_windows_seconds']),
                '--prefixes', *map(str, job['run']['e2_prefix_counts'])]
    else:
        cmd += ['--window-seconds', str(job['run']['metric_bin_seconds']),
                '--duration-seconds', str(job['run']['observation_seconds'])]
    with (out / 'launcher/analysis.log').open('w') as log:
        subprocess.run(cmd, cwd=root, check=True, stdout=log, stderr=subprocess.STDOUT)
    if is_fixed_e2 or is_prefix_e2:
        e2_passed(job)
    elif not read_json(out / 'analysis/summary.json').get('valid_for_publication'):
        raise RunnerError('统计完整性检查未通过')


def cleanup_database(j):
    """Delete only a finished run's database after verifying retained evidence.

    Called only after launch_run has reaped its children, or on a verified
    completed resume. Cleanup metadata is separate from the scientific status
    and from ARTIFACTS, so successful results remain reusable without a database.
    """
    out = Path(j['output'])
    if out.is_symlink() or not out.is_dir():
        raise RunnerError('拒绝清理非真实运行目录: ' + str(out))
    can_resume(j, j['fingerprint'])
    processes = read_json(out / 'launcher/processes.json')
    expected_names = {'supervisor'} | {
        f's{s}n{n}' for s in range(j['condition']['shards'])
        for n in range(j['condition']['nodes_per_shard'])}
    records = processes.get('processes', [])
    if (processes.get('status') != 'completed' or not processes.get('finished_unix_ns')
            or len(records) != len(expected_names)
            or {p.get('name') for p in records} != expected_names
            or any(p.get('exit_code') != 0 or not p.get('exited_unix_ns') for p in records)):
        raise RunnerError('节点正常退出记录不完整，保留 database/: ' + str(out))
    state_path = out / 'run_state.json'
    state = read_json(state_path)
    database = out / 'database'
    previous = state.get('database_cleanup', {})
    if (previous.get('status') == 'completed' and not database.is_symlink()
            and not database.exists()):
        return
    record = {'status': 'running', 'relative_path': 'database', 'started_unix': time.time()}
    state['database_cleanup'] = record
    # Save the intent before deleting anything. Failure to save is fail-closed.
    write_json(state_path, state)
    try:
        if database.is_symlink():
            raise RunnerError('拒绝清理符号链接 database/: ' + str(database))
        if database.exists():
            if not database.is_dir() or database.resolve().parent != out.resolve():
                raise RunnerError('database/ 不是当前运行下的真实目录: ' + str(database))
            if not getattr(shutil.rmtree, 'avoids_symlink_attacks', False):
                raise RunnerError('当前 Python 不支持安全的目录清理，保留 database/')
            for directory, dirs, files in os.walk(database, followlinks=False):
                current = Path(directory)
                if current.is_mount() or any((current / name).is_symlink() for name in dirs + files):
                    raise RunnerError('database/ 含挂载点或符号链接，拒绝自动清理: ' + str(current))
            shutil.rmtree(database)
            record['status'] = 'completed'
        else:
            record['status'] = 'not_present'
        record['finished_unix'] = time.time()
        write_json(state_path, state)
    except BaseException as exc:
        record['status'] = 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed'
        record['error'] = str(exc)
        record['finished_unix'] = time.time()
        write_json(state_path, state)
        if isinstance(exc, KeyboardInterrupt):
            raise
        raise RunnerError('实验统计已通过，但 database/ 清理失败；停止后续队列: ' + str(exc)) from exc
    print(f"  database/ 清理：{record['status']}；结果、日志和配置已保留", flush=True)


@contextmanager
def project_lock(root):
    import fcntl
    state_dir = root / '.experiment_runner'
    state_dir.mkdir(exist_ok=True)
    with (state_dir / 'runner.lock').open('a+') as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunnerError('已有批量任务运行中；不能同时抢用相同端口。') from None
        try:
            yield state_dir
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def snapshot_data(files):
    result = {}
    for path in files:
        path = Path(path).resolve()
        print('计算数据指纹:', path, flush=True)
        before = path.stat()
        digest = file_hash(path)
        after = path.stat()
        signature = (after.st_size, after.st_mtime_ns)
        if (before.st_size, before.st_mtime_ns) != signature:
            raise RunnerError('哈希期间数据发生变化: ' + str(path))
        result[path] = {'sha256': digest, 'signature': signature}
    return result


def assert_data_unchanged(root, j, data):
    for key in ('UnifiedTransactionsFile', 'UnifiedCallsFile'):
        path = resolve(root, j['config'][key]); st = path.stat()
        if (st.st_size, st.st_mtime_ns) != data[path]['signature']:
            raise RunnerError('批量运行期间数据发生变化: ' + str(path))


def check_expected_data(jobs, data):
    for j in jobs:
        for path, expected in j.get('expected_data_sha256', {}).items():
            if data[path]['sha256'] != expected:
                raise RunnerError('数据 SHA256 与清单记录不一致（可能同步不完整或用了不同版本）；'
                                  '没有启动实验: ' + str(path))


def run_queue(root, jobs, files, args):
    from experiment_runner.launcher import launch_run
    if sys.platform not in ('darwin', 'linux'):
        raise RunnerError('当前批量入口支持 macOS/Linux 单机多进程。')
    if shutil.which('go') is None:
        raise RunnerError('未找到 go；需要先安装项目所需 Go 工具链。')
    # Refuse existing non-successful output before starting any experiment.
    for j in jobs:
        if j['output'].exists():
            if not args.resume:
                raise RunnerError('输出目录已存在；成功结果用 --resume 复用，重跑用新的 --output-base: ' + str(j['output']))
            try:
                if read_json(j['output'] / 'run_state.json').get('status') != 'completed':
                    raise RunnerError('已有失败/未完成目录，请用新的 --output-base 保留原结果重跑: ' + str(j['output']))
            except (OSError, ValueError) as exc:
                raise RunnerError('已有目录没有有效运行状态: ' + str(j['output'])) from exc
    with project_lock(root) as state_dir, tempfile.TemporaryDirectory(prefix='block-emulator-batch-') as build_dir:
        data = snapshot_data(files)
        check_expected_data(jobs, data)
        check_workloads(root, jobs)
        prepare_catalogs(root, jobs, data, Path(build_dir), state_dir)
        binary = Path(build_dir) / 'blockEmulator'
        print('编译当前源码（本批次仅编译一次）...', flush=True)
        subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(binary), '.'], cwd=root, check=True)
        binary_sha = file_hash(binary)
        # Identity check of every reused result before dispatching anything new.
        for j in jobs:
            assert_preflight_current(j)
            j['fingerprint'] = fingerprint(root, j, binary_sha, data)
            if j['output'].exists():
                can_resume(j, j['fingerprint'])
        for index, j in enumerate(jobs, 1):
            rid, out = j['run']['run_id'], j['output']
            if out.exists():
                print(f'[{index}/{len(jobs)}] 复用成功结果 {rid}', flush=True)
                if getattr(args, 'cleanup_database', False):
                    cleanup_database(j)
                continue
            assert_data_unchanged(root, j, data)
            assert_preflight_current(j)
            out.mkdir(parents=True, exist_ok=False)
            write_json(out / 'workload_preflight.json', j['workload_preflight'])
            if j['config'].get('ConsensusMethod') == 10:
                catalog_bytes = j['garet_catalog_bytes']
                if sha_bytes(catalog_bytes) != j['garet_catalog']['file_sha256']:
                    raise RunnerError('GARET 分组目录内存快照发生变化')
                (out / 'garet_group_catalog.json').write_bytes(catalog_bytes)
            if j['config'].get('E2PrefixCount'):
                (out / 'e2_prefix.csv').write_bytes(j['e2_prefix_bytes'])
            cfg_path = out / 'runtime_config.json'
            (out / 'runtime_ip_table.json').write_bytes(j['ip_table_bytes'])
            cfg_path.write_bytes(encoded(j['config']))
            state = {'schema_version': 1, 'run_id': rid, 'status': 'running', 'started_unix': time.time(),
                     'source_config': str(j['source_config']), 'fingerprint': j['fingerprint']}
            if fixed_e2(j['config']) or j['config'].get('E2PrefixCount'):
                state['completion_scope'] = 'e2'
            state_path = out / 'run_state.json'
            write_json(state_path, state)
            print(f'[{index}/{len(jobs)}] 开始 {rid}\n  输出: {out}', flush=True)
            try:
                env = dict(os.environ, **j['flags'])
                env['BLOCKEMULATOR_CONFIG'] = str(cfg_path)
                launch_run(binary=binary, project_root=root, config_path=cfg_path, output_root=out,
                           shards=j['condition']['shards'], nodes=j['condition']['nodes_per_shard'],
                           environment=env, addresses=j['addresses'], startup_timeout=args.startup_timeout,
                           run_timeout=args.run_timeout, shutdown_timeout=args.shutdown_timeout)
                assert_data_unchanged(root, j, data)
                if j['config'].get('ConsensusMethod') == 10 and file_hash(out / 'garet_group_catalog.json') != j['garet_catalog']['file_sha256']:
                    raise RunnerError('运行期间 GARET 分组目录被修改，保留结果并停止')
                state['status'] = 'analyzing'; write_json(state_path, state)
                analyze_run(root, j)
                state['artifacts_sha256'] = {name: file_hash(out / name) for name in artifacts_for(j)}
                state['status'] = 'completed'; state['finished_unix'] = time.time(); write_json(state_path, state)
                print(f'[{index}/{len(jobs)}] 完成，统计通过 {rid}', flush=True)
            except BaseException as exc:
                state['status'] = 'interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed'
                state['error'] = str(exc); state['finished_unix'] = time.time(); write_json(state_path, state)
                raise
            # Do not relabel a valid completed experiment if cleanup fails.
            if getattr(args, 'cleanup_database', False):
                cleanup_database(j)
    print('所选队列全部完成/复用；绘图数据在每次输出目录的 analysis/。')


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', type=Path, default=DEFAULT_MANIFEST)
    p.add_argument('--method', nargs='+', choices=['all'] + list(METHODS), default=['all'])
    p.add_argument('--group', choices=['all'] + list(GROUPS), default='all')
    p.add_argument('--repeat', nargs='+', type=int, choices=(1, 2, 3), default=[1, 2, 3])
    p.add_argument('--shards', nargs='+', type=int, choices=(4, 8, 16, 24, 32))
    p.add_argument('--rate', nargs='+', type=int, choices=(250, 500, 1000, 1500, 2000, 2500, 3000))
    p.add_argument('--condition', nargs='+')
    p.add_argument('--output-base', type=Path, help='Only runtime copies change ExpDataRootDir; use a new base to rerun without deleting results')
    mode = p.add_mutually_exclusive_group()
    mode.add_argument('--execute', action='store_true', help='Actually build and launch experiments; default only lists')
    mode.add_argument('--check', action='store_true', help='Build read-only checker and scan selected valid transactions/calls; never launch nodes')
    p.add_argument('--resume', action='store_true', help='Skip successful runs only when config/code/data identity matches')
    p.add_argument('--cleanup-database', action='store_true', help='After verified completion and process exit, delete only this run database/; keep results/logs/configs. Default: keep database.')
    p.add_argument('--startup-timeout', type=float, default=90)
    p.add_argument('--shutdown-timeout', type=float, default=90)
    p.add_argument('--run-timeout', type=float, default=0, help='Whole-run safety limit in seconds, 0=unlimited; NOT observation duration')
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if not all(math.isfinite(v) for v in (args.startup_timeout, args.shutdown_timeout, args.run_timeout)) or args.startup_timeout <= 0 or args.shutdown_timeout <= 0 or args.run_timeout < 0:
            raise RunnerError('超时参数必须为正；run-timeout 可以为 0。')
        manifest_path = args.manifest.resolve()
        selected = select_runs(read_json(manifest_path), args.method, args.group, args.repeat, args.shards, args.rate, args.condition)
        base = resolve(ROOT, args.output_base) if args.output_base else None
        jobs = make_jobs(ROOT, manifest_path, selected, base)
        print_plan(jobs)
        files, errors = dataset_preflight(ROOT, jobs)
        if errors:
            for error in errors: print('  未就绪:', error)
            if args.execute or args.check:
                raise RunnerError('请先准备以上数据。没有编译或启动任何实验。')
        if args.execute:
            def interrupted(*_):
                raise KeyboardInterrupt()
            previous = signal.signal(signal.SIGTERM, interrupted)
            try: run_queue(ROOT, jobs, files, args)
            finally: signal.signal(signal.SIGTERM, previous)
        elif args.check:
            data = snapshot_data(files) if any(j.get('expected_data_sha256') for j in jobs) else None
            if data is not None:
                check_expected_data(jobs, data)
            check_workloads(ROOT, jobs)
            if data is not None:
                for j in jobs:
                    assert_data_unchanged(ROOT, j, data)
            print('文件/表头、有效交易前缀及单笔 Gas 上界检查通过；未启动实验。')
            if any(j['config'].get('ConsensusMethod') == 10 for j in jobs):
                print('GARET 分组参数检查通过；--execute 时生成或验证缓存目录，本次没有生成账户分组数据。')
        else:
            print('仅列出计划。实际执行需追加 --execute；--check 可检查准备情况。')
        return 0
    except KeyboardInterrupt:
        print('已中止；保留本次日志，不继续后续实验。', file=sys.stderr); return 130
    except (RunnerError, LaunchError, OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        print('停止:', exc, file=sys.stderr); return 2


if __name__ == '__main__':
    raise SystemExit(main())
