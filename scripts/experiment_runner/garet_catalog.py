"""Prepare a small, immutable GARET group catalog before any node is launched.

Only GARET jobs enter this module. Catalog identity depends on the canonical
input files, admitted prefix, group count, admission limit and builder binary.
The full input is never copied into a node's group map.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


class CatalogError(ValueError):
    pass


def validate_garet_config(config):
    if config.get('UnifiedExecution') is not True or config.get('UnifiedMetrics') is not True:
        raise CatalogError('GARET requires UnifiedExecution=true and UnifiedMetrics=true')
    values = [config.get(k) for k in ('GARETGroupCount', 'GARETSampleSeconds', 'ReconfigTimeGap')]
    if any(type(v) is not int or v <= 0 for v in values):
        raise CatalogError('GARETGroupCount、GARETSampleSeconds、ReconfigTimeGap 必须为正整数')
    if values[2] % values[1]:
        raise CatalogError('GARET 的重配置周期必须是采样槽长度的整数倍')
    timeout = config.get('GARETReconfigurationTimeoutSeconds', 600)
    if type(timeout) is not int or timeout <= 0:
        raise CatalogError('GARETReconfigurationTimeoutSeconds 必须为正整数（省略时默认 600 秒）')


def sha_bytes(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def _validate_catalog(raw, expected_groups):
    try:
        catalog = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise CatalogError('GARET 分组目录不是有效 JSON') from exc
    if (not isinstance(catalog, dict) or set(catalog) != {'schema_version', 'address_count', 'groups'}
            or type(catalog['schema_version']) is not int or catalog['schema_version'] != 1
            or type(catalog['address_count']) is not int or catalog['address_count'] < expected_groups
            or not isinstance(catalog['groups'], list) or len(catalog['groups']) != expected_groups):
        raise CatalogError('GARET 分组目录结构、账户数或组数不匹配')
    previous = ''
    counts, canonical_groups = [], []
    for group in catalog['groups']:
        if not isinstance(group, dict) or set(group) != {'upper_bound', 'count'}:
            raise CatalogError('GARET 分组条目结构无效')
        bound, count = group['upper_bound'], group['count']
        if (not isinstance(bound, str) or len(bound) != 40 or
                any(c not in '0123456789abcdef' for c in bound) or bound <= previous
                or type(count) is not int or count <= 0):
            raise CatalogError('GARET 分组边界或账户计数无效')
        previous = bound
        counts.append(count)
        canonical_groups.append({'upper_bound': bound, 'count': count})
    if sum(counts) != catalog['address_count'] or max(counts) - min(counts) > 1:
        raise CatalogError('GARET 分组不是等量账户划分')
    # Match partition/garet.Catalog.Digest: Go struct order, compact JSON,
    # no whitespace/newline. This differs from the archived file SHA256.
    canonical = {'schema_version': 1, 'address_count': catalog['address_count'], 'groups': canonical_groups}
    digest = sha_bytes(json.dumps(canonical, separators=(',', ':')).encode())
    return catalog, digest


def _write_atomic(path, raw):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_bytes(raw)
    os.replace(temporary, path)


def _assert_inputs(paths, data):
    for path in paths:
        st = path.stat()
        if (st.st_size, st.st_mtime_ns) != tuple(data[path]['signature']):
            raise CatalogError('生成 GARET 分组目录期间输入发生变化: ' + str(path))


def prepare_catalogs(root, jobs, data, build_dir, state_dir):
    """Build once per invocation; reuse verified persistent catalogs by identity.

    Called only from --execute after the common workload preflight. Node launch
    occurs later, after every selected job and every resume identity is checked.
    """
    selected = [job for job in jobs if job['config'].get('ConsensusMethod') == 10]
    if not selected:
        return
    root, build_dir, state_dir = Path(root), Path(build_dir), Path(state_dir)
    for job in selected:
        validate_garet_config(job['config'])
    binary = build_dir / 'garet_catalog'
    print('编译 GARET 账户分组工具（不启动实验节点）...', flush=True)
    subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(binary),
                    './cmd/garet_catalog'], cwd=root, check=True)
    builder_sha = sha_bytes(binary.read_bytes())
    cache_dir = state_dir / 'garet_catalogs'
    if cache_dir.is_symlink():
        raise CatalogError('拒绝使用符号链接 GARET 分组缓存目录')
    cache_dir.mkdir(exist_ok=True)
    prepared = {}
    for job in selected:
        cfg = job['config']
        tx = (root / cfg['UnifiedTransactionsFile']).resolve()
        calls = (root / cfg['UnifiedCallsFile']).resolve()
        _assert_inputs((tx, calls), data)
        identity = {'schema_version': 1, 'builder_sha256': builder_sha,
                    'transactions_sha256': data[tx]['sha256'], 'calls_sha256': data[calls]['sha256'],
                    'prefix_transactions': cfg['TotalDataSize'], 'block_gas_limit': cfg['BlockGasLimit'],
                    'group_count': cfg['GARETGroupCount']}
        key = sha_bytes(encoded(identity))
        if key not in prepared:
            catalog_path, meta_path = cache_dir / (key + '.json'), cache_dir / (key + '.meta.json')
            if catalog_path.is_symlink() or meta_path.is_symlink():
                raise CatalogError('拒绝使用符号链接 GARET 分组缓存文件')
            if catalog_path.exists() or meta_path.exists():
                try:
                    raw = catalog_path.read_bytes()
                    meta = json.loads(meta_path.read_bytes())
                except (OSError, ValueError, UnicodeError) as exc:
                    raise CatalogError('GARET 分组缓存不完整；保留缓存并停止: ' + str(catalog_path)) from exc
                catalog, digest = _validate_catalog(raw, cfg['GARETGroupCount'])
                if (not isinstance(meta, dict) or meta.get('identity') != identity
                        or meta.get('file_sha256') != sha_bytes(raw) or meta.get('catalog_digest') != digest):
                    raise CatalogError('GARET 分组缓存身份/内容不匹配；拒绝复用: ' + str(catalog_path))
                print('验证并复用 GARET 分组目录:', key[:16], flush=True)
            else:
                print(f"为前 {cfg['TotalDataSize']:,} 笔有效交易建立 {cfg['GARETGroupCount']} 个固定账户组...", flush=True)
                with tempfile.TemporaryDirectory(prefix='garet-catalog-', dir=build_dir) as folder:
                    output = Path(folder) / 'catalog.json'
                    try:
                        result = subprocess.run([str(binary), '--transactions', str(tx), '--calls', str(calls),
                            '--count', str(cfg['TotalDataSize']), '--block-gas-limit', str(cfg['BlockGasLimit']),
                            '--groups', str(cfg['GARETGroupCount']), '--output', str(output)],
                            cwd=root, capture_output=True, text=True, check=True)
                    except subprocess.CalledProcessError as exc:
                        raise CatalogError('GARET 分组准备失败；未启动节点: ' + (exc.stderr or str(exc))[-2000:]) from exc
                    _assert_inputs((tx, calls), data)
                    raw = output.read_bytes()
                    catalog, digest = _validate_catalog(raw, cfg['GARETGroupCount'])
                    try:
                        report = json.loads(result.stdout)
                    except (ValueError, TypeError) as exc:
                        raise CatalogError('GARET 分组工具没有返回有效摘要') from exc
                    if (not isinstance(report, dict) or report.get('catalog_digest') != digest
                            or report.get('group_count') != cfg['GARETGroupCount']
                            or report.get('address_count') != catalog['address_count']
                            or report.get('prefix_transactions') != cfg['TotalDataSize']):
                        raise CatalogError('GARET 分组工具摘要与目录/前缀不一致')
                meta = {'identity': identity, 'file_sha256': sha_bytes(raw), 'catalog_digest': digest}
                _write_atomic(catalog_path, raw)
                _write_atomic(meta_path, encoded(meta))
            prepared[key] = (raw, meta)
        raw, meta = prepared[key]
        _assert_inputs((tx, calls), data)
        job['garet_catalog_bytes'] = raw
        job['garet_catalog'] = dict(meta)
