"""Validate the formal GlasChain configuration before starting any nodes."""


def formal_glas_enabled(cfg):
    return (
        cfg.get('ConsensusMethod') == 12
        and cfg.get('GLASPendingFullWindowEarly') is True
        and type(cfg.get('ExperimentEpochCount')) is int and cfg['ExperimentEpochCount'] == 0
        and type(cfg.get('ExperimentBlocksPerEpoch')) is int and cfg['ExperimentBlocksPerEpoch'] == 6
    )


def validate_glas_pending_config(cfg):
    selected = {
        'GLASPendingPlanningSource': 'snapshot',
        'GLASPendingHeadroomRatio': 0,
        'GLASPendingEarlyReport': False,
        'GLASPendingLocalSearch': False,
        'GLASPendingSnapshotPolicy': 'local_window',
        'GLASPendingExecutionPolicy': 'window_first',
        'GLASPendingSnapshotTiming': 'after_last_commit',
        'GLASPendingWindowPolicy': 'queue_gas',
        'GLASPendingWindowRatio': 1.3,
        'GLASPendingMaxRecords': 0,
        'GLASPendingMaxCallRelations': 0,
        'GLASPendingObjectiveWeight': 0.5,
        'GLASPendingFennelGamma': 1.5,
        'GLASPendingFennelRestarts': 2,
        'GLASPendingMaxMoves': 0,
    }
    if not formal_glas_enabled(cfg):
        raise ValueError('GlasChain requires early full-window reporting and six ordinary blocks per epoch')
    for key, expected in selected.items():
        actual = cfg.get(key, expected)
        if (type(actual) is bool and type(expected) is not bool) or actual != expected:
            raise ValueError(f'GlasChain requires {key}={expected!r}')
    if cfg.get('GLASPendingEarlyReport', False) is not False or cfg.get('GLASPendingLocalSearch', False) is not False:
        raise ValueError('GlasChain requires boolean false for historical mode flags')
    if any(cfg.get(k) is not True for k in ('UnifiedExecution', 'UnifiedMetrics', 'UnifiedPackingOptimized')):
        raise ValueError('GlasChain requires unified execution, metrics and optimized packing')
    if (cfg.get('CLPAReconfigurationScheduler'), cfg.get('CLPAReconfigurationProfile')) != ('poll', 'install_clock'):
        raise ValueError('GlasChain requires poll/install_clock')
    if cfg.get('BlockCapacityMode') != 'gas':
        raise ValueError('GlasChain requires Gas-based packing')
    for key in ('BlockGasLimit', 'Block_Interval', 'ReconfigTimeGap', 'TotalDataSize', 'InjectSpeed', 'TxBatchSize'):
        if type(cfg.get(key)) is not int or cfg[key] <= 0:
            raise ValueError(f'GlasChain requires a positive integer {key}')
    if cfg['BlockGasLimit'] >= (2**64 - 1) / (6 * 1.3):
        raise ValueError('GlasChain planning budget overflows uint64')
    for key in ('UnifiedTransactionsFile', 'UnifiedCallsFile'):
        if not isinstance(cfg.get(key), str) or not cfg[key].strip():
            raise ValueError(f'GlasChain requires {key}')
    timeout = cfg.get('GLASPendingSnapshotTimeoutSeconds', 120)
    cutoff = cfg.get('E2ObservationSeconds', 0)
    max_seconds = (2**63 - 1) // 10**9
    if type(timeout) is not int or not 0 < timeout <= max_seconds:
        raise ValueError('GlasChain snapshot timeout must be a positive number of seconds')
    if type(cutoff) is not int or not 0 <= cutoff <= max_seconds or cfg.get('E2PrefixCount', 0) or cfg.get('E2PrefixFile', ''):
        raise ValueError('GlasChain uses either full drain or a fixed observation cutoff')
