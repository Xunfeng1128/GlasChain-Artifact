"""Validate shared CLPA control opt-ins without changing legacy defaults."""


def uses_shared_clpa_reconfiguration(method, scheduler):
    # Keep this dispatch matrix identical to params.UsesSharedCLPAReconfiguration.
    if type(method) is not int or scheduler not in ('event', 'poll'):
        return False
    return method in (1, 4, 12, 13) or scheduler == 'poll' and method in (7, 8, 9, 10, 11)


def validate_clpa_scheduler_config(cfg):
    # Match Go's optional *string field: absent/null selects the current event
    # behaviour. Do not mutate caller configs or add a key to historical files.
    value = cfg.get('CLPAReconfigurationScheduler')
    if value is None:
        value = 'event'
    if type(value) is not str or value not in ('event', 'poll'):
        raise ValueError('CLPAReconfigurationScheduler 必须为 event 或 poll')
    if value == 'poll' and not uses_shared_clpa_reconfiguration(cfg.get('ConsensusMethod'), value):
        raise ValueError('CLPAReconfigurationScheduler=poll 要求受支持的公共 CLPA 重配置模式')

    profile = cfg.get('CLPAReconfigurationProfile')
    if profile is None:
        profile = 'install_clock'
    if type(profile) is not str or profile not in ('fixed', 'block_resume', 'install_clock'):
        raise ValueError('CLPAReconfigurationProfile 必须为 fixed、block_resume 或 install_clock')
    method = cfg.get('ConsensusMethod')
    if profile != 'install_clock' and not uses_shared_clpa_reconfiguration(method, value):
        raise ValueError('CLPAReconfigurationProfile=' + profile + ' 要求已启用公共 CLPA 重配置流程；新增基线须显式选择 poll')
    # Existing callers depend on the scheduler return value.
    return value
