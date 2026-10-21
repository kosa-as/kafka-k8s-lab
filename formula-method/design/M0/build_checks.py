"""生成可直接由 PAT 打开的检查入口和性质清单；不修改模型。"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = dict(COLD=0, FAULT=0, KIND=1, HAS_DATA=0, RECOVER=0, ISSUE=1, BACKGROUND=1)
FAIR = '(([] <> time_client) && ([] <> c_poll)) -> [] (Pending -> <> Finished)'
SUITES = {
    'Healthy': ({}, [
        ('H01', 'reaches ProduceSuccess', True, '存在从 send 到应用成功的完整路径。'),
        ('H02', 'reaches TwoAckNeeded', True, 'ISR=3 时仅一个远端追平尚不足以推进 HW。'),
        ('H03', '|= [] SafeHW', True, '提交历史满足追加、全部当前 ISR 确认与 minISR。'),
        ('H04', '|= [] SafeSuccess', True, '应用成功必须有提交事实及 Broker 成功授权。'),
        ('H05', '|= ' + FAIR, True, '时间与客户端线程持续推进时，每个操作最终形成结果，结果允许超时。'),
        ('H06', '|= [] (Pending -> <> Finished)', False, '调度负对照：不假设公平性时可永远只调度探针；此失败不是源码缺陷。'),
    ]),
    'WholeSafety': ({'FAULT': -1, 'RECOVER': 1}, [
        ('S00', '|= ([] (SafeHW && SafeSuccess && Bounds && AppendLockSafe && (Ready -> Started) && AfterAppendNotAssumed)) && ([] (ProduceSuccess -> [] ProduceSuccess))', True,
         'S01 提交条件、S02 成功来源、S03 数值范围、S04 分区锁、S05 Startup 门、S06 AFTER_APPEND 前提、S07 已成功结果稳定性：七项合取共享一次全空间搜索；通过蕴含每项通过。'),
    ]),
    'WholeProgress': ({'FAULT': -1, 'RECOVER': 0}, [
        ('P01', '|= ' + FAIR, True, '七角色单故障组合：明确公平前提下最终返回成功或失败。'),
        ('P02', 'reaches Restarted', True, '进程退出可以由 Kubelet 观察并重启。'),
    ]),
    'Startup': ({'COLD': 1, 'ISSUE': 0}, [
        ('B01', 'reaches ReadyBeforeEnable', True, 'Controller TCP 已开放、Broker 处理尚未启用时，Ready 可先成立。'),
        ('B02', '|= [] (Ready -> Enabled)', False, '候选健康契约：Ready 应蕴含 Broker 处理启用；无时间模型给出反例。'),
        ('B03', '|= [] (Ready -> Started)', True, '冷启动路径仍遵守 Startup 门。'),
    ]),
    'ClientNetwork': ({'FAULT': 2, 'RECOVER': 1}, [
        ('N01', 'reaches ReadyClientTimeout', True, '新 RP/LP 均至少成功两次，客户端仍可因独立链路故障交付超时。'),
        ('N02', 'reaches OrderedCommittedTimeout', True, '先观察已提交、调用尚未结束且链路中断，随后客户端才交付超时。'),
        ('N03', 'reaches OrderedLateResponse', True, '先已提交且链路中断，随后超时，最后恢复并送达迟到响应。'),
        ('N04', '|= [] (ProduceTimeout -> !Committed)', False, '错误的业务推断：超时就代表消息未提交。'),
        ('N05', 'reaches Recovered', True, '链路及时恢复后仍存在成功路径。'),
        ('N06', '|= [] (ProduceTimeout -> [] ProduceTimeout)', True, '终态稳定：迟到响应不会把已经超时的调用改成成功。'),
    ]),
    'Execution': ({'FAULT': 3}, [
        ('E01', 'reaches ExecutionPendingBlind', True, '请求已接收且仍等应用结果、执行停滞，新 LP 成功两次而 Ready 保持。'),
        ('E02', '|= [] (lp_sample -> ExecutionCanProgress)', False, '候选健康契约：pgrep 成功应代表处理线程可推进；该蕴含不成立。'),
    ]),
    'Storage': ({'FAULT': 4}, [
        ('D01', 'reaches StoragePendingBlind', True, '请求仍等应用结果且持锁等待存储，新 LP 成功两次而 Ready 保持。'),
        ('D02', '|= [] (Ready -> StorageCanProgress)', False, '候选健康契约：Ready 应蕴含存储可推进。'),
        ('D03', 'reaches OrderedTimeoutInIO', True, '先观察调用仍在等待且 Broker 困于 I/O，随后 Producer 独立交付超时。'),
    ]),
    'Replica': ({'FAULT': 5}, [
        ('R01', 'reaches ReplicaBlind', True, '存在追加和超期未追平条件时，ISR 降至 1，持续 RP 成功。'),
        ('R02', 'reaches IsrRejected', True, 'Ready 状态下客户端实际收到 ISR 不足协议错误。'),
        ('R03', 'reaches AfterAppendError', True, 'HW 已覆盖记录后、检查 DelayedProduce 前 ISR 缩小，可返回 AFTER_APPEND。'),
        ('R04', '|= [] (Ready -> MinIsrSatisfied)', False, '候选健康契约：Ready 蕴含本地分区满足最低 ISR。'),
        ('R05', '|= [] (BrokerError -> !Committed)', False, '错误的业务推断：Broker 返回错误就代表记录未提交。'),
    ]),
    'IdleReplica': ({'FAULT': 5, 'ISSUE': 0, 'BACKGROUND': 0}, [
        ('I01', '|= [] IsrIntact', True, '完全空闲且 LEO 已追平：只断复制链路不会因落后检查缩 ISR。'),
        ('I02', '|= [] (!UnderMin)', True, '负对照中 ISR=1 不可达；防止断网直接等同 ISR 不足的伪反例。'),
    ]),
    'ProbeNetwork': ({'FAULT': 6}, [
        ('Q01', 'reaches ProbeHysteresis', True, 'Readiness 连续失败未到六次时缓存 Ready 可以保留。'),
        ('Q02', 'reaches OrderedNotReadySuccess', True, '先在 NotReady 状态观察仍等待的调用和成功响应，随后调用返回成功。'),
        ('Q03', '|= [] (!OrderedNotReadySuccess)', False, '反向健康契约：NotReady 以后不应还能完成业务成功；独立路径可反驳。'),
        ('Q04', '|= [] (ReadinessThreshold -> !Ready)', True, '六次已消费的连续失败后确实清除 Ready。'),
    ]),
    'FetchData': ({'KIND': 2, 'HAS_DATA': 1}, [
        ('F01', 'reaches PollData', True, '健康已提交数据存在时可以完成有数据的 poll。'),
        ('F02', '|= [] (PollData -> DataAvailable)', True, '不能把不存在的选中数据返回客户端。'),
    ]),
    'FetchEmpty': ({'KIND': 2, 'HAS_DATA': 0}, [
        ('F03', 'reaches PollEmpty', True, '无目标数据时 poll 正常返回空集合。'),
        ('F04', '|= [] (!PollData)', True, '没有数据来源的场景不能产生有数据结果。'),
    ]),
    'FetchNetwork': ({'FAULT': 2, 'KIND': 2, 'HAS_DATA': 1}, [
        ('F05', 'reaches EmptyWithData', True, 'Broker 存有数据但 poll 截止时仍可无数据返回。'),
        ('F06', '|= [] (PollEmpty -> !DataAvailable)', False, '错误的业务推断：poll 返回空集合代表 Broker 没有数据。'),
        ('F07', '|= ' + FAIR, True, '时间与客户端线程公平推进时 poll 有终态，允许空集合。'),
    ]),
    'Metadata': ({'FAULT': 8, 'KIND': 2, 'HAS_DATA': 1}, [
        ('M01', 'reaches RemoteError', True, 'Leader 迁离后旧路由 Fetch 可收到协议错误并内部重试。'),
        ('M02', '|= [] AppendLockSafe', True, '元数据迁移不能在本地读取持锁期间令 I/O 悬空。'),
    ]),
}

PROPS = '''
#define ExecutionCanProgress exec_ok == 1;
#define StorageCanProgress disk_ok == 1;
#define MinIsrSatisfied isr1 + isr2 >= 1;
#define BrokerError response == 2 || response == 3 || response == 4 || response == 7;
#define TimeoutInIO client == 3 && bp == 3 && disk_ok == 0;
#define ReadinessThreshold rp_fail == 6;
#define DataAvailable data_available == 1;
#define EmptyWithData client == 5 && data_available == 1 && fault == 2 && ready == 1;
'''

# 被动观察器仅用于记录先后关系，不限制任何角色事件，观测事件不修改模型状态。
# 避免“超时后才完成提交”“先成功后来 NotReady”冒充更强的目标场景。
OBSERVERS = {
    'ClientNetwork': '''
var observed_commit_pending_cut = 0;
ObserveNetwork() = [hw == 1 && response == 1 && client == 1 && client_expired == 0 && client_net == 0]
  observe_committed_pending_cut{observed_commit_pending_cut = 1;} -> ObserveTimeout();
ObserveTimeout() = [client == 3 && client_net == 0]
  observe_timeout_during_cut{observed_commit_pending_cut = 2;} -> Stop;
Observed() = M0() ||| ObserveNetwork();
#define OrderedCommittedTimeout observed_commit_pending_cut >= 1 && client == 3;
#define OrderedLateResponse observed_commit_pending_cut == 2 && client == 3 && bp == 6 && fault_budget == 2;
''',
    'Storage': '''
#define StoragePendingBlind client == 1 && client_expired == 0 && bp == 3 && disk_ok == 0 && fresh_lp == 2 && ready == 1;
var observed_pending_io = 0;
ObserveIO() = [bp == 3 && client == 1 && client_expired == 0 && disk_ok == 0]
  observe_pending_io{observed_pending_io = 1;} -> Stop;
Observed() = M0() ||| ObserveIO();
#define OrderedTimeoutInIO observed_pending_io == 1 && client == 3 && bp == 3;
''',
    'ProbeNetwork': '''
var observed_notready_pending = 0;
ObserveNotReady() = [ready == 0 && client == 1 && rx == 1]
  observe_notready_pending{observed_notready_pending = 1;} -> Stop;
Observed() = M0() ||| ObserveNotReady();
#define OrderedNotReadySuccess observed_notready_pending == 1 && client == 2 && ready == 0;
''',
}


def main():
    manifest = []
    for suite, (overrides, props) in SUITES.items():
        config = BASE | overrides
        lines = ['// M0-R2 验证入口；由 build_checks.py 生成。模型文件不携带断言。']
        lines += [f'#define {k} {v};' for k, v in config.items()]
        lines += ['#include "..\\model\\System.csp";', PROPS]
        if suite in OBSERVERS:
            lines.append(OBSERVERS[suite])
        if suite == 'Execution':
            lines.append('#define ExecutionPendingBlind client == 1 && client_expired == 0 && bp == 1 && exec_ok == 0 && fresh_lp == 2 && ready == 1;')
        system = 'Observed()' if suite in OBSERVERS else 'M0()'
        for pid, assertion, expected, explanation in props:
            lines += [f'// {pid}：{explanation}', f'#assert {system} {assertion};', '']
            manifest.append(dict(id=pid, suite=suite, assertion=f'{system} {assertion}',
                                 expected='VALID' if expected else 'NOT valid',
                                 explanation=explanation, config=config))
        (ROOT / 'checks' / f'{suite}.csp').write_text('\n'.join(lines), encoding='utf-8-sig')
    (ROOT / 'properties.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(f'{len(SUITES)} entries, {len(manifest)} assertions')


if __name__ == '__main__':
    main()
