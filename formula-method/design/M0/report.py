"""由当前输入和运行记录生成性质/结果表；拒绝陈旧模型哈希。"""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    props = json.loads((ROOT/'properties.json').read_text(encoding='utf-8'))
    runs = json.loads((ROOT/'results/manifest.json').read_text(encoding='utf-8'))
    for name, digest in runs['model_hashes'].items():
        if sha(ROOT/'model'/name) != digest:
            raise SystemExit(f'Stale model: {name}')
    by_id = {}
    for r in runs['runs']:
        if sha(ROOT/'checks'/f'{r["suite"]}.csp') != r['input_sha256']:
            raise SystemExit(f'Stale input: {r["suite"]}')
        if sha(ROOT/'results'/f'{r["suite"]}.txt') != r['output_sha256']:
            raise SystemExit(f'Stale output: {r["suite"]}')
        for got in r['results']:
            by_id[got['id']] = got
    if set(by_id) != {p['id'] for p in props}:
        raise SystemExit('Incomplete property coverage')
    valid = sum(g['result']=='VALID' for g in by_id.values())
    invalid = len(by_id)-valid
    lines = ['# M0-R2 验证性质与实际结果', '',
             f'PAT 3.5.1；运行归档 UTC 时间 `{runs["run_at"]}`。共 **{len(runs["runs"])} 个入口、{len(props)} 条断言：{valid} VALID，{invalid} NOT valid**。', '',
             'S00 是七项安全条件的合取，只执行一次，并不重复计为七条。每个入口只包含当前组合的断言，无旧入口断言继承。', '',
             'NOT valid 包括预先声明的健康契约候选、错误业务推断和一个不加公平性的调度负对照，不能把条数称作已确认源码缺陷数量。', '',
             '`reaches` 的 VALID 只表示存在路径；LTL 的 VALID 表示在声明的有限模型及前提内未发现反例。结果解释见 [FINDINGS.md](FINDINGS.md)。', '',
             '## 运行汇总', '',
             '| 入口 | VALID / NOT valid | 最大访问状态数 | 入口墙钟秒 | 日志 |',
             '|---|---:|---:|---:|---|']
    for r in runs['runs']:
        v=sum(g['result']=='VALID' for g in r['results'])
        maxstates=max(int(g['states'] or 0) for g in r['results'])
        lines.append(f'| [{r["suite"]}](checks/{r["suite"]}.csp) | {v} / {len(r["results"])-v} | {maxstates:,} | {r["wall_seconds"]} | [原始结果](results/{r["suite"]}.txt) |')
    lines += ['', '## 性质逐项解释', '',
              '| ID | 场景 | 要验证的内容 | 预期 | 实测 |',
              '|---|---|---|---|---|']
    for p in props:
        got=by_id[p['id']]
        lines.append(f'| {p["id"]} | {p["suite"]} | {p["explanation"]} | {p["expected"]} | {got["result"]} |')
    lines += ['', '## 精确断言', '']
    for p in props:
        lines += [f'### {p["id"]} — {p["suite"]}', '', p['explanation'], '',
                  '```text', p['assertion'], '```', '']
    lines += ['## 复核与限制', '',
              '- [运行清单](results/manifest.json)保存模型和输入/输出哈希；[properties.json](properties.json)保存配置与预期结果。',
              '- 被动观察器用于 N02/N03、D03、Q02/Q03 的先后关系，仅记录历史，不改变角色状态或同步字母表。',
              '- 首次 WholeSafety 将七项单独枚举，在 180 秒时限内只完成 S01；[原始不完整记录](results/attempts/WholeSafety-before-grouping.run.json)保留。现改成合取，共享一次搜索。未把超时当作通过。',
              '- [变异检测记录](results/mutations/summary.json)检查性质能否发现刻意引入的单 ACK、空闲缩 ISR 和在途不超时错误；它们属于模型质量核查，不是发现了三个实现缺陷。',
              '- 不宣称秒级漏检时长、真实全 Pod 隔离时的选举结果、无限重试、多个请求/分区或故障叠加的结论。', '']
    (ROOT/'VERIFICATION.md').write_text('\n'.join(lines),encoding='utf-8')
    # 提取实际路径，原样保存并另给去掉无状态变化调度自循环的可读版本。
    trace_lines=['# PAT 实际路径摘录', '', '以下原始路径全部来自本轮 PAT 结果，保留时间/线程无操作调度事件。事件个数不能用来估计真实时间。', '']
    for p in props:
        g=by_id[p['id']]
        if not g['trace']:
            continue
        trace_lines += [f'## {p["id"]} — {p["explanation"]}', '', f'结果：{g["result"]}；[原始日志](results/{p["suite"]}.txt)。', '', '```text',g['trace'],'```','']
    (ROOT/'TRACES.md').write_text('\n'.join(trace_lines),encoding='utf-8')
    print(f'{len(props)} assertions: {valid} VALID / {invalid} NOT valid')


if __name__ == '__main__':
    main()
