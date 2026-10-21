"""逐入口运行 PAT；检查解析错误、结果数量及预期结果，记录哈希和原始输出。"""
import argparse
import datetime as dt
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_PAT = r'C:\Program Files\Process Analysis Toolkit\Process Analysis Toolkit 3.5.1\PAT3.Console.exe'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pat', default=DEFAULT_PAT)
    ap.add_argument('--suite', action='append', help='仅运行指定入口，可重复使用')
    ap.add_argument('--timeout', type=int, default=240)
    args = ap.parse_args()
    props = json.loads((ROOT/'properties.json').read_text(encoding='utf-8'))
    suites = list(dict.fromkeys(p['suite'] for p in props))
    if args.suite:
        unknown = set(args.suite)-set(suites)
        if unknown:
            raise SystemExit(f'Unknown suites: {unknown}')
        suites = [s for s in suites if s in args.suite]
    records = []
    failures = []
    model_hashes = {p.name:sha(p) for p in sorted((ROOT/'model').glob('*.csp'))}
    for suite in suites:
        output = ROOT/'results'/f'{suite}.txt'
        console = ROOT/'results'/f'{suite}.console.txt'
        if output.exists():
            output.unlink()  # 仅清除此入口的旧输出，防止误认历史结果。
        t0 = time.monotonic()
        print(f'RUN {suite}', flush=True)
        status = 'completed'
        rc = None
        try:
            cp = subprocess.run([args.pat, '-csp', str(ROOT/'checks'/f'{suite}.csp'), str(output)],
                                capture_output=True, timeout=args.timeout)
            text = cp.stdout.decode('utf-8', errors='replace') + cp.stderr.decode('utf-8', errors='replace')
            rc = cp.returncode
        except subprocess.TimeoutExpired as e:
            status = 'timeout'
            text = (e.stdout or b'').decode('utf-8', errors='replace')+'\nTIMEOUT\n'
        console.write_text(text.rstrip()+'\n', encoding='utf-8')
        raw = output.read_text(encoding='utf-8-sig') if output.exists() else ''
        if output.exists():
            output.write_text(raw.rstrip()+'\n', encoding='utf-8')
        blocks = re.split(r'={20,}\s*\n', raw)
        parsed = []
        for block in blocks:
            m = re.search(r'The Assertion \(.+\) is (NOT valid|VALID)\.', block)
            if not m:
                continue
            def field(pattern):
                match = re.search(pattern, block)
                return match.group(1) if match else None
            parsed.append(dict(result=m.group(1), states=field(r'Visited States:(\d+)'),
                               seconds=field(r'Time Used:([\d.]+)s'),
                               trace=field(r'(<init[^\n]*)')))
        expected = [p for p in props if p['suite'] == suite]
        okay = status == 'completed' and rc == 0 and len(parsed) == len(expected)
        for p, got in zip(expected, parsed):
            got['id'] = p['id']
            got['matches_expected'] = got['result'] == p['expected']
            okay &= got['matches_expected']
        if not okay:
            failures.append(suite)
        record = dict(suite=suite, status=status, exit_code=rc, expected_count=len(expected),
                      results=parsed, matches_expected=bool(okay), wall_seconds=round(time.monotonic()-t0,3),
                      input_sha256=sha(ROOT/'checks'/f'{suite}.csp'),
                      output_sha256=sha(output) if output.exists() else None)
        records.append(record)
        (ROOT/'results'/f'{suite}.run.json').write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print(f'{suite}: {len(parsed)}/{len(expected)} results; expected={bool(okay)}; {record["wall_seconds"]}s', flush=True)
    manifest = dict(run_at=dt.datetime.now(dt.timezone.utc).isoformat(), pat=str(Path(args.pat)),
                    pat_sha256=sha(Path(args.pat)), suites=suites, runs=records,
                    model_hashes=model_hashes)
    name = 'manifest.json' if not args.suite else 'partial-manifest.json'
    (ROOT/'results'/name).write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    # 部分重跑只在模型哈希和其余入口哈希均一致时合并，避免重算未变更的大状态空间。
    if args.suite and not failures and (ROOT/'results/manifest.json').exists():
        previous=json.loads((ROOT/'results/manifest.json').read_text(encoding='utf-8'))
        unchanged=[r for r in previous['runs'] if r['suite'] not in suites]
        if previous['model_hashes'] != model_hashes or any(
            sha(ROOT/'checks'/f'{r["suite"]}.csp') != r['input_sha256'] for r in unchanged):
            raise SystemExit('Cannot merge partial results: unchanged input/model hashes differ')
        combined={r['suite']:r for r in unchanged+records}
        all_suites=list(dict.fromkeys(p['suite'] for p in props))
        manifest['suites']=all_suites
        manifest['runs']=[combined[s] for s in all_suites]
        manifest['note']='Partial rerun merged only after checking unchanged input and model hashes.'
        (ROOT/'results/manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if failures:
        raise SystemExit('Unexpected/incomplete: '+', '.join(failures))


if __name__ == '__main__':
    main()
