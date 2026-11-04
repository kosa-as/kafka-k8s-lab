"""Generate unmodified M0 assertions against M1, then run PAT with bounded time.
Default: the four target contracts and small independent regression checks.
"""
import argparse
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = ROOT.parents[1] / 'design' / 'M0'
PAT = r'C:\Program Files\Process Analysis Toolkit\Process Analysis Toolkit 3.5.1\PAT3.Console.exe'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--id', action='append')
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--resume', action='store_true',help='reuse completed checks with matching input/model hashes')
    ap.add_argument('--timeout', type=int, default=30)
    ap.add_argument('--pat', default=PAT)
    args = ap.parse_args()
    old = json.loads((BASE / 'properties.json').read_text(encoding='utf-8'))
    selected = set(args.id or ['B01','B02','B03','E02','D02','R04','H01','H02','I01','I02'])
    if selected - {p['id'] for p in old}:
        raise SystemExit('Unknown property ID')
    for name in ['checks','results']:
        (ROOT / name).mkdir(exist_ok=True)
    hashes = {str(p.relative_to(ROOT.parents[1])): sha(p)
              for folder in [ROOT / 'model', BASE / 'model'] for p in sorted(folder.glob('*.csp'))}
    for p in old:
        source = (BASE / 'checks' / f"{p['suite']}.csp").read_text(encoding='utf-8-sig')
        prefix = source.split('#assert ',1)[0]
        assertions = re.findall(r'^#assert (.+);$',source,re.M)
        siblings = [x for x in old if x['suite']==p['suite']]
        index = next(i for i,x in enumerate(siblings) if x['id']==p['id'])
        assert assertions[index] == p['assertion']
        prefix = prefix.replace('M0()', 'M1()').replace('#include "..\\model\\System.csp";',
                 '#define N 2;\n#define R 2;\n#define LOSS 0;\n#include "..\\model\\System.csp";')
        check = ROOT / 'checks' / f"{p['id']}.csp"
        check.write_text(prefix+'#assert '+p['assertion'].replace('M0()','M1()')+';\n',encoding='utf-8')
        if not args.all and p['id'] not in selected:
            continue
        previous=ROOT/'results'/f"{p['id']}.json"
        if args.resume and previous.exists():
            saved=json.loads(previous.read_text(encoding='utf-8'))
            if saved['status']=='completed' and saved['model_hashes']==hashes and saved['input_sha256']==sha(check):
                print(p['id'],'reused',saved['m1'],flush=True)
                continue
        output = ROOT / 'results' / f"{p['id']}.txt"
        if output.exists():
            output.unlink()
        start = time.monotonic()
        status = 'completed'
        try:
            cp = subprocess.run([args.pat,'-csp',str(check),str(output)],capture_output=True,timeout=args.timeout)
            console = (cp.stdout+cp.stderr).decode('utf-8',errors='replace')
            code = cp.returncode
        except subprocess.TimeoutExpired:
            status, console, code = 'timeout', 'Time limit reached; not a verdict.', None
        raw = output.read_text(encoding='utf-8-sig') if output.exists() else ''
        results = re.findall(r'The Assertion \(.+\) is (VALID|NOT valid)\.',raw)
        if status=='completed' and (code!=0 or len(results)!=1):
            status='error'
        verdict=results[0] if status=='completed' else None
        if status!='completed' and output.exists():
            output.unlink()
        record=dict(id=p['id'],suite=p['suite'],m0=p['expected'],m1=verdict,status=status,
                    original_assertion=p['assertion'],model_hashes=hashes,input_sha256=sha(check),
                    pat_sha256=sha(Path(args.pat)),seconds=round(time.monotonic()-start,3),console=console)
        (ROOT/'results'/f"{p['id']}.json").write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print(p['id'],p['expected'],'->',verdict or status,record['seconds'],flush=True)
    lines=['# 原公式验证结果','', '| ID | M0 | M1 | 状态 |','|---|---|---|---|']
    for p in old:
        result=ROOT/'results'/f"{p['id']}.json"
        if result.exists():
            x=json.loads(result.read_text(encoding='utf-8'))
            current=x['model_hashes']==hashes and x['input_sha256']==sha(ROOT/'checks'/f"{p['id']}.csp")
            state=x['status'] if current else 'stale'
            lines.append(f"| {p['id']} | {p['expected']} | {x['m1'] if current and x['m1'] else '—'} | {state} |")
        else:
            lines.append(f"| {p['id']} | {p['expected']} | — | 未运行 |")
    (ROOT/'REPORT.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')


if __name__=='__main__':
    main()
