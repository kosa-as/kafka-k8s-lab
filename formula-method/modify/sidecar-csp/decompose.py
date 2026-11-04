"""Check S00/P01 under each single-fault selector without changing formulas."""
import argparse, hashlib, json, re, subprocess, time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PAT=r'C:\Program Files\Process Analysis Toolkit\Process Analysis Toolkit 3.5.1\PAT3.Console.exe'
FAULTS=[0,2,3,4,5,6,7,8]

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--timeout',type=int,default=30);ap.add_argument('--id',action='append');args=ap.parse_args()
    targets=args.id or ['S00','P01']
    for target in targets:
        if target not in ['S00','P01']:raise SystemExit('Only S00/P01 supported')
        original=(ROOT/'checks'/f'{target}.csp').read_text(encoding='utf-8')
        if '#define FAULT -1;' not in original:raise SystemExit('Expected unrestricted fault selector')
        outdir=ROOT/'results'/'decomposed';checkdir=ROOT/'checks'/'decomposed'
        outdir.mkdir(exist_ok=True);checkdir.mkdir(exist_ok=True)
        for fault in FAULTS:
            name=f'{target}_F{fault}';source=checkdir/f'{name}.csp';out=outdir/f'{name}.txt'
            source.write_text(original.replace('#define FAULT -1;',f'#define FAULT {fault};')
                .replace('#include "..\\model\\System.csp";','#include "..\\..\\model\\System.csp";'),encoding='utf-8')
            if out.exists():out.unlink()
            start=time.monotonic()
            try:
                cp=subprocess.run([PAT,'-csp',str(source),str(out)],capture_output=True,timeout=args.timeout)
                raw=out.read_text(encoding='utf-8-sig') if out.exists() else ''
                found=re.findall(r'The Assertion \(.+\) is (VALID|NOT valid)\.',raw)
                status='completed' if cp.returncode==0 and len(found)==1 else 'error'
                result=found[0] if status=='completed' else None
                console=(cp.stdout+cp.stderr).decode('utf-8',errors='replace')
            except subprocess.TimeoutExpired:
                status,result,console='timeout',None,'Time limit reached; not a verdict.'
            record=dict(id=target,fault=fault,status=status,result=result,seconds=round(time.monotonic()-start,3),
                        formula=original.split('#assert ',1)[1].split(';',1)[0],
                        input_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),console=console[:300])
            (outdir/f'{name}.json').write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
            if status!='completed' and out.exists():out.unlink()
            print(name,status,result,record['seconds'],flush=True)
if __name__=='__main__':main()
