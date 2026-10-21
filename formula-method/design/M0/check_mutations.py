"""三项有针对性的模型变异检测。临时副本内修改，主模型不变。"""
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

from verify import DEFAULT_PAT

ROOT = Path(__file__).resolve().parent
OUT = ROOT/'results/mutations'
OUT.mkdir(exist_ok=True)
CASES = [
    ('one_remote_ack', 'ClusterContext.csp',
     [(' && (isr2 == 0 || ack2 == 1)]', ']')],
     dict(FAULT=0,ISSUE=1,BACKGROUND=1), '|= [] SafeHW'),
    ('idle_isr_shrink', 'ClusterContext.csp',
     [(' && (tail_advanced == 1 || (append_done == 1 && ack1 == 0))', ''),
      (' && (tail_advanced == 1 || (append_done == 1 && ack2 == 0))', '')],
     dict(FAULT=5,ISSUE=0,BACKGROUND=0), '|= [] IsrIntact'),
    ('no_inflight_timeout', 'FaultEnvironment.csp',
     [('time_client{if (client == 1)', 'time_client{if (client == 1 && bp == 0)')],
     dict(FAULT=4,ISSUE=1,BACKGROUND=1),
     '|= (([] <> time_client) && ([] <> c_poll)) -> [] (Pending -> <> Finished)'),
]


def main():
    results=[]
    for name, filename, replacements, overrides, assertion in CASES:
        with tempfile.TemporaryDirectory(prefix='pat-m0-mutation-') as tmp:
            root=Path(tmp)
            shutil.copytree(ROOT/'model',root/'model')
            (root/'checks').mkdir()
            file=root/'model'/filename
            original=file.read_text(encoding='utf-8-sig')
            altered=original
            for old,new in replacements:
                if old not in altered:
                    raise RuntimeError(f'Mutation pattern not found: {name}')
                altered=altered.replace(old,new)
            file.write_text(altered,encoding='utf-8-sig')
            config=dict(COLD=0,FAULT=0,KIND=1,HAS_DATA=0,RECOVER=0,ISSUE=1,BACKGROUND=1)|overrides
            entry=root/'checks'/f'{name}.csp'
            entry.write_text('\n'.join(f'#define {k} {v};' for k,v in config.items())+'\n#include "..\\model\\System.csp";\n#assert M0() '+assertion+';\n',encoding='utf-8-sig')
            cp=subprocess.run([DEFAULT_PAT,'-csp',str(entry),str(OUT/f'{name}.txt')],capture_output=True,timeout=60)
            console=cp.stdout.decode('utf-8',errors='replace')+cp.stderr.decode('utf-8',errors='replace')
            (OUT/f'{name}.console.txt').write_text(console.rstrip()+'\n',encoding='utf-8')
            log=(OUT/f'{name}.txt').read_text(encoding='utf-8-sig')
            (OUT/f'{name}.txt').write_text(log.rstrip()+'\n',encoding='utf-8')
            caught=cp.returncode==0 and ') is NOT valid.' in log
            results.append(dict(name=name,changed_file=filename,replacements=replacements,config=config,
                                assertion=assertion,detected=caught,
                                original_sha256=hashlib.sha256(original.encode()).hexdigest(),
                                mutant_sha256=hashlib.sha256(altered.encode()).hexdigest()))
            print(name,caught,flush=True)
    (OUT/'summary.json').write_text(json.dumps(results,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if not all(r['detected'] for r in results):
        raise SystemExit('Mutation escaped detection')


if __name__=='__main__':
    main()
