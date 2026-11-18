"""Run supplementary PAT entries and record current model/input provenance.

An unfinished run may contain completed assertions; these are reported separately
from assertions PAT did not finish. No verdict is inferred from process exit alone.
"""
import argparse
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path

from verify import BASE, PAT, ROOT, sha


def run(name, timeout, pat):
    check = ROOT / 'checks' / f'{name}.csp'
    output = ROOT / 'results' / f'{name}.txt'
    hashes = {str(p.relative_to(ROOT.parents[1])): sha(p)
              for folder in [ROOT / 'model', BASE / 'model']
              for p in sorted(folder.glob('*.csp'))}
    expected_count = len(re.findall(r'^#assert ', check.read_text(encoding='utf-8'), re.M))
    output.unlink(missing_ok=True)
    started = time.monotonic()
    status, code, console = 'completed', None, ''
    try:
        cp = subprocess.run([pat, '-csp', str(check), str(output)],
                            capture_output=True, timeout=timeout)
        code = cp.returncode
        console = (cp.stdout + cp.stderr).decode('utf-8', errors='replace')
    except subprocess.TimeoutExpired:
        status, console = 'timeout', 'Time limit reached; unfinished assertions have no verdict.'
    raw = output.read_text(encoding='utf-8-sig') if output.exists() else ''
    matches = re.findall(r'The Assertion \((.+)\) is (VALID|NOT valid)\.', raw)
    if status == 'completed' and (code != 0 or len(matches) != expected_count):
        status = 'error'
    states = re.findall(r'Visited States:\s*([\d,]+)', raw)
    times = re.findall(r'Time Used:\s*([\d.]+)s', raw)
    assertions = [dict(assertion=a, verdict=v,
                       visited_states=int(states[i].replace(',', '')) if i < len(states) else None,
                       seconds=float(times[i]) if i < len(times) else None)
                  for i, (a, v) in enumerate(matches)]
    record = dict(id=name, status=status, expected_count=expected_count,
                  completed_count=len(assertions), assertions=assertions,
                  model_hashes=hashes, input_sha256=sha(check),
                  pat_sha256=sha(Path(pat)), timeout_seconds=timeout,
                  seconds=round(time.monotonic() - started, 3), console=console)
    (ROOT / 'results' / f'{name}.run.json').write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(name, status, f'{len(assertions)}/{expected_count}',
          [a['verdict'] for a in assertions], record['seconds'], flush=True)
    return record


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--id', action='append')
    ap.add_argument('--timeout', type=int, default=60)
    ap.add_argument('--pat', default=PAT)
    args = ap.parse_args()
    names = args.id or [p.stem for p in sorted((ROOT / 'checks').glob('PerMetric*.csp'))]
    (ROOT / 'results').mkdir(exist_ok=True)
    for name in names:
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', name):
            raise SystemExit(f'Invalid check name: {name}')
        run(name, args.timeout, args.pat)


if __name__ == '__main__':
    main()
