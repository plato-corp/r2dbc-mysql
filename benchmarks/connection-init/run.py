import json, os, pathlib, statistics, subprocess
root = pathlib.Path(__file__).resolve().parents[2]
cp = (root / 'r2dbc-mysql/target/benchmark-classpath.txt').read_text().strip()
rows = []
out = root / 'benchmark-results'
out.mkdir(exist_ok=True)
for mode in ('sql', 'connect'):
    for concurrency in (1, 16, 64):
        for repeat, variant in enumerate(['baseline', 'patched', 'patched', 'baseline'] * 3):
            jar = root / 'benchmark-jars' / (variant + '.jar' if mode == 'connect' else 'baseline.jar')
            classpath = os.pathsep.join([str(root / 'benchmarks/connection-init'), str(jar), cp])
            result = subprocess.run(['java', '-Xms256m', '-Xmx256m', '-Dreactor.netty.ioWorkerCount=4',
                '-cp', classpath, 'ConnectionInitBenchmark', variant, str(concurrency),
                '512' if mode == 'connect' else '4096', mode], text=True, capture_output=True, timeout=240)
            (out / f'{mode}-{concurrency}-{repeat}-{variant}.log').write_text(result.stdout + result.stderr)
            if result.returncode: raise RuntimeError(result.stdout[-2000:] + result.stderr[-2000:])
            row = json.loads(next(l[7:] for l in result.stdout.splitlines() if l.startswith('RESULT ')))
            row['repeat'] = repeat
            rows.append(row)
            (out / 'results.json').write_text(json.dumps(rows, indent=2))
            print(row, flush=True)
summary = []
for mode in ('sql', 'connect'):
    for concurrency in (1, 16, 64):
        groups = {v: [r for r in rows if r['mode'] == mode and r['concurrency'] == concurrency
            and r['variant'] == v] for v in ('baseline', 'patched')}
        assert all(len(g) == 6 and all(r['errors'] == 0 for r in g) for g in groups.values())
        item = {'mode': mode, 'concurrency': concurrency}
        for metric in ('ops_s', 'p50_ms', 'p95_ms', 'p99_ms'):
            a, b = (statistics.median(r[metric] for r in groups[v]) for v in ('baseline', 'patched'))
            item[metric] = {'baseline_median': a, 'patched_median': b, 'change_percent': (b/a-1)*100}
        summary.append(item)
(out / 'summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))
