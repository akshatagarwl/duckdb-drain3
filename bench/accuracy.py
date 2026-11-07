"""Loghub-2k accuracy of drain_parse vs logpai/Drain3, using the data and the
logpai runner of the drain3 crate's harness
(https://github.com/akshatagarwl/drain3/tree/main/bench/compare).

  DRAIN3_BENCH=~/drain3/bench/compare python3 bench/accuracy.py

Prints per-dataset GA/PA and whether the result is identical to logpai/Drain3
(same partition of lines and the same final templates).
"""
import os, subprocess, sys
from collections import Counter, defaultdict

B = os.path.expanduser(os.environ.get("DRAIN3_BENCH", "~/workspace/drain3/bench/compare"))
sys.path.insert(0, B)
from settings import SETTINGS  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
RUNNERS = {
    "logpai/Drain3 0.9.11": [os.path.join(B, ".venv/bin/python"), os.path.join(B, "impls/python/run.py")],
    "drain3 DuckDB extension": [sys.executable, os.path.join(HERE, "runner.py")],
}


def parse_output(out):
    ids, _, rest = out.partition("#TEMPLATES\n")
    templates = dict(l.split("\t", 1) for l in rest.splitlines() if "\t" in l)
    return ids.split("\n")[:-1], templates


def norm(t):
    return " ".join(t.split())


def grouping_accuracy(truth, parsed):
    by_parsed = defaultdict(list)
    for i, p in enumerate(parsed):
        by_parsed[p].append(i)
    truth_size = Counter(truth)
    ok = 0
    for idx in by_parsed.values():
        if len({truth[i] for i in idx}) == 1 and truth_size[truth[idx[0]]] == len(idx):
            ok += len(idx)
    return ok / len(truth)


rows, tot = [], defaultdict(lambda: [0.0, 0.0, 0])
for ds, s in SETTINGS.items():
    path = os.path.join(B, "data/prep", f"{ds}.txt")
    truth = [norm(l) for l in open(os.path.join(B, "data/prep", f"{ds}.truth"), encoding="utf-8").read().split("\n")[:-1]]
    sig = {}
    res = {}
    for name, cmd in RUNNERS.items():
        p = subprocess.run(cmd + ["parse", path, str(s["depth"]), str(s["st"])], capture_output=True, text=True, check=True)
        ids, templates = parse_output(p.stdout)
        assert len(ids) == len(truth), (name, ds, len(ids), len(truth))
        parsed = [norm(templates.get(i, f"<missing {i}>")) for i in ids]
        sig[name] = (tuple(parsed), tuple(sorted(Counter(ids).values())))
        ga = grouping_accuracy(truth, parsed)
        pa = sum(a == b for a, b in zip(parsed, truth)) / len(truth)
        res[name] = (ga, pa)
        t = tot[name]
        t[0] += ga; t[1] += pa
    same = len(set(sig.values())) == 1
    for name in RUNNERS:
        tot[name][2] += same
    rows.append((ds, res, same))
    print(f"{ds:12} " + "  ".join(f"{n}: GA={g:.3f} PA={p:.3f}" for n, (g, p) in res.items()) + ("  identical" if same else "  DIFFERENT"), file=sys.stderr)

n = len(SETTINGS)
print("| implementation | grouping accuracy (mean) | parsing accuracy (mean) | identical to logpai/Drain3 |")
print("|---|--:|--:|--:|")
for name in RUNNERS:
    ga, pa, same = tot[name]
    print(f"| {name} | {ga / n:.3f} | {pa / n:.3f} | {same}/{n} |")
