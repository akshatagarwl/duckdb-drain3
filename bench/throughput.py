"""BGL throughput of the drain3 DuckDB extension (4,747,963 raw lines, Drain3
defaults: depth 4, sim_threshold 0.4, no masking). Lines are loaded into a
DuckDB table first and that is not timed. Best of N runs.

  DRAIN3_BENCH=~/drain3/bench/compare python3 bench/throughput.py [runs]

Also checks parity: drain_parse's cluster per line equals the drain3 crate's
(bin/drain3 parse) for every line, when that runner exists.
"""
import json, os, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from runner import connect, load_lines  # noqa: E402

B = os.path.expanduser(os.environ.get("DRAIN3_BENCH", "~/workspace/drain3/bench/compare"))
BGL = os.path.join(B, "data", "BGL.log")
RUNS = int(sys.argv[1]) if len(sys.argv) > 1 else 3

con = connect()
n = load_lines(con, BGL)
threads = con.execute("SELECT current_setting('threads')").fetchone()[0]


def best(sql, setup=None):
    times = []
    for _ in range(RUNS):
        if setup:
            con.execute(setup)
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    return min(times)


res = {"lines": n, "threads": threads}
res["drain_train_s"] = best("SELECT octet_length(drain_train(message ORDER BY ln)) FROM logs")
con.execute("CREATE TABLE m AS SELECT drain_train(message ORDER BY ln) AS model FROM logs")
res["clusters"] = con.execute("SELECT len(drain_clusters(model)) FROM m").fetchone()[0]
q_match = "SELECT count(drain_match(model, message).cluster_id) FROM logs, m"
res["drain_match_s"] = best(q_match)
res["matched"] = con.execute(q_match).fetchone()[0]
con.execute("SET threads = 1")
res["drain_match_1thread_s"] = best(q_match)
con.execute(f"SET threads = {threads}")
q_parse = "CREATE OR REPLACE TABLE p AS SELECT ln, cluster_id, template, params FROM drain_parse('logs', 'message', order_by := 'ln')"
res["drain_parse_s"] = best(q_parse)

crate = os.path.join(B, "bin", "drain3")
if os.path.exists(crate):
    out = subprocess.run([crate, "parse", BGL, "4", "0.4"], capture_output=True, text=True, check=True).stdout
    ids = out.partition("#TEMPLATES\n")[0].split("\n")[:-1]
    ours = [str(r[0]) for r in con.execute("SELECT cluster_id FROM p ORDER BY ln").fetchall()]
    res["parity_lines_equal"] = sum(a == b for a, b in zip(ids, ours))
    res["parity_lines_total"] = len(ids)

for k in ("drain_train", "drain_match", "drain_match_1thread", "drain_parse"):
    res[k + "_lines_per_s"] = round(n / res[k + "_s"])
print(json.dumps(res, indent=1))
