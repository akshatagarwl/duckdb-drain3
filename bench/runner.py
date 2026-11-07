"""drain3 DuckDB extension runner, same protocol as the drain3 crate's
bench/compare harness (https://github.com/akshatagarwl/drain3/tree/main/bench/compare):

  runner.py parse FILE DEPTH ST   -> cluster id per line, '#TEMPLATES', 'id<TAB>template'

Lines are loaded into a DuckDB table `logs(ln, message)` and parsed with
`drain_parse('logs', 'message', order_by := 'ln', ...)`.
Env: DRAIN3_EXT = path to drain3.duckdb_extension (default: build/release).
"""
import os, sys
import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
EXT = os.environ.get("DRAIN3_EXT", os.path.join(HERE, "..", "build", "release", "extension", "drain3", "drain3.duckdb_extension"))


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXT}'")
    return con


def load_lines(con, path):
    lines = open(path, encoding="utf-8", errors="replace").read().split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    con.execute("CREATE TABLE logs AS SELECT generate_subscripts($l, 1) AS ln, unnest($l) AS message", {"l": lines})
    return len(lines)


def main():
    mode, path, depth, st = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
    assert mode == "parse", mode
    con = connect()
    load_lines(con, path)
    rows = con.execute(
        "SELECT cluster_id FROM drain_parse('logs', 'message', order_by := 'ln', depth := $d, sim_threshold := $s)",
        {"d": depth, "s": st},
    ).fetchall()
    out = sys.stdout
    out.write("\n".join(str(r[0]) for r in rows) + "\n#TEMPLATES\n")
    for cid, tpl in con.execute(
        "SELECT c.cluster_id, c.template FROM (SELECT unnest(drain_clusters(drain_train(message, depth := $d, sim_threshold := $s ORDER BY ln))) AS c FROM logs) ORDER BY 1",
        {"d": depth, "s": st},
    ).fetchall():
        out.write(f"{cid}\t{tpl}\n")


if __name__ == "__main__":
    main()
