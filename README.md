# drain3 for DuckDB

Log template mining in SQL. The `drain3` extension runs the
[Drain](https://jiemingzhu.github.io/pub/pjhe_icws2017.pdf) algorithm inside DuckDB: it groups log
lines into templates like `user <*> logged in from <*>` and pulls out the variable parts.

It is built on the [`drain3`](https://crates.io/crates/drain3) Rust crate, which gives the same
clusters and templates as [logpai/Drain3](https://github.com/logpai/Drain3), the Python reference.
This was checked line by line on the 16 Loghub-2k datasets (see [Benchmarks](#benchmarks)).

```sql
INSTALL drain3 FROM community;
LOAD drain3;

FROM drain_parse('logs', 'message', order_by := 'ts');
```

```
┌─────────────────────┬─────────────────────────────────────┬────────────┬───────────────────────────────┬────────────┐
│         ts          │               message               │ cluster_id │           template            │   params   │
├─────────────────────┼─────────────────────────────────────┼────────────┼───────────────────────────────┼────────────┤
│ 2026-11-01 10:02:00 │ connection closed by peer 10.1.1.1  │          2 │ connection closed by peer <*> │ [10.1.1.1] │
│ ...                 │                                     │            │                               │            │
```

## Functions

| Function | Kind | Returns |
|---|---|---|
| `drain_parse(table, message_column, order_by := NULL, ...)` | table macro | every row, plus `cluster_id`, `template`, `params` |
| `drain_train(message [ORDER BY ...])` | aggregate | the trained model, a `BLOB` |
| `drain_train_ids(message [ORDER BY ...])` | aggregate | `{model, cluster_ids, templates, sizes}`: the model plus the cluster of every input row |
| `drain_match(model, message, search := 'never')` | scalar | `{cluster_id, template, params}`, or `NULL` when nothing matches. Never changes the model |
| `drain_clusters(model)` | scalar | `LIST` of `{cluster_id, size, template}` |
| `drain_params(template, message, extra_delimiters := [])` | scalar | `VARCHAR[]`, the values of the `<*>` and `<MASK>` slots, or `NULL` when the message does not fit |
| `drain_render(template, params)` | scalar | the template with its slots filled in; the inverse of `drain_params` |

`drain_parse`, `drain_train` and `drain_train_ids` take the same named parameters. Every default is
logpai/Drain3's:

| Parameter | Default | Meaning |
|---|---|---|
| `sim_threshold` | `0.4` | share of a template's constant tokens a line must match to join it |
| `depth` | `4` | prefix tree depth (≥ 3); `depth - 2` leading tokens route a line |
| `max_children` | `100` | children per tree node |
| `max_clusters` | `0` | keep at most this many clusters, evicting the least recently used. `0` means no limit |
| `extra_delimiters` | `[]` | extra token separators besides whitespace, such as `['=', ',']` |
| `masks` | `[]` | regex rules applied before tokenizing, in order: `[{'pattern': '\d+\.\d+\.\d+\.\d+', 'mask': '<IP>'}]` |
| `param_str` | `'<*>'` | wildcard token in templates |
| `parametrize_numeric_tokens` | `true` | route tokens that contain a digit to the wildcard branch |

`drain_parse` takes the name of a table or view. To parse a query result, create a view first.
`order_by` is a column name. Without it, rows are trained in scan order.

## Examples

These run as tests in [`test/sql/readme.test`](test/sql/readme.test).

**1. Top templates**

```sql
SELECT c.template, c.size
FROM (SELECT unnest(drain_clusters(drain_train(message ORDER BY ts))) AS c FROM logs)
ORDER BY c.size DESC
LIMIT 10;
```

**2. What is new since the deploy.** Train on the lines from before the deploy. A line after it that
matches no template is a new kind of line.

```sql
WITH before AS (
  SELECT drain_train(message ORDER BY ts) AS model
  FROM logs WHERE ts < TIMESTAMP '2026-11-01 12:00'
)
SELECT message, count(*) AS n
FROM logs, before
WHERE ts >= TIMESTAMP '2026-11-01 12:00'
  AND drain_match(before.model, message) IS NULL
GROUP BY ALL ORDER BY n DESC;
```

**3. Filter on parameters**

```sql
SELECT ts, params[1] AS user
FROM drain_parse('logs', 'message', order_by := 'ts')
WHERE template = 'user <*> logged in from <*> after <*> attempts'
  AND params[3]::INT >= 3;
```

**4. Parquet archive by template.** Store a cluster id and the parameters for each line, and keep
the templates once. Then you can filter by template or parameter without regular expressions.
`drain_render` rebuilds the lines. On BGL this layout is about the same size as the raw lines (see
[Benchmarks](#benchmarks)).

```sql
COPY (SELECT ts, service, cluster_id, params FROM drain_parse('logs', 'message', order_by := 'ts'))
  TO 'lines.parquet';
COPY (SELECT unnest(drain_clusters(drain_train(message ORDER BY ts)), recursive := true) FROM logs)
  TO 'templates.parquet';

SELECT ts, drain_render(t.template, l.params) AS message
FROM 'lines.parquet' l JOIN 'templates.parquet' t USING (cluster_id);
```

Rebuilt lines have single spaces between tokens, the same as templates.

**5. Train once, reuse the model.** A model is a plain `BLOB`, so you can store it in a table or a
Parquet file. Matching runs on all of DuckDB's threads.

```sql
CREATE TABLE models AS
SELECT service, drain_train(message ORDER BY ts) AS model FROM logs GROUP BY service;

SELECT drain_match(m.model, l.message).template AS template, count(*) AS n
FROM new_logs l JOIN models m USING (service)
GROUP BY ALL ORDER BY n DESC;
```

## Training order matters

Drain is order-dependent: the same lines in a different order can produce different clusters. To
get deterministic results that match logpai/Drain3:

- Use `drain_parse(..., order_by := 'ts')`, or write `ORDER BY` inside the aggregate:
  `drain_train(message ORDER BY ts)`. DuckDB then feeds the rows of each group to one state in that
  order, on one thread. That state is the same model logpai/Drain3 builds from that sequence.
- Without `ORDER BY`, DuckDB splits the input across threads. Each thread trains a partial model,
  and the partial models are merged by replaying each cluster's template once, weighted by its size.
  Cluster sizes still add up, but the templates can differ from a sequential run and can change from
  one run to the next. Use this mode only when speed matters more than exactness.

Matching (`drain_match`, `drain_params`, `drain_render`) does not depend on order. It runs in
parallel.

## Limits

- **Training runs on one thread per group.** Use `GROUP BY` (for example by service) to train
  several models in parallel.
- **An ordered aggregate holds its input.** DuckDB collects and sorts the rows of a group before
  training. `drain_parse` also keeps one 8-byte cluster id per row. On large inputs, set
  `memory_limit` and `temp_directory` so DuckDB can spill to disk.
- **`drain_params` and `drain_render` only know two slot forms:** `<*>` and `<NAME>`, where NAME uses
  upper-case letters, digits and `_` (the logpai mask convention). A custom `param_str`, or a mask
  that does not look like `<NAME>`, is treated as literal text.
- **Message columns are matched by exact name.** `drain_parse` accepts a table or view name, not a
  subquery.
- **Models are versioned BLOBs** (`DRAIN3M` + format byte). A BLOB from another tool, a truncated
  BLOB or a corrupt BLOB raises an error.
- **Built against DuckDB's unstable C API v2.** Each release is built for one DuckDB version
  (currently v2.0.0), and DuckDB installs the build that matches it.

## Benchmarks

**Accuracy on Loghub-2k**, 16 datasets, logparser's per-dataset settings. Mean grouping accuracy
(GA) and parsing accuracy (PA). "Identical" means the same partition of lines and the same final
templates as logpai/Drain3.

| implementation | GA | PA | identical to logpai/Drain3 |
|---|--:|--:|--:|
| logpai/Drain3 0.9.11 (Python) | 0.874 | 0.399 | 16/16 |
| drain3 DuckDB extension, `drain_parse` | 0.874 | 0.399 | 16/16 |

Command: `DRAIN3_BENCH=<drain3>/bench/compare configure/venv/bin/python bench/accuracy.py`. See
[`bench/`](bench/).

**Throughput on BGL**: 4,747,963 raw lines, Drain3 defaults (depth 4, `sim_threshold` 0.4, no
masking). Lines were loaded into memory before timing. DuckDB numbers are the best of 3 runs;
logpai/Drain3 got 1 run.

| what | drain3 extension | logpai/Drain3 0.9.11 (Python) |
|---|--:|--:|
| train, in order (`drain_train(message ORDER BY ln)` / `add_log_message` loop) | 2.97 s, 1.60 M lines/s | 143.5 s, 33 K lines/s |
| match every line, 10 threads (`drain_match`) | 0.81 s, 5.86 M lines/s | n/a (single-threaded) |
| match every line, 1 thread | 3.86 s, 1.23 M lines/s | 323.5 s, 14.7 K lines/s |
| `drain_parse`: every row with cluster, template and params | 10.9 s, 437 K lines/s | |

Both found 842 clusters and matched every line. `drain_parse` put every one of the 4,747,963 lines
in the same cluster as the drain3 crate.

Commands: `configure/venv/bin/python bench/throughput.py 3` and
`.venv/bin/python impls/python/run.py bench data/BGL.log 4 0.4`, run from drain3's `bench/compare`.
Machine: Apple M4 (10 cores), 24 GiB, macOS 27.0.1, DuckDB v2.0.0, Python 3.12.12. Other work was
running on the machine (1-minute load average 1.5 to 4), so treat these as rough numbers.

**Archive size**, from [`bench/archive.sql`](bench/archive.sql). The data is BGL, written as ZSTD
Parquet.

| layout | size |
|---|--:|
| raw lines, one `VARCHAR` column | 70.9 MB |
| `cluster_id` + `params` per line, plus an 842-row template table | 68.5 MB |
| for reference: `gzip -6` of the text file | 62.9 MB |

On BGL the template layout saves only about 3%, because most of each line is in its parameters
(timestamps, node names). The benefit is that you can filter by template or parameter without
regular expressions. `drain_render` rebuilt all 4,747,963 lines, identical up to whitespace.

## Building

```sh
git clone --recurse-submodules https://github.com/akshatagarwl/duckdb-drain3
cd duckdb-drain3
make configure     # Python venv with DuckDB and its SQLLogicTest runner
make release       # build/release/extension/drain3/drain3.duckdb_extension
make test_release  # test/sql/*.test
duckdb -unsigned -cmd "LOAD 'build/release/extension/drain3/drain3.duckdb_extension'"
```

The extension uses [duckdb/extension-template-rs](https://github.com/duckdb/extension-template-rs)
(C API v2, [`duckdb-neo`](https://crates.io/crates/duckdb-neo)). There is no C++.

## License

Apache-2.0. Drain is described in He et al., *Drain: An Online Log Parsing Approach with Fixed Depth
Tree*, ICWS 2017. The defaults and behaviour follow [logpai/Drain3](https://github.com/logpai/Drain3)
(MIT).
