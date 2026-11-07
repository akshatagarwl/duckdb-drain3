# Benchmarks

These scripts use the inputs and the logpai/Drain3 runner from the drain3 crate's harness,
[`bench/compare`](https://github.com/akshatagarwl/drain3/tree/main/bench/compare). Set it up there
first:

```sh
git clone https://github.com/akshatagarwl/drain3 && cd drain3/bench/compare
./fetch.sh && python3 prepare.py && python3 run.py build
```

Then, from this repository:

```sh
make configure release
export DRAIN3_BENCH=/path/to/drain3/bench/compare
configure/venv/bin/python bench/accuracy.py      # Loghub-2k: GA/PA, identical to logpai/Drain3?
configure/venv/bin/python bench/throughput.py 3  # BGL: drain_train, drain_match, drain_parse
```

- `runner.py` is a `parse` runner for the harness protocol, built on `drain_parse`.
- `accuracy.py` runs logpai/Drain3 0.9.11 and the extension on the 16 Loghub-2k datasets, using
  logparser's per-dataset settings.
- `throughput.py` loads raw BGL (4,747,963 lines) into a table, then times the queries. Loading is
  not timed. It also checks that `drain_parse` puts every line in the same cluster as the drain3
  crate's runner.
