# Prometheus

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, Prometheus, allows you to do this...

```
D select labels['mode'][1], sum(value) from prometheus('rate(node_cpu_seconds_total[5m])') group by labels['mode'][1];
┌───────────────────┬────────────────────┐
│ labels['mode'][1] │    sum("value")    │
│      varchar      │       double       │
├───────────────────┼────────────────────┤
│ nice              │                0.0 │
│ user              │   2.46502935141325 │
│ idle              │  7.788290292024612 │
│ system            │ 0.6112264217808843 │
└───────────────────┴────────────────────┘
```
