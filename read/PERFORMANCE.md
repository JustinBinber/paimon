# Controlled wide-table READ performance

Same frozen 32768-base-file / 259-column partial-update input as [capacity](CAPACITY.md).
ALL four modes use Driver 2 GiB and two 512 MiB Executors. Each query starts a fresh JVM.
U=clean upstream, F=candidate off, L=lightweight, S=streaming. All 16 queries pass their
actual-value/NULL/domain/count consumer; each mode also has five exact bidirectional
checks through that actual reader, performed once outside timing and reused.

Exactly two reverse-order blocks: `U F S L` and `L S F U`. Same immutable input,
SQL, artifact binding, G1, split target 128 MiB/open cost 4 MiB/shuffle 200 and
observer settings. One untimed options ALTER per arm; serial SQL with no build,
backup, cleanup or offline parsing during either complete block. Only verified
schema1 reset is done between READ arms. All samples and negative results are kept.

One 4-core/16-GiB Ubuntu host, Spark 3.5.9 / Scala 2.12.18, JDK17 SQL processes.
One private restored physical input is reused, with the same data inodes and warm
OS caches. Reversed positions reduce order bias; they do not equalize cache state
or balance all carryover. This is not a complete Williams design or a multi-node
network benchmark. Two samples per cell support descriptive ranges only.

## All measured read-and-validation times

The timer includes planning, execution and the common validation consumer, not
isolated planning. The midpoint is the descriptive median of two observations.

| Query | Mode | Block 1 s | Block 2 s | Range s | Midpoint s | vs U midpoint |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| limit20 | U | 7.335340741 | 7.705224944 | 7.335341–7.705225 | 7.520283 | +0.00% |
| limit20 | F | 7.818478580 | 7.580382836 | 7.580383–7.818479 | 7.699431 | +2.38% |
| limit20 | L | 7.554195153 | 7.460935578 | 7.460936–7.554195 | 7.507565 | -0.17% |
| limit20 | S | 9.625539232 | 9.759116579 | 9.625539–9.759117 | 9.692328 | +28.88% |
| full | U | 80.374490109 | 81.038504964 | 80.374490–81.038505 | 80.706498 | +0.00% |
| full | F | 80.288787756 | 80.697623461 | 80.288788–80.697623 | 80.493206 | -0.26% |
| full | L | 80.344296626 | 80.508821797 | 80.344297–80.508822 | 80.426559 | -0.35% |
| full | S | 82.950126157 | 82.899079137 | 82.899079–82.950126 | 82.924603 | +2.75% |

In both observations streaming LIMIT is slower; its midpoint increases by 28.88%
(about 2.17 s), while the full-read midpoint increases by 2.75%. Added metadata
sorting/replay may contribute, but this experiment does not isolate its cause.
The capacity result is not a speedup comparison. Default off remains the normal
path. These samples do not establish a general acceptable slowdown threshold or
prove zero default-path overhead; users should choose the opt-in mode according
to capacity and workload.

## Observed resource midpoints

All values below are descriptive medians of the two actual observations.
Heap/RSS are sampled Driver peaks; CPU and GC cover the observed lifetimes of
the three JVMs, including startup/teardown, not exact SQL-only work. Task
deserialization CPU is the sum of ended Spark task metrics. CPU is in seconds;
heap/RSS and writes are MiB. Driver and Executor I/O columns use shared endpoints
only while all three identified JVMs are alive, with child PPIDs checked.

| Query | Mode | Driver heap | Driver RSS | JVM CPU s | JVM GC s | Task deserialize CPU s | Driver writes | Executor writes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| limit20 | U | 841.29 | 1479.01 | 35.800 | 0.444 | 0.329155 | 1.60 | 187.83 |
| limit20 | F | 791.34 | 1433.78 | 34.875 | 0.475 | 0.300218 | 1.50 | 188.32 |
| limit20 | L | 376.32 | 856.46 | 34.325 | 0.293 | 0.318410 | 1.48 | 188.32 |
| limit20 | S | 284.96 | 714.84 | 38.995 | 0.290 | 0.309569 | 121.82 | 188.38 |
| full | U | 1093.73 | 1625.16 | 243.360 | 6.408 | 13.495644 | 13.65 | 191.10 |
| full | F | 1115.36 | 1650.85 | 244.100 | 6.351 | 13.220250 | 13.65 | 191.62 |
| full | L | 539.50 | 1036.25 | 240.990 | 6.637 | 13.082112 | 13.68 | 191.89 |
| full | S | 381.00 | 757.63 | 245.775 | 6.598 | 13.500068 | 133.90 | 191.90 |

The machine-readable [performance-results.json](performance-results.json) retains
every raw sample, all task metrics, each process identity/heap/RSS/CPU/GC and raw
I/O endpoints, exact-proof bindings and input/evidence hashes. LIMIT has one task;
the full consumer has 1025. All selected task starts/ends and application ends
are present. Missing process I/O endpoints remain missing, never converted to zero.

Linux process I/O can include waited-for children: raw Driver and child lifetime
counters must not be summed. The coexistence interval is not exact SQL-only or
connector-only accounting; short-lived children may still contribute. Localization,
Spark/event/GC logs and runtime work remain included. `rchar/wchar` are not
disk/network-only; zero storage reads can reflect page cache. Spark shuffle/spill
metrics exclude connector-local metadata sorting/replay. Spark Result Size is not
raw commit-message bytes. Task GC overlaps JVM GC and must not be added to it.

The streaming 512-MiB capacity success is a separate experiment. No ratio is
calculated against its failed upstream. This controlled result is not proof of
repair of the unclassified production three-million-file exit137 incident.

All 16 selected query event logs report zero Spark shuffle read/write and zero Spark memory/disk spill, with no missing values for these counters. This does not imply zero connector-local spill: streaming common-live Driver storage writes are about 120 MiB higher than upstream in both query types. The stored `canonicalUnchanged=false` is retained from the single-restored-input protocol; that protocol does not assert an untouched separate physical clone. `preparedUnchanged=true` records the full inventory comparison after each arm's options ALTER. Immutable archive input and verified between-arm schema resets supply the original-input binding.
