# Recorded results

Comparison base: `b7cfb397de334d56b8c3eaeb60a2cfa9c9cab627`. Candidate: `3d64be92a161a12130ede4f2719fb4df4a1b8844`. Its production, tests and build inputs are byte-identical to frozen `f17caf4cc27eaa87927fe0fb28385462aecb373d`; only generated configuration HTML differs. The previously validated normal runtime artifacts are reused, not claimed rebuilt for the HTML-only commit.

One Ubuntu 22.04 machine, 4 CPUs / 16 GiB RAM, Spark 3.5.9 / Scala 2.12.18, JDK 17 SQL processes. This is a single-host, multiple-process experiment, not a multiple-node cluster. Normal Spark3 connector builds used JDK 8 and a shared warm Maven cache with separate current output trees/class-origin checks.

## Capacity and correctness

The seed has 524288 real two-row base files, 1048576 initial rows, three columns, no primary key, Data Evolution and row tracking enabled, bucket=-1. Frozen inventory SHA-256: `fd0906747c02ddf8afd9046e0c7fa82c8aec64f6881a84a1f0e24e9d94a9a56c`. Every arm uses a fresh physical private copy and the same MERGE.

| Arm | Measured heaps | Outcome |
| --- | --- | --- |
| Clean fixed upstream | Driver 1 GiB, two Executors 512 MiB each | Driver heap OOM during scan planning; no new Snapshot; source/seed unchanged |
| Candidate streaming | Same | Full MERGE completed: SQL 740.262465882 s; only Snapshot 513 added |

Do not calculate a speedup against the failed upstream run. The candidate commit log duration is 19.605 s. A separate oracle using unmodified upstream with Driver 4 GiB and two 1 GiB Executors passed five exact bidirectional checks: full result multiset, unchanged source, old business-key/RowID pairs, complete RowID domain and inserted RowID domain. Expected result: 1153433 rows. It also verified immutable source/seed and all previous Snapshot bytes. These larger oracle heaps are excluded from the measured small-heap capacity claim.

The MERGE process tree observed peak RSS was 3682594816 bytes. The independent oracle observed peak RSS was 7110864896 bytes. These are sampled whole-process-tree RSS, not isolated SQL heap peaks. All three actual JVM heap/GC identities were observed in both phases. Oracle class-origin markers are available for the Driver only; Executor class markers were not emitted by the existing helper. Neither phase violated the reserved disk/memory bounds.

## Successful-case protocol

At each scale, all 16 measured MERGEs and separate five-check oracles passed. Each arm uses a fresh JVM and physical private copy of its frozen seed, the same SQL, split target 128 MiB/open cost 4 MiB/shuffle partitions 200, and Driver 1 GiB/two 512 MiB Executors. Correctness oracles use a separate Driver 2 GiB/two 512 MiB Executors and are excluded from SQL timers. Prerequisites are excluded. No competing build, archive or offline analysis was run during timed blocks. The original balanced order is:

```text
U1 F1 S1 L1
F2 L2 U2 S2
L3 S3 F3 U3
S4 U4 L4 F4
```

U=upstream, F=candidate off, L=lightweight, S=streaming. All four samples are retained. Cache state is not identical or isolated: order balancing reduces but does not eliminate carryover and system drift. This is not a cold-cache benchmark.

## 32-file performance

| Mode | SQL seconds, blocks 1–4 | Median seconds | vs upstream median |
| --- | --- | ---: | ---: |
| upstream | 7.642550046, 8.069579576, 7.987305058, 7.914079115 | 7.950692 | +0.00% |
| off | 7.722187372, 8.025444726, 7.594872604, 7.753153505 | 7.737670 | -2.68% |
| lightweight | 7.873958831, 7.450485943, 7.908746831, 7.727028239 | 7.800494 | -1.89% |
| streaming | 8.515053314, 8.171018948, 7.858951911, 7.995252834 | 8.083136 | +1.67% |

Resource medians from all four saved arms per mode (MiB = 1048576 bytes):

| Mode | Observed JVM CPU s | Task deserialize CPU s | Observed summed JVM GC ms | Commit log ms | Driver sampled heap/RSS MiB | Largest individual Executor heap/RSS MiB | Driver common-live accounted writes MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| upstream | 51.325 | 1.089 | 312.5 | 65.5 | 113.3 / 577.7 | 121.8 / 407.7 | 2.07 |
| off | 48.805 | 1.103 | 302.0 | 59.5 | 120.5 / 555.7 | 127.0 / 409.7 | 2.06 |
| lightweight | 48.385 | 1.112 | 303.0 | 94.5 | 112.5 / 528.9 | 116.7 / 385.4 | 2.05 |
| streaming | 51.410 | 1.128 | 310.5 | 139.0 | 141.3 / 579.8 | 109.5 / 424.3 | 2.12 |

See [resource-results-32.json](resource-results-32.json) for every arm, CPU/GC, shuffle, Spark spill, all three JVMs, I/O windows and source hashes. All modes have median shuffle write 4411 bytes and Spark disk spill 0 bytes. Spark spill does not include connector-local sort/replay I/O.

## 65536-file performance

65536 real two-row base files, 131072 initial rows, 144179 result rows, three columns. Only Snapshot 65 is added; prior Snapshots/source/seed remain unchanged. Frozen seed inventory SHA-256: `96a2fe42d7c14fe59af90c4e65961dc75fdfef43802fbaef7acd3e839952ffeb`.

| Mode | SQL seconds, blocks 1–4 | Median seconds | vs upstream median |
| --- | --- | ---: | ---: |
| upstream | 150.145811571, 148.521482424, 150.491303165, 151.238449463 | 150.318557 | +0.00% |
| off | 152.590030343, 150.447679794, 149.908042841, 149.736485454 | 150.177861 | -0.09% |
| lightweight | 150.220679262, 152.960918286, 149.324403449, 150.101826107 | 150.161253 | -0.10% |
| streaming | 106.747148725, 108.695860394, 107.761899679, 107.748324756 | 107.755112 | -28.32% |

Resource medians from all four saved arms per mode (MiB = 1048576 bytes):

| Mode | Observed JVM CPU s | Task deserialize CPU s | Observed summed JVM GC ms | Commit log ms | Driver sampled heap/RSS MiB | Largest individual Executor heap/RSS MiB | Driver common-live accounted writes MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| upstream | 502.695 | 105.752 | 23886.0 | 1206.5 | 505.5 / 1190.3 | 501.0 / 988.6 | 70.54 |
| off | 500.580 | 106.278 | 24287.0 | 1130.5 | 580.1 / 1309.0 | 504.5 / 998.2 | 70.56 |
| lightweight | 499.675 | 104.897 | 24304.5 | 2082.5 | 512.5 / 1247.8 | 501.6 / 994.4 | 71.04 |
| streaming | 339.790 | 59.734 | 7932.0 | 3254.0 | 511.4 / 1153.0 | 502.2 / 974.1 | 157.46 |

See [resource-results-65536.json](resource-results-65536.json) for every arm, CPU/GC, shuffle, Spark spill, all three JVMs, I/O windows and source hashes. All modes have median shuffle write 8846086 bytes and Spark disk spill 0 bytes. Spark spill does not include connector-local sort/replay I/O.

## Interpretation and limits

Four samples per mode establish neither p95 nor statistical significance. At 32 files streaming is +1.67% by ratio of medians; its first paired block is +11.42%, and its sampled Driver heap is higher. Default-off is measured because it includes shared refactors. Streaming local replay/sort has additional Driver writes and commit cost; lower total SQL time at a particular scale does not remove those costs. No speedup is calculated against an OOM.

SQL seconds use the monotonic helper API-call timer; commit duration is the existing commit log interval, not the whole planning phase. Exact isolated planning/encoding CPU is unavailable. JVM CPU/GC/heap/RSS cover observed JVM lifetime including setup/probe/shutdown; sampled peaks can miss real peaks. Executor values are largest individual peaks, not sums of independent peaks. Task GC overlaps with JVM GC and must not be added to it.

Linux `/proc/PID/io` can include waited-for child counters. Do not sum raw Driver/Executor lifetime I/O or call it exclusive Driver I/O. The reported common-live windows require all three identified JVMs to be alive at both endpoints; these are sampled coexistence intervals, not exact SQL or connector-only intervals. Short-lived other children and endpoint races are not excluded. `rchar`/`wchar` include non-disk syscalls; `write_bytes` is accounted block writing/dirtying, not confirmed durable backend I/O. Cancelled writes, window lengths and raw counters remain in JSON. Zero storage reads can reflect page cache; zero Spark input bytes can reflect custom reads.

This validates the reproduced planning-to-commit Data Evolution MERGE chain. It does not establish repair of the separate production three-million-file SELECT/LIMIT incident or whole-chain constant memory. Task arrays, retained result bytes, largest messages/dependency groups, compact RowID/identity summaries and compatibility/diagnostic fallback paths remain scale-dependent. Deletion-vector/callback paths retain original memory bounds.

See README.md for the reused original helper/controller, fixture origin, commands and exact SQL. The assembled source-only kit has not been rerun as a fresh-host package.
