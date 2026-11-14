# Recorded results

Comparison base: `b7cfb397de334d56b8c3eaeb60a2cfa9c9cab627`. Candidate: `42540b17fd0cf7b8e5ff05efb598b88abfd76ecd` (same source tree as frozen `f17caf4cc27eaa87927fe0fb28385462aecb373d`).

One Ubuntu 22.04 machine, 4 CPUs / 16 GiB RAM, Spark 3.5.9 / Scala 2.12.18, JDK 17 SQL processes. This is a single-host, multiple-process experiment, not a multiple-node cluster. Paimon Spark3 artifacts were built normally with JDK 8. Shared warm dependencies and separate build outputs were used; current reactor/module bindings were checked.

## Capacity and correctness

The seed has 524288 real two-row base files, 1048576 initial rows, three columns, no primary key, Data Evolution and row tracking enabled, bucket=-1. Frozen inventory SHA-256: `fd0906747c02ddf8afd9046e0c7fa82c8aec64f6881a84a1f0e24e9d94a9a56c`. Every arm uses a fresh physical private copy and the same MERGE.

| Arm | Measured heaps | Outcome |
| --- | --- | --- |
| Clean fixed upstream | Driver 1 GiB, two Executors 512 MiB each | Driver heap OOM during scan planning; no new Snapshot; source/seed unchanged |
| Candidate streaming | Same | Full MERGE completed: SQL 740.262465882 s; only Snapshot 513 added |

Do not calculate a speedup against the failed upstream run. The candidate commit log duration is 19.605 s. A separate oracle using unmodified upstream with Driver 4 GiB and two 1 GiB Executors passed five exact bidirectional checks: full result multiset, unchanged source, old business-key/RowID pairs, complete RowID domain and inserted RowID domain. Expected result: 1153433 rows. It also verified immutable source/seed and all previous Snapshot bytes. These larger oracle heaps are excluded from the measured small-heap capacity claim.

The MERGE process tree observed peak RSS was 3682594816 bytes. The independent oracle observed peak RSS was 7110864896 bytes. These are sampled whole-process-tree RSS, not isolated SQL heap peaks. All three actual JVM heap/GC identities were observed in both phases. Oracle class-origin markers are available for the Driver only; Executor class markers were not emitted by the existing helper. Neither phase violated the reserved disk/memory bounds.

## Successful-case SQL performance: 32 files

Four balanced Williams blocks; U=upstream, F=candidate off, L=lightweight, S=streaming. Every measured MERGE and its separate five-check oracle passed; prerequisite cases are excluded. All samples are retained.

```text
U1 F1 S1 L1
F2 L2 U2 S2
L3 S3 F3 U3
S4 U4 L4 F4
```

| Mode | SQL seconds by block (1–4) | Median seconds | Ratio of medians vs upstream |
| --- | --- | ---: | ---: |
| upstream | 7.642550046, 8.069579576, 7.987305058, 7.914079115 | 7.950692087 | +0.00% |
| off | 7.722187372, 8.025444726, 7.594872604, 7.753153505 | 7.737670439 | -2.68% |
| lightweight | 7.873958831, 7.450485943, 7.908746831, 7.727028239 | 7.800493535 | -1.89% |
| streaming | 8.515053314, 8.171018948, 7.858951911, 7.995252834 | 8.083135891 | +1.67% |

Four samples do not establish a p95, statistical significance, or general speedup. Streaming has +1.67% by ratio of medians; its first paired block is +11.42%, which is retained. Measured MERGE heaps are Driver 1 GiB/two 512 MiB Executors. Each separate correctness oracle uses Driver 2 GiB/two 512 MiB Executors.

## 65536-file performance

The four-mode prerequisite completed with all 20 exact checks passing (144179 result rows, only Snapshot 65 added). The balanced timing blocks are still running; no final timing claim is made yet.

## Scope and limitations

This validates the reproduced planning-to-commit Data Evolution MERGE chain. It does not establish repair of the separate production three-million-file SELECT/LIMIT incident or whole-chain constant memory. Task arrays, retained result bytes, largest messages/groups, compact RowID/identity summaries and compatibility/diagnostic fallback paths still have scale-dependent bounds. Default-off includes shared refactors and is measured rather than claimed free of overhead. Deletion-vector/callback compatibility paths retain original memory limits. Additional local sorting/replay and decoding have CPU and I/O costs.

See README.md for the reusable original helper, fixture origin, commands, SQL and balanced protocol. The assembled source-only kit itself has not been rerun as a fresh-host package.
