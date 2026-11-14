# Controlled wide-table READ capacity

The fixed input contains 32768 real two-row base files, one 128-column partial
update of a 259-column Data Evolution table, 72089 logical rows and Snapshot 33.
The complete warehouse has 67739 files including metadata and auxiliary data;
that is not the count of valid target data files. Frozen inventory SHA-256:
`c7760f47dafb21c5909471cc97bc2f39a7bc138ba2f71b68dbf36aeff8472246`.

Clean runtime base: `b7cfb397de334d56b8c3eaeb60a2cfa9c9cab627`, artifact
`4bd1e88ab67bc457b8cf6843af1d23893638dd3e1bb00038b852d7707a9866be`.
Candidate: `3d64be92a161a12130ede4f2719fb4df4a1b8844`, production-equivalent to
frozen f17, normal artifact
`b4e627ab3ac34823b8bff8849e737911dfd73c0b28273d01825b4629868e573a`.

Both measured arms use Driver 512 MiB and two 512 MiB Executors, Spark 3.5.9 /
Scala 2.12.18, JDK17/G1, split target 128 MiB/open cost 4 MiB/shuffle partitions
200. The same existing reader projects all 259 business columns plus `_ROW_ID`,
consumes actual values including NULLs, and checks domains and counts. Each
query uses a fresh JVM. This is one 4-core/16-GiB host with multiple processes.

| Mode | LIMIT 20 | Full read |
| --- | --- | --- |
| Clean upstream | Driver Java heap OOM after query start | Driver Java heap OOM after query start |
| Candidate streaming | 20 valid rows, 9.573720134 s | 72089 valid rows, 82.889682472 s |

Times include the common validation consumer and are single capacity-run
observations. Do not calculate a speedup against the failed upstream. Equal
resource successful-case four-mode timing is a separate experiment at 2 GiB
Driver/two 512 MiB Executors.

The LIMIT main exception propagates through `DataEvolutionFileStoreScan` /
`AbstractFileStoreScan.plan` to Spark input planning, with a root allocation
frame in `BinaryString.fromBytes` during Avro Manifest string-array decoding.
The full-read root allocation is `HeapByteBuffer` / Zstd during Manifest
decompression. These are the observed exception-chain allocation frames, not
proof of the first chronological OOM or of the retained owner.

Both arms separately passed five bidirectional exact differences through the
ACTUAL tested reader: full result multiset, unchanged source, old business-key
and RowID pairs, complete RowID domain, and inserted RowID domain. Each exact
oracle uses the independently defined fixture formulas and original seed
key/RowID mapping, with a separately disclosed 2 GiB Driver/two 512 MiB
Executors. It is not evidence that the full oracle ran at 512 MiB. Snapshot 33
and prepared input bytes remain unchanged. No reserve violation, cgroup kill
or lingering process was observed.

The disk-limited host uses one independent physical private restore from
immutable verified local archives. Before each arm it verifies the full frozen
inventory, makes one untimed schema0-to-schema1 options ALTER, and after the arm
compares all files again. Reset removes only the captured, verified extra
schema1 file and rechecks the original full inventory. Data inodes are reused;
system caches are warm and not identical. No MERGE test uses this reset scheme.

The initial streaming query controller stopped only at the UNSTARTED exact
oracle's disk preflight. Its two successful 512-MiB reads were preserved without
rerun. After fresh-hash-verified duplicate upload indexes were cold-stored,
an exact-only continuation passed all five checks at the preregistered larger
oracle heap. Both original rejection and successful continuation are retained.
An earlier 64-file reader gate had the same kind of pre-query resource rejection;
its preparation was reused, and its later two reads and five exact checks passed.

The upstream legacy `observerCoveragePassed=false` field was computed from
query exit success rather than sampler coverage. The original bytes remain
unchanged; a separate parent audit verifies all three actual 512-MiB JVM
identities, positive sampler rows and successful sampler exits for BOTH failed
queries. Sampled RSS includes the observation JVMs and is not isolated SQL heap.

This establishes improvement of this controlled read-planning case. Task arrays,
retained serialized bytes, maximum messages/dependency groups and compatibility
paths remain scale-dependent. It does not establish repair of the separate
production three-million-file `exit137` incident. See README.md for source-only
construction, reader commands and the difference between a newly generated
fixture and the original frozen bytes.
