# Wide Data Evolution READ reproduction

These are the original existing fixture, reader and controller sources, reused
without changing their SQL/consumer. This source-only assembled package has not
been executed as a fresh-host package. Use Linux, Python 3.10+, Spark 3.5.9 /
Scala 2.12.18, JDK 17 for SQL and JDK 8 for the normal connector builds.

The measured input has 32768 real two-row base files, 259 business columns,
one update of 128 columns, Snapshot 33 and 72089 logical result rows. It has no
primary key, bucket=-1, Data Evolution and row tracking enabled. The existing
fixture SDK was unmodified `d98f08784c2cf9aed42cc759b3f9c1b3c9bec628` (historical
artifact SHA-256 `21e32f455b3b63d01523b4cb3580bdfc1ef16620c0d40c2bb48878dfffc1763b`).
Comparison runtime is fixed clean `b7cfb397de334d56b8c3eaeb60a2cfa9c9cab627`
versus candidate `3d64be92a161a12130ede4f2719fb4df4a1b8844`; production inputs
are unchanged from frozen f17. Do not substitute the comparison SDK for the
historical fixture SDK while claiming identical fixture provenance.

The initial values are `value=id*7`, nullable `payload`, and 256 nullable BIGINT
columns. The single partial MERGE updates `value` and x0..x126 for matching old
rows and inserts new rows. It retains the other 131 business columns. The
original fixture helper also captures the pre-update key/RowID pairs and checks
five bidirectional exact differences after the update. All formulas and SQL
are in [fixture/CommitMemoryBench.java](fixture/CommitMemoryBench.java).

## Generate and freeze a new scratch input

Use the root README's normal Maven build instructions for the fixture, baseline
and candidate SDKs. Set `KIT`, `SPARK_HOME`, `FIXTURE_JAR`, `BASE_JAR`, `CAND_JAR`
and the corresponding actual build-provenance JSON files. Select a NEW scratch
directory and check disk/memory before running; never use a formal table.

```bash
set -euo pipefail
export OUT=/data/new-wide-read-run
mkdir "$OUT"
PAIMON_JAR="$FIXTURE_JAR" HARNESS_BUILD="$OUT/fixture-helper" \
  bash "$KIT/read/fixture/build-helper.sh"
FIXTURE_HELPER="$OUT/fixture-helper/commit-memory-harness.jar"
for PHASE in fixture merge validate; do
  "$SPARK_HOME/bin/spark-submit" --master 'local-cluster[2,1,1024]' \
    --driver-memory 4g --executor-memory 1g \
    --conf spark.sql.shuffle.partitions=200 --conf spark.speculation=false \
    --conf spark.task.maxFailures=1 --conf spark.dynamicAllocation.enabled=false \
    --class bench.CommitMemoryBench --jars "$FIXTURE_JAR" \
    "$FIXTURE_HELPER" "$PHASE" "$OUT/seed" "$OUT/construction-$PHASE" 32768 8 256 0
done
```

Construction and its validation use larger resources than the measured reads.
The loop performs exactly one partial MERGE. Confirm Snapshot 33, 72089 result
rows and all five exact checks, then freeze this input. New UUIDs and hashes are
expected; a new fixture is not byte-identical to our historical frozen c776
inventory. Use the SAME new immutable input for all comparison arms.

Compile the existing reader against clean baseline, outside the source tree:

```bash
mkdir -p "$OUT/reader/classes"
"$JAVA8_HOME/bin/javac" -J-Xmx256m -source 8 -target 8 -encoding UTF-8 \
  -cp "$BASE_JAR:$SPARK_HOME/jars/*" -d "$OUT/reader/classes" \
  "$KIT/read/ReadMemoryVersionedAlignedBench.java"
"$JAVA8_HOME/bin/jar" cf "$OUT/reader/reader.jar" -C "$OUT/reader/classes" .
```

Record actual SHA-256 values for both SDKs, reader and fixture helper. The
controller's artifact proof requires actual `artifactSha256` and successful
`buildExitCode`, and cannot by itself establish that a build was performed.

## Run a read arm

[controllers/run-read-observed.py](controllers/run-read-observed.py) accepts one
JSON configuration. Paths are absolute caller-selected scratch paths. Required
fields are:

```json
{
  "workspace": "/data/new-wide-read-run",
  "output": "/data/new-wide-read-run/upstream-capacity",
  "canonical": "/data/new-wide-read-run/seed",
  "baseFiles": 32768,
  "variant": "upstream",
  "driverMemory": "512m",
  "exactDriverMemory": "2g",
  "exactExecutorMemory": "512m",
  "canonicalTreeSha256": "ACTUAL_FROZEN_INVENTORY_SHA256",
  "artifact": "/data/artifacts/upstream.jar",
  "artifactSha256": "ACTUAL_UPSTREAM_JAR_SHA256",
  "provenance": "/data/artifacts/upstream-build-proof.json",
  "readerHelper": "/data/new-wide-read-run/reader/reader.jar",
  "readerHelperSha256": "ACTUAL_READER_SHA256",
  "oracleHelper": "/data/new-wide-read-run/fixture-helper/commit-memory-harness.jar",
  "oracleHelperSha256": "ACTUAL_FIXTURE_HELPER_SHA256",
  "sparkHome": "/opt/spark-3.5.9-bin-hadoop3",
  "java17Home": "/opt/jdk17",
  "syntheticScratchInput": true
}
```

Replace the placeholders with hashes from your files; they are not valid launch
values. The existing `harness/bench.py` inventory function emits the sorted
relative path/byte/SHA records whose file SHA is `canonicalTreeSha256`.
Run `python3 -B "$KIT/read/controllers/run-read-observed.py" CONFIG.json`.
For candidate arms use the candidate artifact/proof and variant `off`,
`lightweight` or `streaming`, each with a new output path.

The generic controller makes a physical private copy. Our disk-limited actual
experiment instead restored one independent physical input from verified
immutable local archives. Between READ arms it saved the captured schema,
verified that the ONLY extra file was schema-1, removed that verified file under
the runner locks, and checked the full frozen inventory again. Data files kept
the same inodes; fresh JVMs do not remove system page cache. This protocol is
explicitly a warm-cache single-host comparison. MERGE tests used independent
physical private copies.

Every arm performs one untimed common min-retention ALTER (plus candidate mode),
checks schema0 -> schema1 with unchanged fields/Snapshot, then starts a fresh
JVM for each query. The SQL projects all 259 business columns PLUS `_ROW_ID`;
LIMIT adds `LIMIT 20`. See the reader source for the exact projection. An
unordered LIMIT need not choose the same 20 IDs in different arms. The full
consumer checks every business value/NULL and RowID domain; it collects only
partition summaries, not the table, on Driver. Separate exact validation checks
full rows, source, old key/RowID pairs and complete/inserted RowID domains.

The capacity comparison uses 512 MiB Driver/two 512 MiB Executors; the separate
exact validation uses 2 GiB Driver/two 512 MiB Executors. Successful-case timing
uses the common 2 GiB Driver/two 512 MiB Executors for ALL four modes. Keep raw
failures, preflight rejections and completed samples. Do not raise a failed
candidate's measurement heap or calculate speedup against a failed upstream.

Read timers include planning, execution and the common validation consumer;
they are not isolated planning CPU measurements. Reserve at least 5 GiB disk
and 3.2 GiB physical available memory, including temporary metadata stores,
runtime localization, bounded GC logs and all retained cohort outputs. The
generic copy controller needs extra disk beyond our single-copy protocol.

Performance protocol: exactly two position-balanced reverse blocks
`U F S L` / `L S F U`. This is not a complete Williams design. Report both raw
values/ranges per mode, without p95, significance or broad throughput claims.
Independent exact checks are reused once per mode rather than repeated for
every timing sample. Serial timing excludes construction, backup, cleanup and
offline analysis. The production three-million-file exit137 incident remains
unclassified; these controlled results do not establish that incident is fixed.
