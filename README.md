# Data Evolution many-file MERGE reproduction

This source-only kit reuses the helper and controller used in the experiments.
It contains no Paimon/Spark binary, cloud access code or credential. Packaging
these files is not a claim that this assembled kit was executed on a fresh host.
See RESULTS.md for accepted capacity and 32-file results. The 65536-file
performance campaign is running; its final measurements are not yet reported.

Use a Linux scratch directory, Python 3, Spark 3.5.9/Scala 2.12.18 and JDK 17
for the SQL processes. Build the Spark3 Paimon connector with JDK 8 using normal
Maven checks, in separate clean upstream and candidate source/output directories:

```bash
mvn -B -ntp -pl paimon-spark/paimon-spark-3.5 -am -Pspark3 -DskipTests package
```

This command compiles tests but does not execute them. Pin clean upstream to
`b7cfb397de334d56b8c3eaeb60a2cfa9c9cab627`; apply the PR only in the candidate
checkout. For the reported fixture format, also build an unmodified
`d98f08784c2cf9aed42cc759b3f9c1b3c9bec628` connector with the same command.
Use that third artifact only to compile the helper and generate the seed;
the comparison runtime remains fixed-b7 versus the candidate.
Prevent reuse of unrelated Paimon SNAPSHOT artifacts and retain the
actual build command, terminal code and log. The experiments used a shared warm
dependency cache with separate output trees and verified current reactor/module
class bindings; they did not use physically independent caches.

For each artifact, supply a provenance JSON containing its actual
`artifactSha256`, successful `buildExitCode`, source identity and build evidence.
The controller checks the hash and exit code; the JSON alone does not prove the
build. All paths below are caller-selected scratch paths.

```bash
set -euo pipefail
export KIT=/data/merge-reproduction
export SPARK_HOME=/opt/spark-3.5.9-bin-hadoop3
export JAVA_HOME=/opt/jdk17
export PATH="$JAVA_HOME/bin:$PATH"
export BASE_JAR=/data/artifacts/upstream.jar
export CAND_JAR=/data/artifacts/candidate.jar
export BASE_PROOF=/data/artifacts/upstream-provenance.json
export CAND_PROOF=/data/artifacts/candidate-provenance.json
export FIXTURE_JAR=/data/artifacts/unmodified-d98-fixture.jar
export FIXTURE_PROOF=/data/artifacts/unmodified-d98-fixture-provenance.json
export OUT=/data/merge-reproduction-new-run
mkdir "$OUT"
PAIMON_JAR="$FIXTURE_JAR" HARNESS_BUILD="$OUT/helper" bash "$KIT/build-helper.sh"
HELPER="$OUT/helper/commit-memory-harness.jar"
```

The historical frozen seed and helper binary were created with the earlier
unmodified `d98f08784c2cf9aed42cc759b3f9c1b3c9bec628` connector. That fixture
origin is separate from the fixed-b7 comparison runtime. This helper retains
the original generator methods unchanged; its mode-specific checks run during
measured operations. A newly generated seed
has different file UUIDs and hashes: record its identity, then use the same
immutable seed for every arm instead of claiming byte identity with our seed.

Start with `N=32`. The generator writes real two-row files and commits batches
through Paimon; its synthetic table has no primary key, bucket=-1, Data Evolution
and row tracking enabled. It aligns columns by RowID/version, not primary-key LSM.

```bash
N=32
ROOT="$OUT/narrow-$N"
COMMON=(--root "$ROOT" --files "$N" --tasks 8 --extra-columns 0
  --spark-home "$SPARK_HOME" --harness-jar "$HELPER"
  --conf spark.sql.shuffle.partitions=200 --min-disk-free-gib 5 --scale-authorized)
python3 -B "$KIT/bench.py" "${COMMON[@]}" --phase fixture --label generate \
  --paimon-jar "$FIXTURE_JAR" --provenance "$FIXTURE_PROOF" \
  --driver-memory 2g --planned-rss-gib 5.5
for MODE in upstream off lightweight streaming; do
  ART="$CAND_JAR"; PROOF="$CAND_PROOF"
  if [ "$MODE" = upstream ]; then ART="$BASE_JAR"; PROOF="$BASE_PROOF"; fi
  LABEL="gate-$MODE"
  python3 -B "$KIT/bench.py" "${COMMON[@]}" --phase merge --label "$LABEL" \
    --conf "spark.bench.memory.variant=$MODE" --paimon-jar "$ART" --provenance "$PROOF" \
    --master 'local-cluster[2,1,512]' --driver-memory 1g --executor-memory 512m \
    --required-executors 2 --planned-rss-gib 4.5
  python3 -B "$KIT/bench.py" "${COMMON[@]}" --phase validate --label "$LABEL-check" \
    --measurement-label "$LABEL" --paimon-jar "$BASE_JAR" --provenance "$BASE_PROOF" \
    --master 'local-cluster[2,1,512]' --driver-memory 2g --executor-memory 512m \
    --required-executors 2 --planned-rss-gib 5.5
done
```

Each MERGE uses a fresh physical private copy. Exactly one ALTER before the SQL
timer sets retention minimum 10; candidate modes are set in that same ALTER.
The recorded setup marker verifies unchanged fields/Snapshot and effective mode.
The measured SQL is:

```sql
MERGE INTO paimon.bench.target t USING paimon.bench.source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN INSERT (id, value, payload)
VALUES (s.id, s.value, CASE WHEN pmod(s.id, 7) = 0 THEN CAST(NULL AS STRING)
                          ELSE concat('payload-', s.id) END)
```

Require all five exact bidirectional checks, not only COUNT: full rows, unchanged
source, old key/RowID pairs, complete RowID domain and inserted RowID domain.
Also check immutable seed, prior Snapshot bytes and one atomic new Snapshot.
Preserve failed processes, logs and side effects; do not retry an arm with more
heap or a different SQL. `set -e` above deliberately stops on a failure.

For the capacity case use a separately admitted `N=524288` root, the same
1g/512m/512m measured heaps and SQL. The separate correctness oracle uses
4g/1g/1g (`local-cluster[2,1,1024]`, planned RSS 8 GiB). It is not part of the
small-heap capacity claim. Reserve at least 5 GiB disk and 3.2 GiB memory, and
budget private copies, live output, metadata inventories, logs, spill and inodes
before starting. A successful upstream run is not required for a valid negative
capacity observation, but failures cannot serve as latency baselines.

Measure successful-case performance at N=32 and N=65536 using four fresh arms
per mode, with this balanced order (U=upstream, F=off, L=lightweight, S=streaming):

```text
U1 F1 S1 L1
F2 L2 U2 S2
L3 S3 F3 U3
S4 U4 L4 F4
```

Keep prerequisite runs out of these samples. Retain full SQL time, planning and
commit phases, heap/RSS, CPU, GC, Shuffle and process disk I/O for every arm.
The actual experiment controllers additionally bound rotating GC logs and check
the actual three JVM heap arguments and process identities. Do not call this
single-host, multiple-process setup a multiple-node cluster benchmark. A MERGE
result does not establish repair of the separate production SELECT/LIMIT case.
