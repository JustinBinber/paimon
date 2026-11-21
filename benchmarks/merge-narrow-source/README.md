# Narrow shared MERGE source: synthetic reproduction and screening evidence

This capsule accompanies Apache Paimon source commit
`4964431e3d56e161dd29cf87fe3920dd6881a6c2`. It contains the workload/validation
sources used for the nine observations in `samples.csv`, not production data or
SDK binaries. `validation-summary.json` preserves all nine correctness check
results and the original source/SDK hashes. The only portability edit to the
frozen workload sources is the default seed path in `spec.py`; pass `--seed`.
The README and service registration files are provided for reproduction.

## What was measured

One Linux physical host, 4 CPU cores / 16 GiB RAM; Spark 3.5.9 / Scala 2.12,
JDK 17; one 4-GiB Driver and two one-core / 1-GiB Executor JVMs using
`local-cluster[2,1,1024]`. This is not a multiple-physical-worker benchmark.
The SDK was built normally for Java 8, with no production-class overlay.

The target is a no-primary-key Data Evolution append table, bucket=-1,
row tracking enabled: 32,768 two-row files, 65,536 rows and 259 business columns.
UPDATE assigns 128 columns. The ordinary append source has 39,321 rows and,
where applicable, a genuinely stored logical 8-KiB INSERT-only payload.

F disables shared joins; R enables the existing full-width shared join;
N enables the new narrow-source option as well. Three scenarios were screened,
**one observation per arm**, in these actual orders:

| Scenario | Order | F / R / N complete SQL seconds |
|---|---|---|
| `insert90` | F R N | 77.8476 / 76.9516 / 72.6343 |
| `insert100` | R N F | 52.7268 / 53.3671 / 51.5751 |
| `insert90-idpayload` | N F R | 78.4095 / 71.8358 / 73.4211 |

These are screening observations, not statistical significance or a general
speedup guarantee. N/R in `insert90` reduced shuffle from 316.79 to 10.77 MB
and peak shared disk cache from 312.20 to 18.78 MB, but sampled process-tree
RSS increased from 3.78 to 4.80 GiB, approximately 27%; F was 4.66 GiB.
The no-wide-field control retained the existing wide path and N's single SQL
observation was 2.21% slower than R. All-INSERT fell back with no shared cache
or shuffle. Two warm post-validation INSERT-range queries are retained in
the CSV, including an all-INSERT N/R observation 4.28% slower; these do not
establish general query non-regression.

FileIO values are bytes returned by source Parquet streams, not physical disk
or network bytes. Process CPU/write counter differences are sampled interior
lower bounds, not complete process totals. There was no timed task failure or
disk spill. Complete 259-column EXCEPT ALL results, source immutability, old/new
RowIDs, snapshot advancement and physical layout checks passed for every arm.

## Build helpers

Requirements: Python >=3.9 with psutil, Spark 3.5.9 including PySpark, JDK 8
for the helper compilation, and the normally built Paimon Spark 3.5 SDK from
the source commit above. Use JDK 17 to run Spark. No code here authenticates
to a cloud or changes a formal table. Use new private output directories.

```sh
# In a Paimon checkout at the commit above (include its Maven dependencies):
mvn -B -ntp -Pfast-build,flink1,spark3 -pl paimon-spark/paimon-spark-3.5 -am \
  -DskipTests -Dtarget.java.version=1.8 install
# Set SDK, SPARK, JDK8 and JDK17 to your local installation paths.
python build_helpers.py --sdk "$SDK" --spark "$SPARK" --java-home "$JDK8" --output helpers.jar
mkdir fixture-classes
"$JDK8/bin/javac" -source 8 -target 8 -cp "$SDK:$SPARK/jars/*" \
  -d fixture-classes CommitMemoryBench.java
"$JDK8/bin/jar" cf fixture.jar -C fixture-classes .
```

## Calibrate before the cloud-sized run

Run the existing 2,048-file local pilot, without changing the dimensions or
using historical file sizes as a new-SDK bound. It uses a 1-GiB Driver,
`local[2]`, and consumes/validates all columns. Close other Java processes.

```sh
PILOT=/your/new/private/pilot
python run.py --execute --phase fixture --profile local --root "$PILOT" \
  --spark "$SPARK" --java-home "$JDK17" --sdk "$SDK" --helpers helpers.jar --fixture-jar fixture.jar
for phase in prepare measure validate; do
  python run.py --execute --phase "$phase" --profile local --root "$PILOT" \
    --name pilot --variant insert17 --mode N --spark "$SPARK" --java-home "$JDK17" \
    --sdk "$SDK" --helpers helpers.jar || exit 1
done
python calibrate.py --root "$PILOT" --name pilot --output calibration.json
```

## Generate the immutable original target once

Use a new warehouse and evidence directory on the same filesystem as the
later case roots. The Java fixture flushes exactly one two-row Parquet file
per iteration through the normal Paimon writer; the file count is not inferred
from `target-file-size`. Generation is outside SQL timing.

```sh
SEED=/your/new/private/seed
mkdir -p "$SEED" /your/new/private/seed-evidence
JAVA_HOME="$JDK17" "$SPARK/bin/spark-submit" --master 'local[2]' --driver-memory 4g \
  --conf spark.ui.enabled=false --driver-class-path "$SDK" --jars "$SDK" \
  --class bench.CommitMemoryBench fixture.jar fixture "$SEED" \
  /your/new/private/seed-evidence 32768 2 256 0
```

## Run one fresh case per arm, in the actual orders above

Pass the same SDK, helper, calibration and seed to each arm. Each CELL is a
different empty directory; never reuse an already attempted timed phase.
Use the same source regeneration, preparation and warm-up for every arm.
Every phase has its own fresh JVM. Keep other builds, queries and archiving
out of the timed interval. Keep at least 5 GiB free disk and 3.2 GiB available
RAM **in addition to** the launcher's estimated growth; admission may refuse
to launch rather than lower these floors. Size disk for retained evidence;
the runner does not delete completed cases.

```sh
# Example: first case of insert90; repeat with MODE=R and MODE=N in NEW CELL roots.
CELL=/your/new/private/insert90-F
VARIANT=insert90
MODE=F
for phase in prepare measure validate; do
  python run.py --execute --phase "$phase" --profile cloud --root "$CELL" --name case \
    --variant "$VARIANT" --mode "$MODE" --seed "$SEED" --spark "$SPARK" \
    --java-home "$JDK17" --sdk "$SDK" --helpers helpers.jar --budget-proof calibration.json || exit 1
done
```

The measured interval is `spark.sql(merge_sql).collect()`, including planning,
read, join, reread, write and commit. Generation, input warm-up, validation
and archiving are outside it. Do not compare SQL times across different
INSERT proportions as an optimization gain: those scenarios do different work.
`spec.py` includes a historical 45-cell schedule, but that schedule **was not
run for this screen**. Do not invoke `matrix` to reproduce these nine samples.

## Retained failures and limitations

The all-INSERT N case was rejected by disk admission before SQL launch.
Old reproducible build products were cleaned between its R and N observations;
the completed preparation was retained and only its missing measure phase
was started. This environmental event is not hidden. No successful SQL sample
was rerun or selectively replaced. No lower resource floor or disk expansion
was used. Results justify reviewing a default-off, bounded-scope option,
not enabling it by default. DE-source positional RowID reread is separate.

This public capsule is derived from locally byte-verified original evidence.
Its files have their own hashes in `manifest.json`; those are not presented
as the hashes of unmodified private archives. Private connection logs, raw
machine inventories, binaries and unrelated experiments are excluded.
