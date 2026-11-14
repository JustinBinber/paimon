package bench;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Standalone evidence helper; no production sources or formal tables are changed. */
public final class CommitMemoryBench {
    private static volatile boolean sampling;
    private static String identity;
    private static final String TARGET = "paimon.bench.target";
    private static final String SOURCE = "paimon.bench.source";

    private static void require(boolean ok, String message) {
        if (!ok) { throw new IllegalStateException(message); }
    }

    private static long pid() {
        return Long.parseLong(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
    }

    private static synchronized String identity() throws Exception {
        if (identity != null) { return identity; }
        StringBuilder result = new StringBuilder("pid=" + pid() + ";executor="
                + SparkEnv.get().executorId() + ";maxHeap=" + Runtime.getRuntime().maxMemory());
        for (String name : Arrays.asList("org.apache.paimon.table.sink.CommitMessageSerializer",
                "org.apache.paimon.spark.write.InnerTableWriteTaskResult")) {
            Class<?> type = Class.forName(name);
            Path jar = Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI());
            require(Files.isRegularFile(jar), "Expected packaged class origin: " + jar);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (java.io.InputStream in = Files.newInputStream(jar)) {
                byte[] bytes = new byte[1024 * 1024];
                int count;
                while ((count = in.read(bytes)) != -1) { digest.update(bytes, 0, count); }
            }
            StringBuilder hash = new StringBuilder();
            for (byte b : digest.digest()) { hash.append(String.format("%02x", b & 255)); }
            result.append(";").append(name).append("=").append(jar).append("#").append(hash);
        }
        Path writerJar = Paths.get(Class.forName(
                "org.apache.paimon.spark.commands.DataEvolutionPaimonWriter")
                .getProtectionDomain().getCodeSource().getLocation().toURI());
        Path serializerJar = Paths.get(CommitMessageSerializer.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        require(Files.isSameFile(writerJar, serializerJar),
                "DataEvolution writer origin differs from verified commit serializer: " + writerJar);
        result.append(";changedWriterOrigin=").append(writerJar);
        identity = result.toString();
        return identity;
    }

    private static synchronized void sample(String directory) throws Exception {
        if (sampling) { return; }
        sampling = true;
        Files.createDirectories(Paths.get(directory));
        Thread thread = new Thread(() -> {
            Path output = Paths.get(directory, "heap-" + pid() + ".jsonl");
            try {
                while (true) {
                    Runtime r = Runtime.getRuntime();
                    long gcCount = 0, gcMillis = 0;
                    for (java.lang.management.GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                        gcCount += Math.max(0, gc.getCollectionCount());
                        gcMillis += Math.max(0, gc.getCollectionTime());
                    }
                    String line = "{\"timeMillis\":" + System.currentTimeMillis() + ",\"pid\":" + pid()
                            + ",\"heapUsed\":" + (r.totalMemory() - r.freeMemory())
                            + ",\"heapCommitted\":" + r.totalMemory() + ",\"heapMax\":" + r.maxMemory()
                            + ",\"gcCount\":" + gcCount + ",\"gcMillis\":" + gcMillis + "}\n";
                    Files.write(output, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception failure) {
                System.err.println("BENCH_OBSERVER_FAILURE " + failure);
            }
        }, "commit-memory-heap-observer");
        thread.setDaemon(true);
        thread.start();
    }

    private static FileStoreTable table(String warehouse) {
        Options options = new Options();
        options.setString("path", Paths.get(warehouse, "bench.db", "target").toUri().toString());
        return FileStoreTableFactory.create(CatalogContext.create(options));
    }

    private static String extraSchema(int columns) {
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < columns; i++) { sql.append(", x").append(i).append(" BIGINT"); }
        return sql.toString();
    }

    private static String names(int columns) {
        StringBuilder sql = new StringBuilder("id, value, payload");
        for (int i = 0; i < columns; i++) { sql.append(", x").append(i); }
        return sql.toString();
    }

    private static String extras(String id, int columns) {
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < columns; i++) {
            sql.append(", CASE WHEN pmod(").append(id).append(" + ").append(i)
                    .append(", 11) = 0 THEN CAST(NULL AS BIGINT) ELSE ")
                    .append(id).append(" * ").append(i + 3).append("L END AS x").append(i);
        }
        return sql.toString();
    }

    private static String payload(String id) {
        return "CASE WHEN pmod(" + id + ", 7) = 0 THEN CAST(NULL AS STRING) ELSE concat('payload-', " + id + ") END";
    }

    private static String original(long rows, int columns) {
        return "SELECT id, id * 7L AS value, " + payload("id") + " AS payload"
                + extras("id", columns) + " FROM range(" + rows + ")";
    }

    private static String source(long files) {
        long rows = files * 2;
        return "SELECT id, CASE WHEN pmod(id, 10) = 0 THEN CAST(NULL AS BIGINT) ELSE -id - 100L END AS value"
                + " FROM range(0, " + rows + ", 2) UNION ALL SELECT id, -id - 100L AS value FROM range("
                + rows + ", " + (rows + files / 5) + ")";
    }

    private static GenericRow row(long id, int columns) {
        GenericRow row = new GenericRow(3 + columns);
        row.setField(0, id);
        row.setField(1, id * 7L);
        row.setField(2, id % 7 == 0 ? null : BinaryString.fromString("payload-" + id));
        for (int i = 0; i < columns; i++) { row.setField(3 + i, (id + i) % 11 == 0 ? null : id * (i + 3)); }
        return row;
    }

    private static void fixture(SparkSession spark, String warehouse, String evidence,
            long files, int tasks, int columns) throws Exception {
        require(files > 0 && tasks > 0 && tasks <= files, "Invalid fixture dimensions");
        require(!Files.exists(Paths.get(warehouse, "bench.db", "target")), "Fixture target already exists");
        spark.sql("CREATE DATABASE IF NOT EXISTS paimon.bench");
        StringBuilder splitOptions = new StringBuilder();
        for (String key : Arrays.asList("source.split.target-size", "source.split.open-file-cost")) {
            String value = spark.conf().get("bench.table." + key, "");
            if (!value.isEmpty()) {
                require(value.matches("(?i)[0-9]+(b|kb|mb|gb)"), "Invalid split size: " + value);
                splitOptions.append(", '").append(key).append("'='").append(value).append("'");
            }
        }
        spark.sql("CREATE TABLE " + TARGET + " (id BIGINT, value BIGINT, payload STRING" + extraSchema(columns)
                + ") USING paimon TBLPROPERTIES ('bucket'='-1', 'row-tracking.enabled'='true',"
                + " 'data-evolution.enabled'='true', 'file.format'='parquet', 'write-only'='true', 'target-file-size'='128mb'"
                + splitOptions + ")");
        List<Integer> work = new ArrayList<>();
        for (int i = 0; i < tasks; i++) { work.add(i); }
        final String commitUser = "fixture-" + java.util.UUID.randomUUID();
        List<String> shards = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .parallelize(work, tasks).map(task -> {
                    sample(evidence);
                    FileStoreTable target = table(warehouse).copy(Collections.singletonMap("write-buffer-spillable", "false"));
                    Path shard = Paths.get(evidence, "fixture-task-" + task + "-" + TaskContext.get().taskAttemptId() + ".bin");
                    CommitMessageSerializer serializer = new CommitMessageSerializer();
                    try (StreamTableWrite writer = target.newStreamWriteBuilder().withCommitUser(commitUser).newWrite();
                            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(shard)))) {
                        out.writeInt(serializer.getVersion());
                        for (long file = files * task / tasks; file < files * (task + 1) / tasks; file++) {
                            writer.write(row(2 * file, columns));
                            writer.write(row(2 * file + 1, columns));
                            List<CommitMessage> changes = writer.prepareCommit(false, file);
                            require(changes.size() == 1, "Unexpected fixture message count");
                            CommitMessageImpl change = (CommitMessageImpl) changes.get(0);
                            require(change.newFilesIncrement().newFiles().size() == 1
                                    && change.newFilesIncrement().newFiles().get(0).rowCount() == 2,
                                    "Fixture must create exactly one real two-row file per flush");
                            byte[] bytes = serializer.serialize(change);
                            out.writeLong(file);
                            out.writeInt(bytes.length);
                            out.write(bytes);
                        }
                    }
                    System.out.println("BENCH fixtureTask=" + task + ";" + identity());
                    return shard.toString();
                }).collect();
        // Generation is outside capacity measurement; bounded message batches use the real committer.
        StreamWriteBuilder builder = table(warehouse).newStreamWriteBuilder().withCommitUser(commitUser);
        try (StreamTableCommit commit = builder.newCommit()) {
            List<CommitMessage> pending = new ArrayList<>();
            long identifier = -1;
            for (String shard : shards) {
                try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(shard))))) {
                    int version = in.readInt();
                    while (true) {
                        long next;
                        try { next = in.readLong(); } catch (EOFException end) { break; }
                        require(next > identifier, "Out-of-order fixture shard");
                        identifier = next;
                        int length = in.readInt();
                        require(length > 0 && length < 64 * 1024 * 1024, "Bad fixture message frame");
                        byte[] bytes = new byte[length];
                        in.readFully(bytes);
                        pending.add(new CommitMessageSerializer().deserialize(version, bytes));
                        if (pending.size() == 1024) { commit.commit(identifier, pending); pending.clear(); }
                    }
                }
            }
            if (!pending.isEmpty()) { commit.commit(identifier, pending); }
        }
        inspect(warehouse, files, true);
        Dataset<Row> expected = spark.sql(original(files * 2, columns));
        checkSame(spark.sql("SELECT " + names(columns) + " FROM " + TARGET), expected, "seed rows");
        spark.sql("SELECT id, _ROW_ID AS row_id FROM " + TARGET).write().mode("errorifexists")
                .parquet(Paths.get(warehouse, "seed-rowids").toString());
        spark.sql("CREATE TABLE " + SOURCE + " USING paimon AS " + source(files));
        checkSame(spark.sql("SELECT * FROM " + SOURCE), spark.sql(source(files)), "source rows");
        System.out.println("BENCH fixtureValidated=true;files=" + files + ";rows=" + (2 * files)
                + ";sourceRows=" + (files + files / 5) + ";extraColumns=" + columns);
    }

    private static void checkSame(Dataset<Row> actual, Dataset<Row> expected, String name) {
        long unexpected = actual.exceptAll(expected).count();
        long missing = expected.exceptAll(actual).count();
        System.out.println("BENCH correctness=" + name + ";unexpected=" + unexpected + ";missing=" + missing);
        require(unexpected == 0 && missing == 0, "Full multiset mismatch: " + name);
    }

    private static void inspect(String warehouse, long expectedFiles, boolean seed) {
        FileStoreTable target = table(warehouse);
        List<ManifestEntry> entries = target.store().newScan().plan().files();
        Set<String> identities = new HashSet<>();
        long rows = 0, bytes = 0, insertedFiles = 0, insertedRows = 0;
        for (ManifestEntry entry : entries) {
            String identity = entry.partition() + ":" + entry.bucket() + ":" + entry.file().fileName();
            require(identities.add(identity), "Duplicate effective file metadata: " + identity);
            require(entry.file().firstRowId() != null, "Unassigned row ID");
            rows += entry.file().rowCount();
            bytes += entry.file().fileSize();
            if (entry.file().firstRowId() >= 2 * expectedFiles) {
                insertedFiles++;
                insertedRows += entry.file().rowCount();
            }
        }
        require(!seed || entries.size() == expectedFiles, "Unexpected effective seed file count");
        System.out.println("BENCH snapshot=" + target.store().snapshotManager().latestSnapshotId()
                + ";effectiveFiles=" + entries.size() + ";fileRows=" + rows + ";fileBytes=" + bytes
                + ";insertFiles=" + insertedFiles + ";insertFileRows=" + insertedRows);
        System.out.println("BENCH tableOptions=" + new java.util.TreeMap<>(target.options()));
    }

    private static void describeTable(String warehouse) {
        FileStoreTable target = table(warehouse);
        System.out.println("BENCH tableOptions=" + new java.util.TreeMap<>(target.options()));
        System.out.println("BENCH splitTargetBytes=" + target.coreOptions().splitTargetSize()
                + ";splitOpenFileCostBytes=" + target.coreOptions().splitOpenFileCost());
    }

    private static void processGate(SparkSession spark, String evidence, int executors) throws Exception {
        List<Integer> work = new ArrayList<>();
        for (int i = 0; i < 16; i++) { work.add(i); }
        List<String> results = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .parallelize(work, work.size()).map(i -> {
                    sample(evidence);
                    Thread.sleep(250);
                    return identity();
                }).collect();
        Set<String> unique = new HashSet<>(results);
        for (String result : unique) { System.out.println("BENCH executorIdentity=" + result); }
        require(unique.size() >= executors, "Required executor process count was not observed");
        for (String result : unique) {
            require(!result.startsWith("pid=" + pid() + ";"), "Executor shares driver PID");
        }
    }

    private static void merge(SparkSession spark, int columns) {
        StringBuilder insert = new StringBuilder("s.id, s.value, " + payload("s.id"));
        for (int i = 0; i < columns; i++) {
            insert.append(", CASE WHEN pmod(s.id + ").append(i)
                    .append(", 11) = 0 THEN CAST(NULL AS BIGINT) ELSE s.id * ").append(i + 3).append("L END");
        }
        String sql = "MERGE INTO " + TARGET + " t USING " + SOURCE + " s ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET t.value = s.value WHEN NOT MATCHED THEN INSERT ("
                + names(columns) + ") VALUES (" + insert + ")";
        System.out.println("BENCH measuredSql=" + sql);
        long start = System.nanoTime();
        spark.sql(sql).collectAsList();
        System.out.println("BENCH sqlCompleted=true;sqlNanos=" + (System.nanoTime() - start));
        // Snapshot identity only; full manifest inspection is an independent validation phase.
        System.out.println("BENCH publishedSnapshot="
                + table(spark.conf().get("bench.warehouse")).store().snapshotManager().latestSnapshotId());
    }

    private static void validate(SparkSession spark, String warehouse, long files, int columns) {
        long oldRows = files * 2, inserted = files / 5;
        String expected = "SELECT id, CASE WHEN pmod(id, 2) = 0 THEN CASE WHEN pmod(id, 10) = 0"
                + " THEN CAST(NULL AS BIGINT) ELSE -id - 100L END ELSE id * 7L END AS value, "
                + payload("id") + " AS payload" + extras("id", columns) + " FROM range(" + oldRows + ")"
                + " UNION ALL SELECT id, -id - 100L AS value, " + payload("id") + " AS payload"
                + extras("id", columns) + " FROM range(" + oldRows + ", " + (oldRows + inserted) + ")";
        checkSame(spark.sql("SELECT " + names(columns) + " FROM " + TARGET), spark.sql(expected), "merged rows");
        checkSame(spark.sql("SELECT * FROM " + SOURCE), spark.sql(source(files)), "unchanged source");
        Dataset<Row> mapping = spark.read().parquet(Paths.get(warehouse, "seed-rowids").toString());
        checkSame(spark.sql("SELECT id, _ROW_ID AS row_id FROM " + TARGET + " WHERE id < " + oldRows), mapping, "old key row-ID pairs");
        checkSame(spark.sql("SELECT _ROW_ID AS row_id FROM " + TARGET),
                spark.sql("SELECT id AS row_id FROM range(" + (oldRows + inserted) + ")"), "complete row-ID domain");
        checkSame(spark.sql("SELECT _ROW_ID AS row_id FROM " + TARGET + " WHERE id >= " + oldRows),
                spark.sql("SELECT id AS row_id FROM range(" + oldRows + ", " + (oldRows + inserted) + ")"), "inserted row-ID domain");
        inspect(warehouse, files, false);
        System.out.println("BENCH validationCompleted=true;expectedRows=" + (oldRows + inserted));
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 7, "phase warehouse evidence files tasks extraColumns requiredExecutors");
        String phase = args[0], warehouse = Paths.get(args[1]).toAbsolutePath().toString();
        String evidence = Paths.get(args[2]).toAbsolutePath().toString();
        long files = Long.parseLong(args[3]);
        int tasks = Integer.parseInt(args[4]), columns = Integer.parseInt(args[5]), requiredExecutors = Integer.parseInt(args[6]);
        require(columns >= 0 && columns <= 256 && files > 0, "Invalid dimensions");
        sample(evidence);
        SparkSession spark = SparkSession.builder().appName("commit-memory-" + phase)
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse", Paths.get(warehouse).toUri().toString())
                .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .config("bench.warehouse", warehouse).getOrCreate();
        try {
            require(spark.version().equals("3.5.9"), "Expected pinned Spark3.5.9, got " + spark.version());
            System.out.println("BENCH driverIdentity=" + identity());
            System.out.println("BENCH sparkVersion=" + spark.version() + ";master=" + spark.sparkContext().master());
            if (phase.equals("fixture")) { fixture(spark, warehouse, evidence, files, tasks, columns); }
            else if (phase.equals("validate")) { validate(spark, warehouse, files, columns); }
            else if (phase.equals("inspect")) { inspect(warehouse, files, false); }
            else if (phase.equals("merge") || phase.equals("limit20") || phase.equals("read")) {
                if (phase.equals("merge")) {
                    FileStoreTable beforeSetup = table(warehouse);
                    require(beforeSetup.schema().id() == 0L, "Expected original schema0");
                    require(beforeSetup.coreOptions().snapshotNumRetainMin() == 10, "Original effective minimum must be10");
                    require(beforeSetup.coreOptions().writeOnly(), "Expected write-only fixture");
                    if (files == 32L) {
                        List<ManifestEntry> initial = beforeSetup.store().newScan().plan().files();
                        require(initial.size() == 32, "Expected exactly32 original files");
                        for (ManifestEntry entry : initial) {
                            require(entry.file().schemaId() == 0L, "Original file schema must be0");
                        }
                        System.out.println("ALIGNED_UPSTREAM originalFileSchema=0;files=32;directMetadataCheck=true");
                    }
                    final String legacyKey = "data-evolution.metadata-memory-optimization.enabled";
                    final String key = "data-evolution.metadata-memory-optimization.mode";
                    final String variant = spark.conf().get("spark.bench.memory.variant");
                    require(variant.equals("upstream") || variant.equals("off")
                                    || variant.equals("lightweight") || variant.equals("streaming"),
                            "Expected explicit upstream|off|lightweight|streaming benchmark variant");
                    require(!beforeSetup.options().containsKey(key)
                                    && !beforeSetup.options().containsKey(legacyKey),
                            "Fresh fixture must omit candidate options");
                    final long beforeSnapshot = beforeSetup.latestSnapshot().get().id();
                    String properties = "'snapshot.num-retained.min'='10'";
                    if (!variant.equals("upstream")) {
                        properties += ", '" + key + "'='" + variant + "'";
                    }
                    spark.sql("ALTER TABLE " + TARGET + " SET TBLPROPERTIES (" + properties + ")").collectAsList();
                    FileStoreTable afterSetup = table(warehouse);
                    require(afterSetup.schema().id() == 1L, "Exactly one schema advancement required");
                    require("10".equals(afterSetup.options().get("snapshot.num-retained.min")), "Persisted minimum mismatch");
                    require(afterSetup.coreOptions().snapshotNumRetainMin() == 10 && afterSetup.coreOptions().writeOnly(), "Effective options changed");
                    require(afterSetup.schema().fields().equals(beforeSetup.schema().fields()),
                            "Preparation changed table fields");
                    require(afterSetup.latestSnapshot().get().id() == beforeSnapshot,
                            "Preparation published a data snapshot");
                    require(!afterSetup.options().containsKey(legacyKey), "Legacy option was unexpectedly added");
                    Object getter = "upstream-not-applicable";
                    Object mode = "upstream-not-applicable";
                    Object streaming = "upstream-not-applicable";
                    if (variant.equals("upstream")) {
                        require(!afterSetup.options().containsKey(key), "Upstream received candidate option");
                    } else {
                        require(variant.equals(afterSetup.options().get(key)), "Stored variant mismatch");
                        Object core = afterSetup.coreOptions();
                        getter = core.getClass().getMethod("dataEvolutionMetadataMemoryOptimizationEnabled").invoke(core);
                        mode = core.getClass().getMethod("dataEvolutionMetadataMemoryMode").invoke(core);
                        streaming = core.getClass().getMethod("dataEvolutionMetadataStreamingEnabled").invoke(core);
                        require(Boolean.valueOf(!variant.equals("off")).equals(getter), "Effective Core flag mismatch");
                        require(variant.equals(mode.toString()), "Effective Core mode mismatch");
                        require(Boolean.valueOf(variant.equals("streaming")).equals(streaming), "Streaming getter mismatch");
                    }
                    System.out.println("BENCH alignedSetup=true;setupOutsideMeasuredSql=true;alterCount=1;currentSchema=1;"
                            + "originalSchema=0;effectiveSnapshotMin=10;writeOnly=true;fieldsEqual=true;"
                            + "snapshotUnchanged=true;variant=" + variant + ";storedOption="
                            + afterSetup.options().get(key) + ";coreGetter=" + getter
                            + ";coreMode=" + mode + ";streamingGetter=" + streaming);
                }
                describeTable(warehouse);
                if (requiredExecutors > 0) { processGate(spark, evidence, requiredExecutors); }
                System.out.println("BENCH phaseStart=" + phase + ";timeMillis=" + System.currentTimeMillis());
                if (phase.equals("merge")) { merge(spark, columns); }
                else {
                    System.out.println("BENCH measuredSql=SELECT * FROM " + TARGET + (phase.equals("limit20") ? " LIMIT 20" : ""));
                    long start = System.nanoTime();
                    // foreach consumes all projected columns without collecting a full table to the driver.
                    if (phase.equals("limit20")) { spark.sql("SELECT * FROM " + TARGET + " LIMIT 20").collectAsList(); }
                    else { spark.sql("SELECT * FROM " + TARGET).foreachPartition((org.apache.spark.api.java.function.ForeachPartitionFunction<Row>) rows -> {
                        while (rows.hasNext()) { Row row = rows.next(); for (int i = 0; i < row.size(); i++) { row.get(i); } }
                    }); }
                    System.out.println("BENCH readCompleted=true;readNanos=" + (System.nanoTime() - start));
                }
            } else { throw new IllegalArgumentException("Unknown phase: " + phase); }
        } finally { spark.stop(); }
    }
}
