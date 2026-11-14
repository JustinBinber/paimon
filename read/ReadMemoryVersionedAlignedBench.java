package bench;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Read-only consumer after exactly one 128-column fixture MERGE; validates all business columns. */
public final class ReadMemoryVersionedAlignedBench {
    private static volatile String verifiedJar;

    private static final String LEGACY_KEY =
            "data-evolution.metadata-memory-optimization.enabled";
    private static final String FEATURE_KEY =
            "data-evolution.metadata-memory-optimization.mode";

    private static void verifyMode(FileStoreTable table, String variant) throws Exception {
        require(table.coreOptions().dataEvolutionEnabled()
                        && table.coreOptions().rowTrackingEnabled(),
                "Expected a Data Evolution row-tracking table");
        require(!table.options().containsKey(LEGACY_KEY), "Legacy option must remain absent");
        Object actual = "upstream-not-applicable";
        Object mode = "upstream-not-applicable";
        Object streaming = "upstream-not-applicable";
        if (variant.equals("upstream")) {
            require(!table.options().containsKey(FEATURE_KEY), "Upstream must not receive candidate option");
        } else {
            require(variant.equals(table.options().get(FEATURE_KEY)), "Stored option differs from variant");
            Object core = table.coreOptions();
            actual = CoreOptions.class.getMethod("dataEvolutionMetadataMemoryOptimizationEnabled").invoke(core);
            mode = CoreOptions.class.getMethod("dataEvolutionMetadataMemoryMode").invoke(core);
            streaming = CoreOptions.class.getMethod("dataEvolutionMetadataStreamingEnabled").invoke(core);
            require(Boolean.valueOf(!variant.equals("off")).equals(actual), "Effective Core flag differs");
            require(variant.equals(mode.toString()), "Effective Core mode differs");
            require(Boolean.valueOf(variant.equals("streaming")).equals(streaming), "Streaming flag differs");
        }
        System.out.println("READ_MODE variant=" + variant + ";storedOption="
                + table.options().get(FEATURE_KEY) + ";coreGetter=" + actual
                + ";coreMode=" + mode + ";streamingGetter=" + streaming);
    }

    private static void prepare(SparkSession spark, Options options, String variant) throws Exception {
        FileStoreTable before = FileStoreTableFactory.create(CatalogContext.create(options));
        require(before.schema().id() == 0L, "Prepare requires a fresh schema0 fixture copy");
        require(before.coreOptions().dataEvolutionEnabled() && before.coreOptions().rowTrackingEnabled(),
                "Expected Data Evolution row tracking");
        require(before.coreOptions().snapshotNumRetainMin() == 10 && before.coreOptions().writeOnly(),
                "Expected unchanged min10/write-only fixture options");
        require(!before.options().containsKey(FEATURE_KEY)
                        && !before.options().containsKey(LEGACY_KEY), "Fixture already has candidate option");
        long snapshot = before.latestSnapshot().get().id();
        String properties = "'snapshot.num-retained.min'='10'";
        if (!variant.equals("upstream")) {
            properties += ", '" + FEATURE_KEY + "'='" + variant + "'";
        }
        spark.sql("ALTER TABLE paimon.bench.target SET TBLPROPERTIES (" + properties + ")").collectAsList();
        FileStoreTable after = FileStoreTableFactory.create(CatalogContext.create(options));
        require(after.schema().id() == 1L, "Expected exactly one schema advancement");
        require(after.schema().fields().equals(before.schema().fields()), "Setup changed fields");
        require(after.latestSnapshot().get().id() == snapshot, "Setup published a data Snapshot");
        require(after.coreOptions().snapshotNumRetainMin() == 10 && after.coreOptions().writeOnly(),
                "Setup changed effective common options");
        verifyMode(after, variant);
        System.out.println("READ_PREP completed=true;alterCount=1;originalSchema=0;currentSchema=1;"
                + "snapshotBefore=" + snapshot + ";snapshotAfter=" + after.latestSnapshot().get().id()
                + ";fieldsEqual=true;readPerformed=false;variant=" + variant);
    }


    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static synchronized void verifyJar(String expected) throws Exception {
        if (expected.equals(verifiedJar)) {
            return;
        }
        Path jar = Paths.get(CoreOptions.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        require(Files.isRegularFile(jar), "Expected packaged production JAR: " + jar);
        Path scanJar = Paths.get(Class.forName("org.apache.paimon.operation.DataEvolutionFileStoreScan")
                .getProtectionDomain().getCodeSource().getLocation().toURI());
        require(Files.isSameFile(jar, scanJar), "CoreOptions and DE scan have different origins");
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(jar)) {
            byte[] buffer = new byte[1024 * 1024];
            int length;
            while ((length = in.read(buffer)) != -1) {
                digest.update(buffer, 0, length);
            }
        }
        StringBuilder actual = new StringBuilder();
        for (byte value : digest.digest()) {
            actual.append(String.format("%02x", value & 255));
        }
        require(expected.equals(actual.toString()), "Unexpected production JAR: " + jar + "#" + actual);
        verifiedJar = expected;
        System.out.println("READ_BENCH verifiedJar=" + jar + "#" + actual);
    }

    // Totals: rows, invalid value, invalid payload, invalid id, invalid RowID,
    // id sum, RowID sum, id xor, RowID xor, invalid extra columns. No full table is collected.
    private static void consume(Row row, long oldRows, long expectedRows, int columns, long[] totals) {
        require(row.size() == 4 + columns, "Expected all business columns followed by _ROW_ID");
        totals[0]++;
        if (row.isNullAt(0)) {
            totals[3]++;
        } else {
            long id = row.getLong(0);
            if (id < 0 || id >= expectedRows) {
                totals[3]++;
            }
            boolean matchedOld = id < oldRows && id % 2L == 0L;
            boolean expectedNullValue = matchedOld && id % 10L == 0L;
            long expectedValue = matchedOld || id >= oldRows ? -id - 100L : id * 7L;
            if (expectedNullValue ? !row.isNullAt(1)
                    : row.isNullAt(1) || row.getLong(1) != expectedValue) {
                totals[1]++;
            }
            String expectedPayload = id % 7 == 0 ? null : "payload-" + id;
            if (!Objects.equals(expectedPayload, row.isNullAt(2) ? null : row.getString(2))) {
                totals[2]++;
            }
            for (int column = 0; column < columns; column++) {
                int index = 3 + column;
                boolean expectedNull = (id + column) % 11L == 0L;
                long expectedColumn = id * (column + 3L)
                        + (matchedOld && column < 127 ? 1000L : 0L);
                if (expectedNull ? !row.isNullAt(index)
                        : row.isNullAt(index) || row.getLong(index) != expectedColumn) {
                    totals[9]++;
                }
            }
            totals[5] += id;
            totals[7] ^= id;
        }
        if (row.isNullAt(3 + columns)) {
            totals[4]++;
        } else {
            long rowId = row.getLong(3 + columns);
            if (rowId < 0 || rowId >= expectedRows) {
                totals[4]++;
            }
            totals[6] += rowId;
            totals[8] ^= rowId;
        }
    }

    private static long xorThrough(long last) {
        switch ((int) (last & 3L)) {
            case 0: return last;
            case 1: return 1L;
            case 2: return last + 1L;
            default: return 0L;
        }
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 6,
                "warehouse prepare|limit20|full baseFiles expectedProductionJarSha256 extraColumns upstream|off|lightweight|streaming");
        final int columns = Integer.parseInt(args[4]);
        final String variant = args[5];
        require(variant.equals("upstream") || variant.equals("off")
                        || variant.equals("lightweight") || variant.equals("streaming"),
                "Unknown variant");
        final boolean setup = args[1].equals("prepare");
        require(columns >= 127 && columns <= 256, "Invalid versioned fixture width");
        final String warehouse = Paths.get(args[0]).toAbsolutePath().toString();
        final boolean limit = args[1].equals("limit20");
        require(setup || limit || args[1].equals("full"), "Unknown read mode");
        final long baseFiles = Long.parseLong(args[2]);
        require(baseFiles > 0 && baseFiles <= 10000000L, "Fixture dimension outside helper scope");
        final long oldRows = baseFiles * 2L;
        final long expectedRows = oldRows + baseFiles / 5L;
        final String expectedJar = args[3];
        verifyJar(expectedJar);
        Options options = new Options();
        options.setString("path", Paths.get(warehouse, "bench.db", "target").toUri().toString());
        SparkSession spark = SparkSession.builder().appName("read-memory-" + args[1])
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse", Paths.get(warehouse).toUri().toString())
                .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                .getOrCreate();
        try {
            require(spark.version().equals("3.5.9"), "Expected Spark3.5.9");
            if (setup) {
                prepare(spark, options, variant);
                return;
            }
            FileStoreTable table = FileStoreTableFactory.create(CatalogContext.create(options));
            require(table.rowType().getFieldCount() == 3 + columns, "Unexpected business schema width");
            final long snapshot = table.latestSnapshot().get().id();
            final long schema = table.schema().id();
            require(schema == 1L, "Read requires the separately prepared schema1 copy");
            require(table.coreOptions().snapshotNumRetainMin() == 10 && table.coreOptions().writeOnly(),
                    "Unexpected common options");
            verifyMode(table, variant);
            System.out.println("READ_BENCH snapshot=" + snapshot + ";schema=" + schema
                    + ";splitTargetBytes=" + table.coreOptions().splitTargetSize()
                    + ";openFileCostBytes=" + table.coreOptions().splitOpenFileCost()
                    + ";maxDriverHeap=" + Runtime.getRuntime().maxMemory());
            StringBuilder projection = new StringBuilder("SELECT id, value, payload");
            for (int column = 0; column < columns; column++) {
                projection.append(", x").append(column);
            }
            final String sql = projection.append(", _ROW_ID FROM paimon.bench.target")
                    .append(limit ? " LIMIT 20" : "").toString();
            System.out.println("READ_BENCH phaseStart=true;sql=" + sql);
            long start = System.nanoTime();
            Dataset<Row> data = spark.sql(sql);
            long[] totals;
            if (limit) {
                List<Row> rows = data.collectAsList();
                totals = new long[10];
                for (Row row : rows) {
                    consume(row, oldRows, expectedRows, columns, totals);
                }
            } else {
                JavaRDD<long[]> partials = data.javaRDD().mapPartitions(rows -> {
                    verifyJar(expectedJar);
                    long[] summary = new long[10];
                    while (rows.hasNext()) {
                        consume(rows.next(), oldRows, expectedRows, columns, summary);
                    }
                    return Collections.singletonList(summary).iterator();
                });
                totals = partials.fold(new long[10], (left, right) -> {
                    for (int i = 0; i < 7; i++) {
                        left[i] += right[i];
                    }
                    left[9] += right[9];
                    left[7] ^= right[7];
                    left[8] ^= right[8];
                    return left;
                });
            }
            long nanos = System.nanoTime() - start;
            require(totals[0] == (limit ? Math.min(20L, expectedRows) : expectedRows), "Unexpected row count");
            for (int i = 1; i < 5; i++) {
                require(totals[i] == 0, "Invalid field/domain at check " + i + ": " + totals[i]);
            }
            require(totals[9] == 0, "Invalid extra-column values: " + totals[9]);
            if (!limit) {
                long expectedSum = expectedRows * (expectedRows - 1L) / 2L;
                require(totals[5] == expectedSum && totals[6] == expectedSum, "Domain sum mismatch");
                require(totals[7] == xorThrough(expectedRows - 1L)
                        && totals[8] == xorThrough(expectedRows - 1L), "Domain xor mismatch");
            }
            require(table.latestSnapshot().get().id() == snapshot
                            && FileStoreTableFactory.create(CatalogContext.create(options)).schema().id() == schema,
                    "Read changed table identity");
            System.out.println("READ_BENCH readCompleted=true;rows=" + totals[0] + ";readAndValidationNanos=" + nanos
                    + ";extraColumns=" + columns + ";updatedBusinessColumns=128;versioned=true;perRowValueAndDomainChecks=passed;exactMultisetAndKeyRowIdValidation=notPerformed");
        } finally {
            spark.stop();
        }
    }
}
