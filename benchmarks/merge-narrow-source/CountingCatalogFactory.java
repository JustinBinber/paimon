package bench;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.spark.TaskContext;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Bench-only source attribution; identical helper jar and catalog for every arm. */
public class CountingCatalogFactory implements CatalogFactory {
    @Override
    public String identifier() { return "bench-counted"; }

    @Override
    public Catalog create(CatalogContext context) {
        Options options = Options.fromMap(context.options().toMap());
        String directory = options.get("bench.metrics-directory");
        if (directory == null || directory.isEmpty()) {
            throw new IllegalArgumentException("bench.metrics-directory is required");
        }
        options.setString("metastore", "filesystem");
        return CatalogFactory.createUnwrappedCatalog(
                CatalogContext.create(options, context.hadoopConf(), new Loader(directory), null),
                CountingCatalogFactory.class.getClassLoader());
    }

    public static class Loader implements FileIOLoader {
        private static final long serialVersionUID = 1L;
        private final String directory;
        public Loader(String directory) { this.directory = directory; }
        @Override
        public String getScheme() { return "file"; }
        @Override
        public FileIO load(Path path) { return new CountedLocalIO(directory); }
    }

    public static class CountedLocalIO extends LocalFileIO {
        private static final long serialVersionUID = 1L;
        private final String directory;
        public CountedLocalIO(String directory) { this.directory = directory; }
        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream input = super.newInputStream(path);
            String file = path.toUri().getPath().replace('\\', '/');
            if (!file.contains("/bench.db/source/") || !file.endsWith(".parquet")) {
                return input;
            }
            return new CountedStream(input, directory, file);
        }
    }

    // Preserve positional/vector reads; merely wrapping SeekableInputStream would disable them.
    private static class CountedStream extends SeekableInputStream implements VectoredReadable {
        private final SeekableInputStream input;
        private final String directory;
        private final Map<String, Object> event = new LinkedHashMap<>();
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicLong reads = new AtomicLong();
        private final AtomicLong seeks = new AtomicLong();
        private final AtomicLong positionalReads = new AtomicLong();
        private final AtomicBoolean closed = new AtomicBoolean();

        CountedStream(SeekableInputStream input, String directory, String path) {
            this.input = input;
            this.directory = directory;
            event.put("type", "sourceFileIOClose");
            event.put("pid", ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
            event.put("processStartEpochMillis", ManagementFactory.getRuntimeMXBean().getStartTime());
            event.put("openEpochMillis", System.currentTimeMillis());
            event.put("path", path);
            TaskContext task = TaskContext.get();
            event.put("taskAttemptId", task == null ? null : task.taskAttemptId());
            event.put("stageId", task == null ? null : task.stageId());
            event.put("partitionId", task == null ? null : task.partitionId());
            event.put("attemptNumber", task == null ? null : task.attemptNumber());
        }

        private int counted(int amount) {
            reads.incrementAndGet();
            if (amount > 0) { bytes.addAndGet(amount); }
            return amount;
        }
        @Override
        public int read() throws IOException {
            int value = input.read();
            counted(value < 0 ? -1 : 1);
            return value;
        }
        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            return counted(input.read(buffer, offset, length));
        }
        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            positionalReads.incrementAndGet();
            return counted(((VectoredReadable) input).pread(position, buffer, offset, length));
        }
        @Override
        public void seek(long desired) throws IOException {
            input.seek(desired);
            seeks.incrementAndGet();
        }
        @Override
        public long getPos() throws IOException { return input.getPos(); }
        @Override
        public int available() throws IOException { return input.available(); }
        @Override
        public int minSeekForVectorReads() { return ((VectoredReadable) input).minSeekForVectorReads(); }
        @Override
        public int batchSizeForVectorReads() { return ((VectoredReadable) input).batchSizeForVectorReads(); }
        @Override
        public int parallelismForVectorReads() { return ((VectoredReadable) input).parallelismForVectorReads(); }
        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) { return; }
            boolean success = false;
            try {
                input.close();
                success = true;
            } finally {
                event.put("epochMillis", System.currentTimeMillis());
                event.put("bytes", bytes.get());
                event.put("reads", reads.get());
                event.put("seeks", seeks.get());
                event.put("positionalReads", positionalReads.get());
                event.put("closeSucceeded", success);
                emit(directory, event);
            }
        }
    }

    private static synchronized void emit(String directory, Map<String, Object> event) throws IOException {
        java.nio.file.Path folder = Paths.get(directory);
        Files.createDirectories(folder);
        java.nio.file.Path path = folder.resolve("source-io-" + event.get("pid") + ".jsonl");
        byte[] data = (new ObjectMapper().writeValueAsString(event) + "\n").getBytes("UTF-8");
        Files.write(path, data, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
