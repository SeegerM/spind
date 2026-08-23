package io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.RelationMetadata;
import structures.SortJob;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Splits the value stream into hash partitions, replacing the global sort.
 *
 * <p>SPIND sorts everything so that equal values meet in one merged stream. Meeting is all the
 * validator needs — the ordering itself is incidental. Hashing gives the same guarantee for
 * strictly less work: equal canonical values always land in the same partition, so a partition can
 * be validated on its own and no merge step is required to reconcile partitions afterwards.</p>
 *
 * <p>Partitions are bounded by <em>distinct</em> values, not occurrences, because entries are
 * aggregated on the way in. A value appearing ten million times is one entry, so frequency skew
 * cannot blow up a partition — only distinct-value skew can, and that is what re-partitioning with
 * further hash bits addresses.</p>
 *
 * <p>Files use the same {@code value \n attrId,count;... \n} layout the sorter spills, so the same
 * parsing serves both paths.</p>
 */
public final class Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(Partitioner.class);

    /** Hash bits consumed per recursion level; 5 gives 32 ways per level, 6 levels before exhaustion. */
    public static final int BITS_PER_LEVEL = 5;
    public static final int PARTITIONS_PER_LEVEL = 1 << BITS_PER_LEVEL;

    private final Config config;

    public Partitioner(Config config) {
        this.config = config;
    }

    /**
     * @param files            one file list per partition
     * @param recordsWritten   entries written per partition; an upper bound on its distinct values,
     *                         since the same value may arrive from several tasks
     * @param totalValues      non-null occurrences per attribute, exact
     * @param nullValues       null occurrences per attribute, exact
     */
    public record Result(List<List<Path>> files, long[] recordsWritten, long[] totalValues, long[] nullValues) {
    }

    /**
     * Which partition a value belongs to at a given recursion level.
     *
     * Each level consumes its own slice of hash bits, so values that collided at one level get a
     * fresh chance to separate at the next. The hash is mixed first because
     * {@link String#hashCode()} leaves heavy structure in its low bits, which would defeat the
     * split for values sharing a prefix or a length.
     */
    public static int partitionOf(String value, int level) {
        int hash = mix(value.hashCode());
        int shift = level * BITS_PER_LEVEL;
        return (hash >>> shift) & (PARTITIONS_PER_LEVEL - 1);
    }

    /** Whether any hash bits remain for a further split. */
    public static boolean canSplitFurther(int level) {
        return (level + 1) * BITS_PER_LEVEL <= 32;
    }

    private static int mix(int hash) {
        // murmur3 finalizer
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;
        return hash;
    }

    /** Reads the chunk files and writes level-0 partitions, gathering per-attribute counts as it goes. */
    public Result fromChunks(RelationMetadata[] relations, Attribute[] attributes, String tag)
            throws InterruptedException {

        List<List<ChunkRef>> groups = groupChunks(relations);
        List<Callable<TaskOutput>> tasks = new ArrayList<>();
        for (int taskId = 0; taskId < groups.size(); taskId++) {
            int id = taskId;
            List<ChunkRef> group = groups.get(taskId);
            tasks.add(() -> {
                try (PartitionWriters writers = new PartitionWriters(config, tag, id)) {
                    Aggregator aggregator = new Aggregator(writers, 0);
                    TaskOutput output = new TaskOutput(attributes.length);
                    for (ChunkRef chunk : group) {
                        ingestChunk(chunk, aggregator, output);
                    }
                    aggregator.flush();
                    output.records = writers.recordsWritten();
                    output.files = writers.paths();
                    return output;
                }
            });
        }
        return runTasks(tasks, attributes.length);
    }

    /**
     * Reads the sources directly and writes level-0 partitions, never materializing chunks.
     *
     * Relations are grouped so that one writer set serves several of them, keeping the file count
     * at {@code PARALLEL * 32} rather than one set per relation. A relation is a stream and cannot
     * be split, so parallelism here is bounded by the relation count and, more sharply, by the
     * largest relation.
     */
    public Result fromSources(RelationMetadata[] relations, Attribute[] attributes, String tag)
            throws InterruptedException {

        // Work units are heterogeneous by design: a chunk of a large relation, or a whole small
        // relation. Chunked relations therefore still spread across workers while unchunked ones
        // cost nothing to prepare.
        List<Unit> units = new ArrayList<>();
        for (RelationMetadata relation : relations) {
            if (relation.chunked) {
                for (Path chunk : relation.chunks) {
                    units.add(new Unit(relation, chunk));
                }
            } else {
                units.add(new Unit(relation, null));
            }
        }

        List<List<Unit>> groups = balanceUnits(units, config.PARALLEL);
        List<Callable<TaskOutput>> tasks = new ArrayList<>();
        for (int taskId = 0; taskId < groups.size(); taskId++) {
            int id = taskId;
            List<Unit> group = groups.get(taskId);
            tasks.add(() -> {
                try (PartitionWriters writers = new PartitionWriters(config, tag, id)) {
                    Aggregator aggregator = new Aggregator(writers, 0);
                    TaskOutput output = new TaskOutput(attributes.length);
                    for (Unit unit : group) {
                        if (unit.chunk() == null) {
                            ingestSource(unit.relation(), aggregator, output);
                        } else {
                            ingestChunk(new ChunkRef(unit.chunk(), unit.relation()), aggregator, output);
                        }
                    }
                    aggregator.flush();
                    output.records = writers.recordsWritten();
                    output.files = writers.paths();
                    return output;
                }
            });
        }
        return runTasks(tasks, attributes.length);
    }

    /** A chunk of a chunked relation, or a whole unchunked relation when {@code chunk} is null. */
    private record Unit(RelationMetadata relation, Path chunk) {
        long weight() {
            // Chunks are roughly uniform; a whole relation carries its full size. Only the relative
            // magnitudes matter, for spreading the heavy units across workers.
            return chunk == null ? Math.max(1L, relation.inputSizeBytes()) : 1L;
        }
    }

    /** Greedy largest-first assignment, so one heavy unit does not share a worker with another. */
    private List<List<Unit>> balanceUnits(List<Unit> units, int workers) {
        List<Unit> sorted = new ArrayList<>(units);
        sorted.sort((a, b) -> Long.compare(b.weight(), a.weight()));

        int count = Math.max(1, Math.min(workers, sorted.size()));
        List<List<Unit>> groups = new ArrayList<>();
        long[] load = new long[count];
        for (int i = 0; i < count; i++) {
            groups.add(new ArrayList<>());
        }
        for (Unit unit : sorted) {
            int lightest = 0;
            for (int i = 1; i < count; i++) {
                if (load[i] < load[lightest]) lightest = i;
            }
            groups.get(lightest).add(unit);
            load[lightest] += unit.weight();
        }
        return groups;
    }

    /**
     * Streams one relation from its source, applying the same null rule the chunk reader would.
     *
     * The values must match what a chunk round-trip would have produced, or the result changes.
     * Line-ending rewriting already happened inside the reader; the null-string substitution is
     * the part the chunk reader did on the way back out, so it is reproduced here.
     */
    private void ingestSource(RelationMetadata relation, Aggregator aggregator, TaskOutput output) throws IOException {
        int offset = relation.offset;
        int columns = relation.columnNames.length;
        boolean nullsAreValues = config.nullHandling == Config.NullHandling.EQUALITY;

        try (RelationalInput input = relation.relationalInput) {
            while (input.hasNext()) {
                String[] row = input.next();
                int limit = Math.min(row.length, columns);
                for (int column = 0; column < limit; column++) {
                    String value = row[column];
                    int attributeId = offset + column;
                    if (value == null || (value.equals(config.nullString) && !nullsAreValues)) {
                        output.nullValues[attributeId]++;
                        continue;
                    }
                    output.totalValues[attributeId]++;
                    aggregator.add(value, attributeId);
                }
                // A short row leaves its missing columns unaccounted; the chunk path would have
                // written and re-read them as nulls, so count them the same way.
                for (int column = limit; column < columns; column++) {
                    output.nullValues[offset + column]++;
                }
            }
        }
    }

    /**
     * Distributes relations over workers largest-first, so one giant relation does not end up
     * sharing a worker with other large ones.
     */
    private List<List<RelationMetadata>> balanceBySize(RelationMetadata[] relations, int workers) {
        List<RelationMetadata> sorted = new ArrayList<>(List.of(relations));
        sorted.sort((a, b) -> Long.compare(b.inputSizeBytes(), a.inputSizeBytes()));

        int count = Math.max(1, Math.min(workers, sorted.size()));
        List<List<RelationMetadata>> groups = new ArrayList<>();
        long[] load = new long[count];
        for (int i = 0; i < count; i++) {
            groups.add(new ArrayList<>());
        }
        for (RelationMetadata relation : sorted) {
            int lightest = 0;
            for (int i = 1; i < count; i++) {
                if (load[i] < load[lightest]) lightest = i;
            }
            groups.get(lightest).add(relation);
            load[lightest] += Math.max(1L, relation.inputSizeBytes());
        }
        return groups;
    }

    /** Re-splits one oversized partition using the next slice of hash bits. */
    public Result fromPartition(List<Path> input, int level, String tag) throws InterruptedException {
        List<List<Path>> groups = split(input, config.PARALLEL);
        List<Callable<TaskOutput>> tasks = new ArrayList<>();
        for (int taskId = 0; taskId < groups.size(); taskId++) {
            int id = taskId;
            List<Path> group = groups.get(taskId);
            tasks.add(() -> {
                try (PartitionWriters writers = new PartitionWriters(config, tag, id)) {
                    Aggregator aggregator = new Aggregator(writers, level);
                    TaskOutput output = new TaskOutput(0);
                    for (Path path : group) {
                        ingestPartitionFile(path, aggregator);
                    }
                    aggregator.flush();
                    output.records = writers.recordsWritten();
                    output.files = writers.paths();
                    return output;
                }
            });
        }
        return runTasks(tasks, 0);
    }

    private Result runTasks(List<Callable<TaskOutput>> tasks, int attributeCount) throws InterruptedException {
        List<List<Path>> files = new ArrayList<>();
        for (int p = 0; p < PARTITIONS_PER_LEVEL; p++) {
            files.add(new ArrayList<>());
        }
        long[] records = new long[PARTITIONS_PER_LEVEL];
        long[] totals = new long[attributeCount];
        long[] nulls = new long[attributeCount];

        ExecutorService executors = Executors.newFixedThreadPool(config.PARALLEL);
        try {
            for (Future<TaskOutput> future : executors.invokeAll(tasks)) {
                TaskOutput output;
                try {
                    output = future.get();
                } catch (ExecutionException e) {
                    throw new IllegalStateException("partitioning failed", e.getCause());
                }
                for (int p = 0; p < PARTITIONS_PER_LEVEL; p++) {
                    if (output.files[p] != null) {
                        files.get(p).add(output.files[p]);
                    }
                    records[p] += output.records[p];
                }
                for (int a = 0; a < attributeCount; a++) {
                    totals[a] += output.totalValues[a];
                    nulls[a] += output.nullValues[a];
                }
            }
        } finally {
            executors.shutdown();
        }
        return new Result(files, records, totals, nulls);
    }

    // ----------------------------------------------------------------------------- input reading

    private void ingestChunk(ChunkRef chunk, Aggregator aggregator, TaskOutput output) throws IOException {
        // Same reader the sorter and the in-memory validator use, so values are byte-identical
        // across all three physical strategies.
        SortJob job = new SortJob(chunk.path, chunk.relation.connectedAttributes, chunk.relation.id,
                config.SORT_SIZE, config.CHUNK_SIZE, config, null, 1);
        try (CsvRelationalInput input = new CsvRelationalInput(job, config)) {
            while (input.hasNext()) {
                input.updateAttributeCombinations(null, 1);
                for (Attribute local : input.attributes) {
                    String value = local.getCurrentValue();
                    if (value == null) {
                        output.nullValues[local.getId()]++;
                        continue;
                    }
                    output.totalValues[local.getId()]++;
                    aggregator.add(value, local.getId());
                }
            }
        }
    }

    private void ingestPartitionFile(Path path, Aggregator aggregator) throws IOException {
        if (config.useBinaryPartitions) {
            // The consumer is stateful across callbacks: begin() selects the entry, attribute()
            // fills it. Records arrive whole, so there is no partial state between them.
            PartitionCodec.read(path, new PartitionCodec.RecordConsumer() {
                private ValueEntry current;

                @Override
                public void begin(String value, int attributeCount) {
                    current = aggregator.entryFor(value);
                }

                @Override
                public void attribute(int attributeId, long occurrences) {
                    aggregator.countAdded(current.add(attributeId, occurrences));
                }
            });
            aggregator.flushIfFull();
        } else {
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                String value;
                while ((value = reader.readLine()) != null) {
                    String serialized = reader.readLine();
                    if (serialized == null) break;
                    aggregator.addSerialized(value, serialized);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    // ------------------------------------------------------------------------------- aggregation

    /** Buffers values in memory and flushes them into their partitions once the budget is spent. */
    private final class Aggregator {

        private final PartitionWriters writers;
        private final int level;
        private final int budget;
        private HashMap<String, ValueEntry> values = new HashMap<>();
        private int entries;

        Aggregator(PartitionWriters writers, int level) {
            this.writers = writers;
            this.level = level;
            this.budget = Math.max(1_000, config.SORT_SIZE / config.PARALLEL);
        }

        void add(String value, int attributeId) throws IOException {
            ValueEntry entry = values.computeIfAbsent(value, v -> new ValueEntry());
            if (entry.add(attributeId) && ++entries > budget) {
                flush();
            }
        }

        /** Entry for a value, created if absent. Used by the binary reader, which fills it directly. */
        ValueEntry entryFor(String value) {
            return values.computeIfAbsent(value, v -> new ValueEntry());
        }

        /** Records that an attribute was newly added, so the flush budget stays accurate. */
        void countAdded(boolean added) {
            if (added) {
                entries++;
            }
        }

        void flushIfFull() throws IOException {
            if (entries > budget) {
                flush();
            }
        }

        void addSerialized(String value, String serialized) throws IOException {
            ValueEntry entry = values.computeIfAbsent(value, v -> new ValueEntry());
            int index = 0;
            int length = serialized.length();
            while (index < length) {
                int comma = serialized.indexOf(',', index);
                if (comma < 0) break;
                int end = serialized.indexOf(';', comma + 1);
                if (end < 0) end = length;
                int id = Integer.parseInt(serialized, index, comma, 10);
                long count = Long.parseLong(serialized, comma + 1, end, 10);
                if (entry.add(id, count)) {
                    entries++;
                }
                index = end + 1;
            }
            if (entries > budget) {
                flush();
            }
        }

        void flush() throws IOException {
            for (Map.Entry<String, ValueEntry> entry : values.entrySet()) {
                writers.write(partitionOf(entry.getKey(), level), entry.getKey(), entry.getValue());
            }
            values = new HashMap<>();
            entries = 0;
        }
    }

    /** One output file per partition, owned by a single task so no locking is needed. */
    private static final class PartitionWriters implements AutoCloseable {

        private final BufferedWriter[] textWriters = new BufferedWriter[PARTITIONS_PER_LEVEL];
        private final PartitionCodec.Writer[] binaryWriters = new PartitionCodec.Writer[PARTITIONS_PER_LEVEL];
        private final Path[] paths = new Path[PARTITIONS_PER_LEVEL];
        private final long[] records = new long[PARTITIONS_PER_LEVEL];
        private final Config config;
        private final String tag;
        private final int taskId;

        PartitionWriters(Config config, String tag, int taskId) {
            this.config = config;
            this.tag = tag;
            this.taskId = taskId;
        }

        void write(int partition, String value, ValueEntry entry) throws IOException {
            if (paths[partition] == null) {
                paths[partition] = Path.of(config.tempFolder + File.separator
                        + "p_" + tag + "_" + partition + "_t" + taskId + (config.useBinaryPartitions ? ".bin" : ".txt"));
                if (config.useBinaryPartitions) {
                    binaryWriters[partition] = PartitionCodec.writer(paths[partition]);
                } else {
                    textWriters[partition] = Files.newBufferedWriter(paths[partition],
                            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                }
            }
            if (config.useBinaryPartitions) {
                binaryWriters[partition].write(value, entry);
            } else {
                BufferedWriter writer = textWriters[partition];
                writer.write(value);
                writer.newLine();
                for (int i = 0; i < entry.size; i++) {
                    writer.write(Integer.toString(entry.ids[i]));
                    writer.write(',');
                    writer.write(Long.toString(entry.occurrences[i]));
                    writer.write(';');
                }
                writer.newLine();
            }
            records[partition]++;
        }

        Path[] paths() {
            return paths;
        }

        long[] recordsWritten() {
            return records;
        }

        @Override
        public void close() throws IOException {
            for (BufferedWriter writer : textWriters) {
                if (writer != null) writer.close();
            }
            for (PartitionCodec.Writer writer : binaryWriters) {
                if (writer != null) writer.close();
            }
        }
    }

    // ------------------------------------------------------------------------------------ plumbing

    private record ChunkRef(Path path, RelationMetadata relation) {
    }

    private static final class TaskOutput {
        final long[] totalValues;
        final long[] nullValues;
        Path[] files;
        long[] records;

        TaskOutput(int attributeCount) {
            this.totalValues = new long[attributeCount];
            this.nullValues = new long[attributeCount];
        }
    }

    private List<List<ChunkRef>> groupChunks(RelationMetadata[] relations) {
        List<ChunkRef> all = new ArrayList<>();
        for (RelationMetadata relation : relations) {
            for (Path chunk : relation.chunks) {
                all.add(new ChunkRef(chunk, relation));
            }
        }
        List<List<ChunkRef>> groups = new ArrayList<>();
        int workers = Math.max(1, Math.min(config.PARALLEL, all.size()));
        for (int i = 0; i < workers; i++) {
            groups.add(new ArrayList<>());
        }
        // Round-robin rather than contiguous blocks: chunk sizes vary widely between relations and
        // a task owning only the tail of a huge table would hold everyone else up.
        for (int i = 0; i < all.size(); i++) {
            groups.get(i % workers).add(all.get(i));
        }
        return groups;
    }

    private static <T> List<List<T>> split(List<T> items, int parts) {
        List<List<T>> groups = new ArrayList<>();
        int workers = Math.max(1, Math.min(parts, items.size()));
        for (int i = 0; i < workers; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < items.size(); i++) {
            groups.get(i % workers).add(items.get(i));
        }
        return groups;
    }

    /** Removes every partition file the run created, including any left by an abandoned level. */
    public static void cleanup(List<List<Path>> partitions) {
        for (List<Path> files : partitions) {
            for (Path path : files) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    logger.warn("could not delete partition file {}", path);
                }
            }
        }
    }
}
