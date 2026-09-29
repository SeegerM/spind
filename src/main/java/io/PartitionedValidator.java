package io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.CandidateCoverage;
import structures.Candidates;
import structures.Metadata;
import structures.PINDList;
import structures.RelationMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * Third physical strategy for unary discovery: hash-partition the value stream, then run the
 * inverted-index validator on one partition at a time.
 *
 * <p>The existing strategies sit at two extremes. Sort-merge streams everything through a global
 * order so equal values meet; the in-memory validator skips that entirely but needs the whole value
 * index resident. This one keeps the index approach while bounding its memory: hashing preserves
 * exactly the property the sort was there to provide — equal canonical values co-locate — so each
 * partition is independently validatable and nothing has to be merged afterwards.</p>
 *
 * <p>Coverage accumulates across partitions in a {@link CandidateCoverage}, one counter per
 * surviving candidate rather than per attribute pair. Once every partition has been seen,
 * {@code violations = total(A) - coverage(A,B)} exactly as in the whole-dataset case.</p>
 *
 * <p>Partitions that still do not fit are re-split with further hash bits rather than guessing a
 * large fan-out up front, which keeps the file count low on well-behaved data and still copes with
 * skew. Note that partitions are bounded by <em>distinct</em> values: entries are aggregated on
 * write, so a value occurring ten million times is a single record.</p>
 *
 * <p>Restricted to unary, duplicate-aware runs. Unary for the same Bloom-filter reason as the
 * whole-dataset validator; duplicate-aware because the violation budget must be known before the
 * first partition is processed for the remaining-mass bound to be usable, and the duplicate-unaware
 * budget depends on a distinct count that is only complete once every partition has been read.</p>
 */
public final class PartitionedValidator {

    private static final Logger logger = LoggerFactory.getLogger(PartitionedValidator.class);

    private final Config config;
    private final Partitioner partitioner;

    private CandidateCoverage coverage;
    private Attribute[] attributes;
    private long[] processedOccurrences;
    private long[] requiredCoverage;
    private int partitionsProcessed;
    private int maxDepth;
    private long largestPartition;
    private long removedByBound;
    // Split so the write half (read chunks, aggregate, spill to partitions) can be told apart from
    // the read half (load a partition, index it, accumulate coverage). They have different fixes.
    private long writeMillis;
    private long loadMillis;
    private long accumulateMillis;

    public PartitionedValidator(Config config) {
        this.config = config;
        this.partitioner = new Partitioner(config);
    }

    public static boolean isApplicable(Config config) {
        if (!config.usePartitionedValidation) {
            return false;
        }
        if (config.maxNary != 1) {
            logger.info("Partitioned validation skipped: unary only (maxNary={})", config.maxNary);
            return false;
        }
        if (config.duplicateHandling != Config.DuplicateHandling.AWARE) {
            logger.info("Partitioned validation skipped: requires duplicate-aware semantics");
            return false;
        }
        return true;
    }

    public void validate(RelationMetadata[] relations, Attribute[] attributes, Candidates candidates)
            throws InterruptedException {

        this.attributes = attributes;

        long writeStart = System.currentTimeMillis();
        Partitioner.Result level0 = config.usePartitionFromSource
                ? partitioner.fromSources(relations, attributes, "l0")
                : partitioner.fromChunks(relations, attributes, "l0");
        writeMillis += System.currentTimeMillis() - writeStart;

        // Occurrence counts come out of the partition-write pass exactly, so the violation budget
        // is known before any partition is validated — which is what makes the remaining-mass
        // bound usable rather than merely correct.
        for (int id = 0; id < attributes.length; id++) {
            Metadata metadata = attributes[id].getMetadata();
            metadata.totalValues = level0.totalValues()[id];
            metadata.nullEntries = level0.nullValues()[id];
        }

        requiredCoverage = new long[attributes.length];
        for (int id = 0; id < attributes.length; id++) {
            PINDList referenced = attributes[id].getReferenced();
            if (referenced == null) continue;
            long total = attributes[id].getMetadata().totalValues;
            long cap = (long) ((1.0 - config.threshold) * total);
            referenced.setViolationBudget(cap, total);
            // violations = total - coverage <= cap  <=>  coverage >= total - cap
            requiredCoverage[id] = total - cap;
        }

        candidates.pruneNull(attributes);

        this.coverage = new CandidateCoverage(attributes);
        this.processedOccurrences = new long[attributes.length];

        List<List<Path>> partitions = level0.files();
        try {
            for (int p = 0; p < partitions.size(); p++) {
                processPartition(partitions.get(p), 0, level0.recordsWritten()[p]);
            }
        } finally {
            Partitioner.cleanup(partitions);
        }

        for (int id = 0; id < attributes.length; id++) {
            coverage.applyTo(id, attributes, attributes[id].getMetadata().totalValues);
        }

        logger.info("Partitioned validation: {} partitions, max depth {}, largest {} records, "
                        + "{} candidates removed early by the remaining-mass bound",
                partitionsProcessed, maxDepth, largestPartition, removedByBound);
    }

    /** Records for the benchmark: partitions processed, deepest recursion, biggest partition, early kills. */
    public int partitionsProcessed() {
        return partitionsProcessed;
    }

    public int maxDepth() {
        return maxDepth;
    }

    public long largestPartition() {
        return largestPartition;
    }

    public long removedByBound() {
        return removedByBound;
    }

    public long writeMillis() {
        return writeMillis;
    }

    public long loadMillis() {
        return loadMillis;
    }

    public long accumulateMillis() {
        return accumulateMillis;
    }

    private void processPartition(List<Path> files, int level, long records) throws InterruptedException {
        if (files.isEmpty()) {
            return;
        }
        // The record count is an upper bound on the partition's distinct values (the same value can
        // arrive from several writer tasks), so deciding on it never under-estimates the load.
        if (records > config.partitionMaxRecords && Partitioner.canSplitFurther(level)) {
            long splitStart = System.currentTimeMillis();
            Partitioner.Result split = partitioner.fromPartition(files, level + 1, "l" + (level + 1));
            writeMillis += System.currentTimeMillis() - splitStart;
            List<List<Path>> sub = split.files();
            try {
                for (int p = 0; p < sub.size(); p++) {
                    processPartition(sub.get(p), level + 1, split.recordsWritten()[p]);
                }
            } finally {
                Partitioner.cleanup(sub);
            }
            return;
        }

        maxDepth = Math.max(maxDepth, level);
        largestPartition = Math.max(largestPartition, records);
        partitionsProcessed++;

        long loadStart = System.currentTimeMillis();
        Index index = load(files);
        loadMillis += System.currentTimeMillis() - loadStart;

        long accumulateStart = System.currentTimeMillis();
        accumulate(index);
        applyRemainingMassBound();
        accumulateMillis += System.currentTimeMillis() - accumulateStart;

        for (Path path : files) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                logger.warn("could not delete partition file {}", path);
            }
        }
    }

    // -------------------------------------------------------------------------------- index load

    /** Value entries of one partition, plus the per-attribute distinct counts needed to size the transpose. */
    private static final class Index {
        final Map<String, ValueEntry> byValue = new HashMap<>();
        final long[] distinctPerAttribute;

        Index(int attributeCount) {
            this.distinctPerAttribute = new long[attributeCount];
        }
    }

    private Index load(List<Path> files) {
        Index index = new Index(attributes.length);
        Map<String, ValueEntry> byValue = new ConcurrentHashMap<>();

        files.parallelStream().forEach(path -> {
            try {
                if (config.useBinaryPartitions) {
                    readBinary(path, byValue);
                } else {
                    readText(path, byValue);
                }
            } catch (IOException e) {
                throw new IllegalStateException("cannot read partition file " + path, e);
            }
        });

        index.byValue.putAll(byValue);
        // Counted after the parallel load rather than during it: incrementing a shared array per
        // parsed pair would have been the one real contention point in the read path.
        for (ValueEntry entry : index.byValue.values()) {
            for (int i = 0; i < entry.size; i++) {
                index.distinctPerAttribute[entry.ids[i]]++;
            }
        }
        return index;
    }

    /**
     * Reads a binary partition file into the shared map.
     *
     * Each record is staged in a reusable buffer and merged once complete, rather than written
     * into the map as it is parsed: holding the map's bin lock across stream reads would serialize
     * the parallel readers on I/O, which is the whole point of reading the files in parallel.
     */
    private static void readBinary(Path path, Map<String, ValueEntry> byValue) throws IOException {
        BinaryLoader loader = new BinaryLoader(byValue);
        PartitionCodec.read(path, loader);
        loader.finish();
    }

    private static final class BinaryLoader implements PartitionCodec.RecordConsumer {

        private final Map<String, ValueEntry> byValue;
        private final ValueEntry staging = new ValueEntry();
        private String pending;

        BinaryLoader(Map<String, ValueEntry> byValue) {
            this.byValue = byValue;
        }

        @Override
        public void begin(String value, int attributeCount) {
            merge();
            pending = value;
            staging.size = 0;
        }

        @Override
        public void attribute(int attributeId, long occurrences) {
            staging.add(attributeId, occurrences);
        }

        /** Merges the final record; the others are merged when the next one begins. */
        void finish() {
            merge();
            pending = null;
        }

        private void merge() {
            if (pending == null) {
                return;
            }
            byValue.compute(pending, (key, entry) -> {
                ValueEntry target = entry == null ? new ValueEntry() : entry;
                for (int i = 0; i < staging.size; i++) {
                    target.add(staging.ids[i], staging.occurrences[i]);
                }
                return target;
            });
        }
    }

    private static void readText(Path path, Map<String, ValueEntry> byValue) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String value;
            while ((value = reader.readLine()) != null) {
                String serialized = reader.readLine();
                if (serialized == null) break;
                // compute() locks the bin, which serializes concurrent writers for one value
                // while leaving distinct values fully parallel.
                byValue.compute(value, (key, entry) -> {
                    ValueEntry target = entry == null ? new ValueEntry() : entry;
                    parseInto(target, serialized);
                    return target;
                });
            }
        }
    }

    private static void parseInto(ValueEntry entry, String serialized) {
        int index = 0;
        int length = serialized.length();
        while (index < length) {
            int comma = serialized.indexOf(',', index);
            if (comma < 0) break;
            int end = serialized.indexOf(';', comma + 1);
            if (end < 0) end = length;
            int id = Integer.parseInt(serialized, index, comma, 10);
            long count = Long.parseLong(serialized, comma + 1, end, 10);
            entry.add(id, count);
            index = end + 1;
        }
    }

    // ------------------------------------------------------------------------------ accumulation

    /**
     * Adds this partition's contribution to every surviving candidate's coverage.
     *
     * Mirrors {@link InMemoryValidator}'s inner loop; the difference is that the per-dependent
     * scratch is folded into persistent per-candidate counters instead of being consumed on the
     * spot, because the rest of the dependent's values live in other partitions.
     */
    private void accumulate(Index index) {
        int attributeCount = attributes.length;
        int valueCount = index.byValue.size();

        int[][] valueToAttributes = new int[valueCount][];
        int[][] attributeToValues = new int[attributeCount][];
        long[][] attributeToOccurrences = new long[attributeCount][];
        int[] cursor = new int[attributeCount];
        for (int id = 0; id < attributeCount; id++) {
            int distinct = (int) index.distinctPerAttribute[id];
            attributeToValues[id] = new int[distinct];
            attributeToOccurrences[id] = new long[distinct];
        }

        int valueId = 0;
        for (ValueEntry entry : index.byValue.values()) {
            valueToAttributes[valueId] = entry.trimmedIds();
            for (int i = 0; i < entry.size; i++) {
                int attributeId = entry.ids[i];
                int position = cursor[attributeId]++;
                attributeToValues[attributeId][position] = valueId;
                attributeToOccurrences[attributeId][position] = entry.occurrences[i];
            }
            valueId++;
        }
        index.byValue.clear();

        int[][] finalValueToAttributes = valueToAttributes;
        IntStream.range(0, attributeCount).parallel().forEach(dependantId -> {
            int[] values = attributeToValues[dependantId];
            long[] occurrences = attributeToOccurrences[dependantId];
            if (values.length == 0) {
                return;
            }

            long seen = 0;
            for (long occurrence : occurrences) {
                seen += occurrence;
            }
            processedOccurrences[dependantId] += seen;

            if (!coverage.hasCandidates(dependantId)) {
                return;
            }

            long[] scratch = Scratch.LOCAL.get().forSize(attributeCount);
            int[] touched = new int[Math.min(attributeCount, 64)];
            int touchedCount = 0;
            boolean trackTouched = true;

            for (int i = 0; i < values.length; i++) {
                long weight = occurrences[i];
                for (int otherId : finalValueToAttributes[values[i]]) {
                    if (otherId == dependantId) continue;
                    if (scratch[otherId] == 0L && trackTouched) {
                        if (touchedCount == touched.length) {
                            if (touched.length * 2 > attributeCount) {
                                trackTouched = false;
                            } else {
                                touched = Arrays.copyOf(touched, touched.length * 2);
                                touched[touchedCount++] = otherId;
                            }
                        } else {
                            touched[touchedCount++] = otherId;
                        }
                    }
                    scratch[otherId] += weight;
                }
            }

            int slots = coverage.slots(dependantId);
            for (int slot = 0; slot < slots; slot++) {
                int referencedId = coverage.referencedId(dependantId, slot);
                if (referencedId >= 0 && scratch[referencedId] != 0L) {
                    coverage.addCoverage(dependantId, slot, scratch[referencedId]);
                }
            }

            if (trackTouched) {
                for (int i = 0; i < touchedCount; i++) {
                    scratch[touched[i]] = 0L;
                }
            } else {
                Arrays.fill(scratch, 0L);
            }
        });
    }

    /**
     * Drops candidates that can no longer reach the coverage their budget demands.
     *
     * Everything still unprocessed for dependent {@code A} is worth at most {@code remaining(A)}
     * further coverage, so a candidate already short by more than that is impossible — exactly, with
     * no sketching and no false negatives. The bound is per dependent, not per candidate, so it
     * costs one subtraction per dependent plus a walk of its surviving slots.
     */
    private void applyRemainingMassBound() {
        long[] removed = {0};
        IntStream.range(0, attributes.length).parallel().forEach(dependantId -> {
            if (!coverage.hasCandidates(dependantId)) {
                return;
            }
            long remaining = attributes[dependantId].getMetadata().totalValues - processedOccurrences[dependantId];
            long required = requiredCoverage[dependantId];
            int slots = coverage.slots(dependantId);
            long localRemoved = 0;
            for (int slot = 0; slot < slots; slot++) {
                if (coverage.referencedId(dependantId, slot) < 0) {
                    continue;
                }
                if (coverage.covered(dependantId, slot) + remaining < required) {
                    coverage.remove(dependantId, slot);
                    localRemoved++;
                }
            }
            if (localRemoved > 0) {
                synchronized (removed) {
                    removed[0] += localRemoved;
                }
            }
        });
        removedByBound += removed[0];
    }

    /** Per-thread coverage scratch, reused across dependents and partitions. */
    private static final class Scratch {
        static final ThreadLocal<Scratch> LOCAL = ThreadLocal.withInitial(Scratch::new);
        private long[] buffer = new long[0];

        long[] forSize(int size) {
            if (buffer.length < size) {
                buffer = new long[size];
            }
            return buffer;
        }
    }
}
