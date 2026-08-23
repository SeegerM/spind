package io;

import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.Candidates;
import structures.Metadata;
import structures.PINDList;
import structures.RelationMetadata;
import structures.SortJob;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Unary validator that replaces the sort / merge / stream-validate pipeline with an in-memory
 * value-to-attribute inverted index, in the style of De Marchi's all-column approach.
 *
 * <p>The motivation is not mainly avoiding disk. The streaming validator charges violations
 * pairwise: for every value group it walks each dependent attribute's <em>entire</em> candidate
 * list and debits every referenced attribute absent from the group. Its cost therefore scales with
 * the length of the candidate lists, which start at roughly the attribute count. Working from an
 * index inverts the accounting — for a dependent attribute {@code A},</p>
 *
 * <pre>
 *   coverage(A, B) = sum of occ_A(v) over values v that A and B share
 *   violations(A, B) = total(A) - coverage(A, B)
 * </pre>
 *
 * <p>so only attributes that actually <em>share</em> a value are ever touched. The cost becomes
 * {@code sum over v of |S(v)|^2} — where {@code S(v)} is the set of attributes containing
 * {@code v} — instead of {@code sum over v of |S(v)| * candidateListLength}. When values are
 * spread thinly over many attributes, which is exactly the many-small-relations case, that is a
 * large asymptotic difference.</p>
 *
 * <p>The result is identical, not merely equivalent: a surviving candidate accumulates violations
 * from every value group either way, so the totals agree, and the pruning decision is a comparison
 * against the same budget. Candidates the streaming validator drops early do end up with different
 * (partial) violation counts, but those are removed and never reported.</p>
 *
 * <p><b>Restricted to unary discovery.</b> At layer 1 the streaming validator also seeds the Bloom
 * filter used to mask non-informative values in higher layers, and it does so per value group in
 * sorted order interleaved with pruning. Reproducing those exact contents from the index is
 * possible but not obviously worth the risk, so the caller falls back to the standard pipeline
 * whenever {@code maxNary != 1}.</p>
 */
public final class InMemoryValidator {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryValidator.class);

    private final Config config;

    public InMemoryValidator(Config config) {
        this.config = config;
    }

    /**
     * Decides whether this validator may run at all.
     *
     * The size test is on raw input bytes, which under-states the in-memory footprint (a Java
     * String costs far more than its characters). It is a cheap first gate only; the build itself
     * aborts on a distinct-value ceiling, which is the bound that actually protects the heap.
     */
    public static boolean isApplicable(Config config, RelationMetadata[] relations) {
        if (!config.useInMemoryValidation) {
            return false;
        }
        if (config.maxNary != 1) {
            logger.info("In-memory validation skipped: only unary discovery is supported (maxNary={})", config.maxNary);
            return false;
        }
        long bytes = 0;
        for (RelationMetadata relation : relations) {
            bytes += relation.inputSizeBytes();
        }
        if (bytes > config.inMemoryValidationLimitBytes) {
            logger.info("In-memory validation skipped: input is {} MB, limit is {} MB",
                    bytes / (1024 * 1024), config.inMemoryValidationLimitBytes / (1024 * 1024));
            return false;
        }
        return true;
    }

    /**
     * Builds the index and prunes every unary candidate against it.
     *
     * @return {@code true} if validation completed, {@code false} if it gave up on the
     * distinct-value ceiling. On {@code false} nothing is left behind: attribute metadata is reset
     * and the chunk files are untouched, so the caller can run the standard pipeline instead.
     */
    public boolean validate(RelationMetadata[] relations, Attribute[] attributes, Candidates candidates)
            throws InterruptedException {

        Map<String, ValueEntry> index = new ConcurrentHashMap<>();
        AtomicBoolean aborted = new AtomicBoolean(false);

        buildIndex(relations, attributes, index, aborted);

        if (aborted.get()) {
            logger.info("In-memory validation aborted: more than {} distinct values; falling back to sort-merge",
                    config.inMemoryMaxDistinctValues);
            for (Attribute attribute : attributes) {
                attribute.setMetadata(new Metadata());
            }
            return false;
        }

        logger.info("Built in-memory index over {} distinct values", index.size());

        Postings postings = transpose(index, attributes);
        index = null; // the string keys are dead weight from here on

        candidates.calculateViolations(attributes);
        candidates.pruneNull(attributes);

        prune(attributes, candidates, postings);
        return true;
    }

    // --------------------------------------------------------------------------- index building

    private void buildIndex(RelationMetadata[] relations, Attribute[] attributes,
                            Map<String, ValueEntry> index, AtomicBoolean aborted) throws InterruptedException {

        ExecutorService executors = Executors.newFixedThreadPool(config.PARALLEL);
        List<Runnable> ignored = new ArrayList<>();
        try {
            List<java.util.concurrent.Callable<Void>> jobs = new ArrayList<>();
            for (RelationMetadata relation : relations) {
                jobs.add(() -> {
                    // Attributes belong to exactly one relation, so the metadata counters touched
                    // here are private to this task even though the index itself is shared.
                    for (Path chunkPath : relation.chunks) {
                        if (aborted.get()) return null;
                        ingestChunk(chunkPath, relation, attributes, index, aborted);
                    }
                    return null;
                });
            }
            for (java.util.concurrent.Future<Void> future : executors.invokeAll(jobs)) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new IllegalStateException("in-memory index construction failed", e.getCause());
                }
            }
        } finally {
            executors.shutdown();
            ignored.clear();
        }
    }

    private void ingestChunk(Path chunkPath, RelationMetadata relation, Attribute[] attributes,
                             Map<String, ValueEntry> index, AtomicBoolean aborted) {

        // Reading through the same reader the Sorter uses guarantees identical values: same CSV
        // dialect, same null-string handling, same n-ary combination logic.
        SortJob job = new SortJob(chunkPath, relation.connectedAttributes, relation.id,
                config.SORT_SIZE, config.CHUNK_SIZE, config, null, 1);

        try (CsvRelationalInput input = new CsvRelationalInput(job, config)) {
            while (input.hasNext()) {
                input.updateAttributeCombinations(null, 1);
                for (Attribute local : input.attributes) {
                    String value = local.getCurrentValue();
                    Metadata metadata = attributes[local.getId()].getMetadata();
                    if (value == null) {
                        metadata.nullEntries++;
                        continue;
                    }
                    metadata.totalValues++;

                    int attributeId = local.getId();
                    boolean[] isNewForAttribute = {false};
                    index.compute(value, (key, entry) -> {
                        if (entry == null) {
                            entry = new ValueEntry();
                        }
                        isNewForAttribute[0] = entry.add(attributeId);
                        return entry;
                    });
                    if (isNewForAttribute[0]) {
                        metadata.uniqueValues++;
                    }
                }
                if (index.size() > config.inMemoryMaxDistinctValues) {
                    aborted.set(true);
                    return;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot read chunk " + chunkPath, e);
        }
    }

    /**
     * Flips {@code value -> attributes} into {@code attribute -> values}, keeping both directions.
     *
     * Values lose their text here and become dense ids. Only the co-occurrence structure matters
     * for counting coverage, and dropping several million strings at this point is most of the
     * memory this validator would otherwise hold.
     */
    private Postings transpose(Map<String, ValueEntry> index, Attribute[] attributes) {
        int valueCount = index.size();
        int[][] valueToAttributes = new int[valueCount][];

        int[] cursor = new int[attributes.length];
        for (int attributeId = 0; attributeId < attributes.length; attributeId++) {
            cursor[attributeId] = (int) attributes[attributeId].getMetadata().uniqueValues;
        }
        int[][] attributeToValues = new int[attributes.length][];
        long[][] attributeToOccurrences = new long[attributes.length][];
        for (int attributeId = 0; attributeId < attributes.length; attributeId++) {
            attributeToValues[attributeId] = new int[cursor[attributeId]];
            attributeToOccurrences[attributeId] = new long[cursor[attributeId]];
            cursor[attributeId] = 0;
        }

        int valueId = 0;
        for (ValueEntry entry : index.values()) {
            valueToAttributes[valueId] = entry.trimmedIds();
            for (int i = 0; i < entry.size; i++) {
                int attributeId = entry.ids[i];
                int position = cursor[attributeId]++;
                attributeToValues[attributeId][position] = valueId;
                attributeToOccurrences[attributeId][position] = entry.occurrences[i];
            }
            valueId++;
        }
        return new Postings(valueToAttributes, attributeToValues, attributeToOccurrences);
    }

    // ---------------------------------------------------------------------------------- pruning

    /**
     * For each dependent attribute, accumulates how much of it each other attribute covers, then
     * keeps exactly the candidates whose resulting violation count fits the budget.
     */
    private void prune(Attribute[] attributes, Candidates candidates, Postings postings) {
        boolean duplicateAware = config.duplicateHandling == Config.DuplicateHandling.AWARE;

        IntStream.range(0, attributes.length).parallel().forEach(dependantId -> {
            PINDList referencedList = candidates.current[dependantId].getReferenced();
            if (referencedList == null || referencedList.isEmpty()) {
                return;
            }

            long[] coverage = CoverageScratch.SCRATCH.get().forSize(attributes.length);

            int[] values = postings.attributeToValues[dependantId];
            long[] occurrences = postings.attributeToOccurrences[dependantId];
            int[] touched = new int[Math.min(attributes.length, 64)];
            int touchedCount = 0;
            boolean trackTouched = true;

            for (int i = 0; i < values.length; i++) {
                long weight = duplicateAware ? occurrences[i] : 1L;
                for (int otherId : postings.valueToAttributes[values[i]]) {
                    if (otherId == dependantId) continue;
                    if (coverage[otherId] == 0L && trackTouched) {
                        if (touchedCount == touched.length) {
                            if (touched.length * 2 > attributes.length) {
                                // More attributes touched than it is worth remembering; clearing
                                // the whole array afterwards is cheaper than growing this list.
                                trackTouched = false;
                            } else {
                                touched = java.util.Arrays.copyOf(touched, touched.length * 2);
                                touched[touchedCount++] = otherId;
                            }
                        } else {
                            touched[touchedCount++] = otherId;
                        }
                    }
                    coverage[otherId] += weight;
                }
            }

            long dependantSize = duplicateAware
                    ? attributes[dependantId].getMetadata().totalValues
                    : attributes[dependantId].getMetadata().uniqueValues;

            PINDList.PINDIterator referenced = referencedList.elementIterator();
            while (referenced.hasNext()) {
                PINDList.PINDElement element = referenced.next();
                long violations = dependantSize - coverage[element.id];
                if (violations > 0 && element.violate(violations) < 0L) {
                    referenced.remove();
                }
            }
            if (referencedList.isEmpty()) {
                candidates.current[dependantId].setReferenced(null);
            }

            if (trackTouched) {
                for (int i = 0; i < touchedCount; i++) {
                    coverage[touched[i]] = 0L;
                }
            } else {
                java.util.Arrays.fill(coverage, 0L);
            }
        });
    }

    /** Per-thread coverage accumulator, reused across dependent attributes to avoid reallocation. */
    private static final class CoverageScratch {
        static final ThreadLocal<CoverageScratch> SCRATCH = ThreadLocal.withInitial(CoverageScratch::new);
        private long[] buffer = new long[0];

        long[] forSize(int size) {
            if (buffer.length < size) {
                buffer = new long[size];
            }
            return buffer;
        }
    }

    // ------------------------------------------------------------------------------- structures

    private record Postings(int[][] valueToAttributes, int[][] attributeToValues, long[][] attributeToOccurrences) {
    }
}
