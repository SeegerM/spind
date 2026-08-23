package core;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.opencsv.exceptions.CsvValidationException;
import io.Merger;
import io.InMemoryValidator;
import io.Output;
import io.PartitionedValidator;
import io.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import similarity.*;
import structures.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @noinspection UnstableApiUsage
 */
public class Spind {
    final Config config;
    private final Metrics metrics;
    private final int maxNary;
    private final Logger logger;
    private final Output output;
    private final Clock clock;
    RelationMetadata[] relationMetadata;
    int layer;
    private BloomFilter<Integer> filter;

    public Spind(Config config) {
        this.clock = new Clock();
        clock.start("total");

        this.metrics = new Metrics();
        this.config = config;
        maxNary = config.maxNary;
        this.output = new Output(config.resultFolder);
        this.logger = LoggerFactory.getLogger(Spind.class);
        if (config.useFilter) this.filter = BloomFilter.create(Funnels.integerFunnel(), 100_000_000, 0.05);
    }

    public void execute() throws IOException, InterruptedException, CsvValidationException {

        logger.info("Starting execution");

        clock.start("init");
        this.relationMetadata = initializeRelations();

        // 1) init the helper Classes
        Candidates candidates = new Candidates(config);

        // 1) get all unary attributes.
        Attribute[] attributes = buildUnaryAttributes();

        // Similarity-based discovery bypasses the sort/merge/validate layer loop entirely
        // (lexicographic sort doesn't bring similar strings together). Unary only.
        if (config.similarityMode != Config.SimilarityMode.NONE) {
            runSimilarityDiscovery(attributes);
            cleanupTempFiles();
            output.storeMetadata(config, clock, metrics);
            return;
        }

        ColumnStats columnStats = config.collectColumnStats ? collectColumnStats(attributes.length) : null;

        clock.start("preprune");
        if (config.usePrePrune && columnStats != null) {
            metrics.prePruned = candidates.loadUnary(attributes, columnStats);
        } else {
            candidates.loadUnary(attributes);
        }
        clock.stop("preprune");

        if (config.useProgressiveSampling && columnStats != null) {
            clock.start("sampling");
            runSamplePass(attributes, candidates, columnStats);
            logger.info("Finished progressive sampling. Took: " + clock.stop("sampling") + "ms");
        }

        logger.info("Finished initialization. Took: " + clock.stop("init") + "ms");

        // 2b) Physical strategy choice for unary discovery. The whole-dataset index is fastest but
        // needs the values resident; partitioning bounds that memory while keeping the index
        // approach; sort-merge below remains the fallback that always applies.
        if (InMemoryValidator.isApplicable(config, relationMetadata) && runInMemoryValidation(attributes, candidates)) {
            cleanupTempFiles();
            output.storeMetadata(config, clock, metrics);
            return;
        }

        if (PartitionedValidator.isApplicable(config)) {
            runPartitionedValidation(attributes, candidates);
            cleanupTempFiles();
            output.storeMetadata(config, clock, metrics);
            return;
        }

        // 3) while attributes not empty.
        while (attributes.length > 0) {
            layer++;

            // create sort jobs
            attachAttributes(attributes);
            List<SortJob> sortJobs = createSortJobs();

            int numAttributes = attributes.length;
            int numCandidates = Arrays.stream(attributes).mapToInt(x -> x.getReferenced() == null ? 0 : x.getReferenced().size()).sum();

            metrics.layerAttributes.add(numAttributes);
            metrics.layerCandidates.add(numCandidates);
            logger.info("Starting layer: " + layer + " with " + numAttributes + " attributes forming " + numCandidates + " candidates");
            // candidates.current.get(x).size()).sum() + " candidates");
            // 3.1) Load all attributes of the candidates.
            clock.start("sorting");
            ExecutorService executors = Executors.newFixedThreadPool(config.PARALLEL);
            List<SortResult> sortResults = executors.invokeAll(sortJobs.stream().sorted().toList()).stream().map(sortResultFuture -> {
                try {
                    return sortResultFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    return new SortResult(null, null);
                }
            }).toList();
            executors.shutdown();

            logger.info("Finished sorting. Took: " + clock.stop("sorting") + "ms");

            metrics.sortFiles += sortResults.stream().mapToInt(sortResult -> sortResult.mergeJob().chunkPaths().size()).sum();

            long totalSaved = 0;
            for (SortResult sortResult : sortResults) {
                for (Attribute sortAttribute : sortResult.connectedAttributes()) {
                    attributes[sortAttribute.getId()].getMetadata().globalUnique += sortAttribute.getMetadata().globalUnique;
                    attributes[sortAttribute.getId()].getMetadata().nullEntries += sortAttribute.getMetadata().nullEntries;
                    totalSaved += sortAttribute.getMetadata().globalUnique;
                }
            }
            logger.info("In total " + totalSaved + " occurrences where skipped due to global uniqueness");

            List<MergeJob> mergeJobs = sortResults.stream().map(SortResult::mergeJob).toList();

            clock.start("merging");
            int activeRelations = iterativeMerge(attributes, mergeJobs);
            logger.info("Finished merging. Took: " + clock.stop("merging") + "ms");

            // 3.2) Validate candidates.
            clock.start("validation");
            Validator validator = new Validator(config, candidates, config.VALIDATION_SIZE / activeRelations);
            filter = validator.validate(layer, filter);

            // remove all dependant candidates, that do not reference any attribute
            candidates.cleanCandidates();
            logger.info("Finished validation. Took: " + clock.stop("validation") + "ms");

            int numPINDs = calcPINDs(attributes);
            metrics.layerPINDs.add(numPINDs);
            if (layer == 1) metrics.unary = numPINDs;
            else metrics.nary += numPINDs;

            logger.info("Found " + numPINDs + " pINDs at level " + layer);
            // Timed separately: on result-heavy datasets (WebTables yields ~20M unary pINDs)
            // serializing the result costs more than discovering it, which would otherwise
            // swamp any algorithmic difference between variants.
            clock.start("output");
            output.storePINDs(relationMetadata, attributes, layer, config);
            if (config.canonicalFolder != null) {
                output.storeCanonicalPINDs(relationMetadata, attributes, layer, config, config.canonicalFolder);
            }
            clock.stop("output");

            // clean relation files
            for (RelationMetadata relation : relationMetadata) {
                Path relationFile = Path.of(config.tempFolder + File.separator + "relation_" + relation.id + ".txt");
                if (Files.exists(relationFile)) Files.delete(relationFile);
            }

            if (maxNary > 0 && layer == maxNary) break;

            // 3.4) Generate new attributes for next layer.
            clock.start("generateNext");
            attributes = candidates.generateNextLayer(attributes, relationMetadata, layer);
            logger.info("Finished generating next layer. Took: " + clock.stop("generateNext") + "ms");
        }
        // clean up temp
        Arrays.stream(Objects.requireNonNull((new File(config.tempFolder)).listFiles())).forEach(File::delete);

        // 4) Save the output
        output.storeMetadata(config, clock, metrics);
    }

    private int iterativeMerge(Attribute[] attributes, List<MergeJob> mergeJobs) {
        int activeRelations = 1;
        int merge = Math.max(2, config.MERGE_SIZE / config.PARALLEL); // we need to always merge at least two files
        while (!mergeJobs.isEmpty()) {
            // 1) group by relation
            List<MergeJob> groupedMergeJobs = new ArrayList<>();
            for (int i = 0; i < relationMetadata.length; i++) {
                groupedMergeJobs.add(new MergeJob(new ArrayList<>(), i, null, false));
            }
            for (MergeJob mergeJob : mergeJobs) {
                groupedMergeJobs.get(mergeJob.relationId()).chunkPaths().addAll(mergeJob.chunkPaths());
            }

            // split jobs and save next parts
            List<MergeJob> nextJobs = new ArrayList<>();
            List<MergeJob> currentJobs = new ArrayList<>();

            for (MergeJob job : groupedMergeJobs) {
                if (job.chunkPaths().isEmpty()) {
                    continue;
                }
                int n = job.chunkPaths().size();
                if (n >= merge) {
                    // Case 1: The number of files exceeds the merge size threshold -> Merge subsets of the spilled files and re-add then to the next iteration
                    List<Path> nextPaths = new ArrayList<>();
                    for (int i = 0; i < n; i += merge) {
                        Path resultPath = Path.of(job.chunkPaths().get(i) + "_m_" + i + ".txt");
                        currentJobs.add(new MergeJob(new ArrayList<>(job.chunkPaths().subList(i, Math.min(i + merge, n))), job.relationId(), resultPath, false));
                        nextPaths.add(resultPath);
                    }
                    nextJobs.add(new MergeJob(nextPaths, job.relationId(), null, false));
                } else {
                    // Case 2: The number of files does not exceed the threshold -> the next merge finishes the relation file.
                    activeRelations++;
                    Path resultPath = Path.of(config.tempFolder + File.separator + "relation_" + job.relationId() + ".txt");
                    currentJobs.add(new MergeJob(job.chunkPaths(), job.relationId(), resultPath, true));
                }
            }

            metrics.mergeFiles += currentJobs.size();

            currentJobs.parallelStream().forEach(mergeJob -> {
                if (mergeJob.chunkPaths().isEmpty()) {
                    return;
                }
                Merger merger = new Merger();
                merger.merge(mergeJob.chunkPaths(), mergeJob.to(), attributes, mergeJob.isFinal());
            });
            mergeJobs = nextJobs;
        }
        return activeRelations;
    }

    private int calcPINDs(Attribute[] attributes) {
        int total = 0;
        for (Attribute attribute : attributes) {
            if (attribute.getReferenced() != null) {
                total += attribute.getReferenced().size();
            }
        }
        return total;
    }

    private RelationMetadata[] initializeRelations() throws IOException, CsvValidationException, InterruptedException {
        RelationMetadata[] relationMetadata = new RelationMetadata[config.tableSources.size()];

        int relationOffset = 0;
        for (int relationId = 0; relationId < config.tableSources.size(); relationId++) {
            relationMetadata[relationId] = new RelationMetadata(relationId, relationOffset,
                    config.tableSources.get(relationId), config, config.connectionRegistry());
            relationOffset += relationMetadata[relationId].columnNames.length;
        }

        clock.start("chunking");

        // Under direct-source partitioning only the relations big enough to need decomposing into
        // parallel work units still get chunked; the rest are read from source and skip the
        // round-trip entirely.
        List<RelationMetadata> toChunk = new ArrayList<>();
        if (skipChunking()) {
            long totalBytes = 0;
            for (RelationMetadata relation : relationMetadata) {
                totalBytes += relation.inputSizeBytes();
            }
            long threshold = config.chunkThresholdBytes(totalBytes);
            for (RelationMetadata relation : relationMetadata) {
                relation.chunked = relation.inputSizeBytes() >= threshold;
                if (relation.chunked) {
                    toChunk.add(relation);
                }
            }
            logger.info("Chunking " + toChunk.size() + " of " + relationMetadata.length
                    + " relations (threshold " + (threshold / (1024 * 1024))
                    + " MB); the rest are partitioned directly from source");
        } else {
            toChunk.addAll(List.of(relationMetadata));
            logger.info("Stating chunking");
        }

        if (toChunk.isEmpty()) {
            clock.stop("chunking");
            return relationMetadata;
        }

        ExecutorService executors = Executors.newFixedThreadPool(config.PARALLEL);
        executors.invokeAll(toChunk.stream().sorted().toList()).forEach(relation -> {

            try {
                relation.get();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        executors.shutdown();

        logger.info("Finished chunking. Took: " + clock.stop("chunking"));

        this.metrics.chunkFiles = Arrays.stream(relationMetadata).mapToInt(metadata -> metadata.chunks.size()).sum();

        return relationMetadata;
    }

    private Attribute[] buildUnaryAttributes() {
        Attribute[] attributes = new Attribute[Arrays.stream(relationMetadata).mapToInt(x -> x.columnNames.length).sum()];
        for (RelationMetadata input : relationMetadata) {
            int relationOffset = input.offset;
            for (int i = 0; i < input.columnNames.length; i++) {
                attributes[relationOffset + i] = new Attribute(relationOffset + i, input.id, new int[]{i});
            }
        }
        return attributes;
    }

    private void attachAttributes(Attribute[] attributes) {
        // reset connectedAttributes
        for (RelationMetadata relation : relationMetadata) {
            relation.connectedAttributes = new ArrayList<>();
        }
        for (Attribute attribute : attributes) {
            relationMetadata[attribute.getRelationId()].connectedAttributes.add(attribute);
        }
        for (RelationMetadata relation : relationMetadata) {
            relation.connectedAttributes = Collections.unmodifiableList(relation.connectedAttributes);
        }
    }

    private List<SortJob> createSortJobs() {
        List<SortJob> jobs = new ArrayList<>();

        for (RelationMetadata relation : relationMetadata) {
            if (relation.connectedAttributes.isEmpty()) {
                continue;
            }
            for (Path chunkPath : relation.chunks) {
                jobs.add(new SortJob(chunkPath, relation.connectedAttributes, relation.id, config.SORT_SIZE / config.PARALLEL, config.CHUNK_SIZE, config, filter, layer));
            }
        }

        return jobs;
    }

    /**
     * Runs unary discovery entirely in memory and writes the results.
     *
     * @return true if it completed; false if the index hit its ceiling, in which case no state has
     * been kept and the caller must run the standard pipeline.
     */
    private boolean runInMemoryValidation(Attribute[] attributes, Candidates candidates)
            throws IOException, InterruptedException {

        layer = 1;
        attachAttributes(attributes);

        int numCandidates = calcPINDs(attributes);
        metrics.layerAttributes.add(attributes.length);
        metrics.layerCandidates.add(numCandidates);
        logger.info("Starting in-memory layer 1 with " + attributes.length + " attributes forming " + numCandidates + " candidates");

        clock.start("inmemory");
        boolean completed = new InMemoryValidator(config).validate(relationMetadata, attributes, candidates);
        long elapsed = clock.stop("inmemory");

        if (!completed) {
            // Metrics recorded above describe an attempt that produced nothing; drop them so the
            // standard pipeline's own layer-1 entry is not duplicated.
            metrics.layerAttributes.clear();
            metrics.layerCandidates.clear();
            layer = 0;
            return false;
        }

        candidates.cleanCandidates();
        logger.info("Finished in-memory validation. Took: " + elapsed + "ms");

        int numPINDs = calcPINDs(attributes);
        metrics.layerPINDs.add(numPINDs);
        metrics.unary = numPINDs;
        logger.info("Found " + numPINDs + " pINDs at level 1");

        clock.start("output");
        output.storePINDs(relationMetadata, attributes, 1, config);
        if (config.canonicalFolder != null) {
            output.storeCanonicalPINDs(relationMetadata, attributes, 1, config, config.canonicalFolder);
        }
        clock.stop("output");
        return true;
    }

    /**
     * Whether chunk files can be skipped entirely.
     *
     * Only when the partitioned validator will run and will read sources itself. Column statistics
     * are gathered during chunking, so requesting both is contradictory — chunking wins, since
     * dropping the statistics would silently disable pre-pruning instead.
     */
    private boolean skipChunking() {
        if (!config.usePartitionFromSource || !PartitionedValidator.isApplicable(config)) {
            return false;
        }
        if (config.collectColumnStats) {
            logger.info("Not skipping chunking: column statistics are collected during it");
            return false;
        }
        return true;
    }

    /** Runs unary discovery over hash partitions and writes the results. */
    private void runPartitionedValidation(Attribute[] attributes, Candidates candidates)
            throws IOException, InterruptedException {

        layer = 1;
        attachAttributes(attributes);

        int numCandidates = calcPINDs(attributes);
        metrics.layerAttributes.add(attributes.length);
        metrics.layerCandidates.add(numCandidates);
        logger.info("Starting partitioned layer 1 with " + attributes.length + " attributes forming " + numCandidates + " candidates");

        clock.start("partitioned");
        PartitionedValidator validator = new PartitionedValidator(config);
        validator.validate(relationMetadata, attributes, candidates);
        long elapsed = clock.stop("partitioned");

        metrics.partitions = validator.partitionsProcessed();
        metrics.partitionDepth = validator.maxDepth();
        metrics.largestPartition = validator.largestPartition();
        metrics.removedByBound = validator.removedByBound();
        metrics.partitionWriteMillis = validator.writeMillis();
        metrics.partitionLoadMillis = validator.loadMillis();
        metrics.partitionAccumulateMillis = validator.accumulateMillis();

        candidates.cleanCandidates();
        logger.info("Finished partitioned validation. Took: " + elapsed + "ms");

        int numPINDs = calcPINDs(attributes);
        metrics.layerPINDs.add(numPINDs);
        metrics.unary = numPINDs;
        logger.info("Found " + numPINDs + " pINDs at level 1");

        clock.start("output");
        output.storePINDs(relationMetadata, attributes, 1, config);
        if (config.canonicalFolder != null) {
            output.storeCanonicalPINDs(relationMetadata, attributes, 1, config, config.canonicalFolder);
        }
        clock.stop("output");
    }

    /** Gathers the per-relation counters produced during chunking into one attribute-indexed view. */
    private ColumnStats collectColumnStats(int attributeCount) {
        ColumnStats stats = new ColumnStats(attributeCount);
        for (RelationMetadata relation : relationMetadata) {
            if (relation.columnTotalValues == null) continue;
            stats.ingest(relation.offset, relation.columnTotalValues, relation.columnNullValues,
                    relation.columnDistinctValues, relation.columnNumeric, relation.columnHasValues);
        }
        return stats;
    }

    /**
     * Runs one full sort/merge/validate cycle over a fraction of each relation's chunks, purely to
     * eliminate candidates cheaply before the expensive full pass.
     *
     * Everything the pass accumulates as a side effect — violation counters, reference counts,
     * per-attribute metadata, the sorted relation files and the Bloom filter — is rolled back
     * afterwards, so the subsequent full pass behaves exactly as if the sample had never run. The
     * only thing that survives is the set of candidates it disproved.
     */
    private void runSamplePass(Attribute[] attributes, Candidates candidates, ColumnStats columnStats)
            throws IOException, InterruptedException {

        Map<Integer, List<Path>> fullChunks = new HashMap<>();
        for (RelationMetadata relation : relationMetadata) {
            fullChunks.put(relation.id, new ArrayList<>(relation.chunks));
            int sampleSize = Math.max(1, (int) Math.ceil(relation.chunks.size() * config.samplingFraction));
            if (sampleSize >= relation.chunks.size()) {
                continue; // relation fits in a single chunk: sampling it would just do the work twice
            }
            List<Path> sample = new ArrayList<>(relation.chunks.subList(0, sampleSize));
            relation.chunks.clear();
            relation.chunks.addAll(sample);
        }

        int candidatesBefore = countCandidates(attributes);
        try {
            attachAttributes(attributes);
            List<SortJob> sortJobs = createSortJobs();

            ExecutorService executors = Executors.newFixedThreadPool(config.PARALLEL);
            List<SortResult> sortResults = executors.invokeAll(sortJobs.stream().sorted().toList()).stream().map(future -> {
                try {
                    return future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    return new SortResult(null, null);
                }
            }).toList();
            executors.shutdown();

            int activeRelations = iterativeMerge(attributes, sortResults.stream().map(SortResult::mergeJob).toList());

            Validator validator = new Validator(config, candidates, config.VALIDATION_SIZE / activeRelations);
            validator.validateSample(columnStats, filter);
            candidates.cleanCandidates();
        } finally {
            // Restore the full chunk lists whatever happened, or the main loop would silently run
            // on the sample and under-report violations.
            for (RelationMetadata relation : relationMetadata) {
                relation.chunks.clear();
                relation.chunks.addAll(fullChunks.get(relation.id));
            }
            for (RelationMetadata relation : relationMetadata) {
                Files.deleteIfExists(Path.of(config.tempFolder + File.separator + "relation_" + relation.id + ".txt"));
            }
            candidates.resetAfterSampling(attributes);
            if (config.useFilter) {
                this.filter = BloomFilter.create(Funnels.integerFunnel(), 100_000_000, 0.05);
            }
        }

        metrics.samplePruned = candidatesBefore - countCandidates(attributes);
        logger.info("Progressive sampling removed " + metrics.samplePruned + " of " + candidatesBefore + " candidates");
    }

    private int countCandidates(Attribute[] attributes) {
        int total = 0;
        for (Attribute attribute : attributes) {
            if (attribute.getReferenced() != null) total += attribute.getReferenced().size();
        }
        return total;
    }

    private void runSimilarityDiscovery(Attribute[] attributes) throws IOException, CsvValidationException, InterruptedException {
        SimilarityMeasure measure = SimilarityMeasureFactory.create(config);
        logger.info("Starting similarity discovery (mode={}, measure={}, threshold={})",
                config.similarityMode, measure.name(), config.threshold);

        clock.start("preprocessing");
        Map<Integer, LengthBucketedColumn> columns =
                ColumnPreprocessor.preprocess(relationMetadata, attributes, measure, config);
        logger.info("Finished preprocessing. Took: " + clock.stop("preprocessing") + "ms");

        clock.start("pruning");
        Map<Integer, Set<Integer>> surviving = CandidatePruner.prune(columns, measure, attributes, config);
        logger.info("Finished candidate pruning. Took: " + clock.stop("pruning") + "ms");

        clock.start("validation");
        new SimilarityValidator(measure, config).validate(columns, surviving, attributes);
        logger.info("Finished similarity validation. Took: " + clock.stop("validation") + "ms");

        int psINDs = 0;
        for (Attribute a : attributes) {
            if (a.getReferenced() != null) psINDs += a.getReferenced().size();
        }
        metrics.unary = psINDs;
        metrics.layerAttributes.add(attributes.length);
        metrics.layerCandidates.add(psINDs);
        metrics.layerPINDs.add(psINDs);
        logger.info("Found " + psINDs + " psINDs at unary layer (" + measure.name() + ")");
        output.storePINDs(relationMetadata, attributes, 1, config);
    }

    private void cleanupTempFiles() {
        File[] temp = new File(config.tempFolder).listFiles();
        if (temp == null) return;
        for (File f : temp) {
            //noinspection ResultOfMethodCallIgnored
            f.delete();
        }
    }
}
