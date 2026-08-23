package runner;

import core.Spind;
import structures.TableSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Comparison harness for SPIND optimizations.
 *
 * Runs a fixed suite of datasets under a named variant (a set of optimization flags), records
 * wall-clock and per-phase timings plus candidate/pIND counts, and appends one JSON object per
 * run to {@code benchmark/results/runs.jsonl}.
 *
 * Every run also writes a canonical (sorted) dump of the discovered pINDs and records its
 * SHA-256. Two variants are only comparable if their digests match for the same dataset — that
 * is the correctness gate: an optimization may change the runtime, never the result.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=runner.Benchmark -Dexec.args="baseline"
 *   mvn exec:java -Dexec.mainClass=runner.Benchmark -Dexec.args="optimized tpch1 t2d"
 *
 * First argument is the variant name; any further arguments restrict the suite to those
 * dataset keys.
 */
public class Benchmark {

    /** Number of measured runs per (variant, dataset). The reported figure is the minimum. */
    private static final int REPETITIONS = Integer.getInteger("bench.repetitions", 3);

    private static final String DATA_ROOT = "F:\\metaserve\\io\\data";
    private static final String TEMP_ROOT = "F:\\temp\\spind_bench";
    private static final String ARTIFACT_ROOT = "F:\\statistics\\spind_bench";
    private static final Path RESULT_FILE = Path.of("benchmark", "results", "runs.jsonl");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: Benchmark <variant> [datasetKey...]");
            System.exit(2);
        }
        String variant = args[0];
        List<String> only = List.of(args).subList(1, args.length);

        Map<String, Experiment> suite = suite();
        List<String> keys = only.isEmpty() ? new ArrayList<>(suite.keySet()) : new ArrayList<>(only);

        Files.createDirectories(RESULT_FILE.getParent());
        Files.createDirectories(Path.of(TEMP_ROOT));

        for (String key : keys) {
            Experiment experiment = suite.get(key);
            if (experiment == null) {
                System.err.println("unknown dataset key: " + key + " (known: " + suite.keySet() + ")");
                continue;
            }
            runExperiment(variant, key, experiment);
        }
    }

    private static void runExperiment(String variant, String key, Experiment experiment) throws Exception {
        System.out.println("=== " + variant + " / " + key + " ===");

        List<Run> runs = new ArrayList<>();
        for (int repetition = 0; repetition < REPETITIONS; repetition++) {
            clearDirectory(Path.of(TEMP_ROOT));
            Run run = singleRun(variant, key, experiment, repetition);
            runs.add(run);
            System.out.printf(Locale.ROOT, "  run %d: %,d ms algo (+%,d ms output, %d pINDs, digest %s)%n",
                    repetition, run.algoMillis, run.outputMillis, run.pinds, run.digest.substring(0, 12));
        }

        // The minimum is the least noise-contaminated estimate of the true cost; the median is
        // reported alongside so a single lucky run cannot carry the comparison.
        runs.sort(Comparator.comparingLong(r -> r.algoMillis));
        Run best = runs.get(0);
        long median = runs.get(runs.size() / 2).algoMillis;

        // A variant that finds different pINDs across its own repetitions is broken outright.
        for (Run run : runs) {
            if (!run.digest.equals(best.digest)) {
                throw new IllegalStateException("non-deterministic result for " + key + ": digests differ between repetitions");
            }
        }

        StringBuilder json = new StringBuilder();
        json.append('{');
        appendString(json, "variant", variant).append(',');
        appendString(json, "dataset", key).append(',');
        appendString(json, "timestamp", java.time.Instant.now().toString()).append(',');
        json.append("\"relations\":").append(best.relations).append(',');
        json.append("\"attributes\":").append(best.attributes).append(',');
        json.append("\"threshold\":").append(experiment.threshold).append(',');
        json.append("\"maxNary\":").append(experiment.maxNary).append(',');
        json.append("\"repetitions\":").append(REPETITIONS).append(',');
        json.append("\"algo_ms_min\":").append(best.algoMillis).append(',');
        json.append("\"algo_ms_median\":").append(median).append(',');
        json.append("\"algo_ms_all\":").append(runs.stream().map(r -> String.valueOf(r.algoMillis)).toList()).append(',');
        json.append("\"total_ms_min\":").append(best.totalMillis).append(',');
        json.append("\"output_ms\":").append(best.outputMillis).append(',');
        json.append("\"chunk_ms\":").append(best.chunkMillis).append(',');
        json.append("\"sort_ms\":").append(best.sortMillis).append(',');
        json.append("\"merge_ms\":").append(best.mergeMillis).append(',');
        json.append("\"validate_ms\":").append(best.validateMillis).append(',');
        json.append("\"prune_ms\":").append(best.pruneMillis).append(',');
        json.append("\"peak_heap_mb\":").append(best.peakHeapMb).append(',');
        json.append("\"sampling_ms\":").append(best.samplingMillis).append(',');
        json.append("\"inmemory_ms\":").append(best.inMemoryMillis).append(',');
        json.append("\"partitioned_ms\":").append(best.partitionedMillis).append(',');
        json.append("\"candidates_layer1\":").append(best.candidatesLayer1).append(',');
        json.append("\"pre_pruned\":").append(best.prePruned).append(',');
        json.append("\"sample_pruned\":").append(best.samplePruned).append(',');
        json.append("\"removed_by_bound\":").append(best.removedByBound).append(',');
        json.append("\"partitions\":").append(best.partitions).append(',');
        json.append("\"partition_depth\":").append(best.partitionDepth).append(',');
        json.append("\"pinds\":").append(best.pinds).append(',');
        appendString(json, "digest", best.digest);
        json.append('}');

        Files.writeString(RESULT_FILE, json + System.lineSeparator(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.printf(Locale.ROOT, "  -> best %,d ms | median %,d ms | %,d candidates | %,d pINDs | %,d MB peak%n",
                best.algoMillis, median, best.candidatesLayer1, best.pinds, best.peakHeapMb);
    }

    private static Run singleRun(String variant, String key, Experiment experiment, int repetition) throws Exception {
        Config config = new Config(experiment.threshold);
        config.folderPath = DATA_ROOT;
        config.tempFolder = TEMP_ROOT;
        config.resultFolder = ARTIFACT_ROOT + java.io.File.separator + variant + "_" + key;
        config.canonicalFolder = config.resultFolder;
        config.maxNary = experiment.maxNary;
        config.executionName = variant + "_" + key;
        config.databaseName = key;
        Files.createDirectories(Path.of(config.resultFolder));

        experiment.configure.accept(config);
        applyVariant(config, variant);

        PeakHeapSampler sampler = new PeakHeapSampler();
        sampler.start();

        long start = System.currentTimeMillis();
        Spind spind = new Spind(config);
        spind.execute();
        long elapsed = System.currentTimeMillis() - start;

        sampler.stop();

        Run run = new Run();
        run.totalMillis = elapsed;
        run.peakHeapMb = sampler.peakMb();
        run.relations = config.tableSources.size();
        run.digest = digestOf(Path.of(config.canonicalFolder));
        run.pinds = countPINDs(Path.of(config.canonicalFolder));

        // Phase timings and counters come from the metadata file the run just wrote.
        Map<String, String> metadata = latestMetadata(Path.of(config.resultFolder), config.executionName);
        run.sortMillis = sumOfList(metadata.get("sort_times"));
        run.mergeMillis = sumOfList(metadata.get("merge_times"));
        run.validateMillis = sumOfList(metadata.get("validate_times"));
        run.pruneMillis = sumOfList(metadata.get("preprune_times"));
        run.chunkMillis = sumOfList(metadata.get("chunk_times"));
        run.outputMillis = sumOfList(metadata.get("output_times"));
        run.algoMillis = elapsed - run.outputMillis;
        run.attributes = (int) parseLong(metadata.get("attributes"), 0);
        run.candidatesLayer1 = firstOfList(metadata.get("candidates_per_layer"));
        run.samplingMillis = sumOfList(metadata.get("sampling_times"));
        run.inMemoryMillis = sumOfList(metadata.get("inmemory_times"));
        run.partitionedMillis = sumOfList(metadata.get("partitioned_times"));
        run.prePruned = parseLong(metadata.get("pre_pruned"), 0);
        run.samplePruned = parseLong(metadata.get("sample_pruned"), 0);
        run.removedByBound = parseLong(metadata.get("removed_by_bound"), 0);
        run.partitions = parseLong(metadata.get("partitions"), 0);
        run.partitionDepth = parseLong(metadata.get("partition_depth"), 0);
        return run;
    }

    /**
     * Turns a variant name into the optimization flags it stands for. Each optimization is also
     * available on its own so a combined speedup can be attributed rather than just observed.
     * Unknown names run the unmodified algorithm, which is what calibration runs want.
     */
    private static void applyVariant(Config config, String variant) {
        // A representation change such as the candidate-list storage cannot hide behind a flag —
        // it affects every variant, baseline included. Those are measured by running two compiled
        // binaries against each other in one session, so the name carries a binary suffix that
        // must not change the configuration.
        switch (variant.replaceFirst("^(linkedlist|arraybacked)-", "")) {
            case "o1-preprune" -> {
                config.collectColumnStats = true;
                config.usePrePrune = true;
            }
            case "o2-fastvalidation" -> config.useFastValidation = true;
            case "o4-inmemory" -> config.useInMemoryValidation = true;
            // Partitioned on its own: the whole-dataset index is disabled so every dataset takes
            // the partitioned path, including ones small enough to fit in memory.
            case "o5-partitioned" -> config.usePartitionedValidation = true;
            // Forces deep re-partitioning on data that would otherwise never need it. Without this
            // the recursion is dead code on every dataset here — largest partition observed is 4.4M
            // records against an 8M budget — and untested recursion is not a feature.
            case "o5-forced-recursion" -> {
                config.usePartitionedValidation = true;
                config.partitionMaxRecords = 20_000L;
            }
            // Same algorithm, delimited-text partition spills instead of binary. Isolates how much
            // of the write/load cost was text formatting rather than bytes moved.
            case "o5-text-partitions" -> {
                config.usePartitionedValidation = true;
                config.useBinaryPartitions = false;
            }
            // Skips the chunk round-trip: the partitioner reads sources directly.
            case "o5-direct-source" -> {
                config.usePartitionedValidation = true;
                config.usePartitionFromSource = true;
                
                config.chunkThresholdBytes = Long.MAX_VALUE;
            }
            // Chunks only the relations large enough to need parallel decomposition and reads the
            // rest straight from source.
            case "o5-hybrid-source" -> {
                config.usePartitionedValidation = true;
                config.usePartitionFromSource = true;
            }
            case "o5-only-large" -> {
                config.useInMemoryValidation = true;
                config.usePartitionedValidation = true;
            }
            case "optimized-partitioned" -> {
                config.collectColumnStats = true;
                config.usePrePrune = true;
                config.useFastValidation = true;
                config.useInMemoryValidation = true;
                config.usePartitionedValidation = true;
            }
            // Forces the in-memory build to hit its ceiling part-way and hand back to the
            // sort-merge pipeline. Exercises the rollback, which no size-gated run reaches: the
            // result must still be bit-identical to the baseline.
            case "o4-fallback" -> {
                config.useInMemoryValidation = true;
                config.inMemoryValidationLimitBytes = Long.MAX_VALUE;
                config.inMemoryMaxDistinctValues = 1_000;
            }
            case "optimized-inmemory" -> {
                config.collectColumnStats = true;
                config.usePrePrune = true;
                config.useFastValidation = true;
                config.useInMemoryValidation = true;
            }
            case "o3-sampling" -> {
                config.collectColumnStats = true;
                config.useProgressiveSampling = true;
            }
            // Sampling is deliberately absent: it loses valid pINDs here (see Config#useProgressiveSampling),
            // and an optimization that changes the result is not a candidate for the combined variant.
            case "optimized" -> {
                config.collectColumnStats = true;
                config.usePrePrune = true;
                config.useFastValidation = true;
            }
            case "base", "baseline" -> {
                // Every flag stays at its default of off.
            }
            // A typo used to fall through to the baseline configuration and quietly report the
            // baseline's runtime under the optimization's name. Fail instead: a misspelled variant
            // is never something you want to discover from a suspiciously flat result table.
            default -> throw new IllegalArgumentException(
                    "unknown variant '" + variant + "'. Known: baseline, o1-preprune, o2-fastvalidation, "
                            + "o3-sampling, o4-inmemory, o4-fallback, o5-partitioned, o5-text-partitions, "
                            + "o5-direct-source, o5-hybrid-source, o5-forced-recursion, o5-only-large, "
                            + "optimized, optimized-inmemory, optimized-partitioned. "
                            + "Prefix with 'linkedlist-' or 'arraybacked-' to label a binary.");
        }
    }

    // ------------------------------------------------------------------ suite

    private static Map<String, Experiment> suite() {
        Map<String, Experiment> suite = new LinkedHashMap<>();

        // 1.05 GB across 8 relations / 61 columns. Volume-bound: dominated by sorting, merging
        // and the per-value validation scan. Few columns, so the candidate space is tiny.
        suite.put("tpch1", new Experiment(0.93, 1, config -> {
            config.separator = '|';
            config.inputFileHasHeader = false;
            addFolder(config, "TPC-H 1", ".tbl");
        }));

        // Same data at threshold 1.0 (exact INDs). Bound-based pruning is strongest here
        // because the tolerated violation budget is zero.
        suite.put("tpch1-exact", new Experiment(1.0, 1, config -> {
            config.separator = '|';
            config.inputFileHasHeader = false;
            addFolder(config, "TPC-H 1", ".tbl");
        }));

        // 1.27 GB of real-world open data across 17 wide CSVs. Mixed types, many nulls.
        suite.put("us", new Experiment(0.93, 1, config -> addFolder(config, "US", ".csv")));

        // 5.4 GB, same 8 relations as tpch1. Five times the data at an unchanged candidate
        // count, which isolates per-value cost from per-candidate cost.
        suite.put("tpch5", new Experiment(0.93, 1, config -> {
            config.separator = '|';
            config.inputFileHasHeader = false;
            addFolder(config, "TPC-H 5", ".tbl");
        }));

        // 6.1 GB of real IMDB dumps across 7 relations: long free-text strings and heavy null
        // usage, unlike TPC-H's uniform synthetic values.
        suite.put("imdb", new Experiment(0.93, 1, config -> {
            config.separator = '\t';
            config.quoteChar = '\0';
            config.nullString = "\\N";
            addFolder(config, "IMDB", ".csv");
        }));

        // 669 small relations, 109 MB. Candidate-space-bound: thousands of columns means the
        // unary candidate set is quadratic while the data itself is trivial to scan.
        suite.put("t2d", new Experiment(0.93, 1, config -> addFolder(config, "T2D", ".csv")));

        // 5000 relations, 12 MB. The extreme of the same axis — here essentially all cost is
        // candidate bookkeeping rather than I/O.
        suite.put("webtables", new Experiment(0.93, 1, config -> addFolder(config, "WebTables", ".csv")));

        // 17 GB across 171 relations — the only dataset here that is large *and* attribute-rich.
        // Every other entry sits on one axis or the other, which is why the remaining-mass bound
        // has so far been an impressive pruning statistic with no measurable effect on runtime.
        suite.put("musicbrainz", new Experiment(0.93, 1, config -> {
            config.separator = '\t';
            config.quoteChar = '\0';
            config.nullString = "\\N";
            config.inputFileHasHeader = false;
            addFolder(config, "Musicbrainz", "");
        }));

        // The one genuinely multi-model entry: the same TPC-H source split across a relational, a
        // document and a graph store, so every cross-model IND is known by construction. See
        // benchmark/multimodel/. Threshold 0.95 so the deliberately injected partial dependency
        // (coverage 0.9697) is discovered rather than filtered out.
        suite.put("multimodel", new Experiment(0.95, 1, config -> {
            config.databaseName = "multimodel";
            config.addTable(new TableSource.Postgres("pg", "public", "customer"));
            config.addTable(new TableSource.Postgres("pg", "public", "nation"));
            config.addTable(new TableSource.Postgres("pg", "public", "region"));
            config.addTable(new TableSource.Mongo("mongo", "shop", "orders"));
            config.addTable(new TableSource.Neo4jLabel("graph", "neo4j", "Part"));
            config.addTable(new TableSource.Neo4jLabel("graph", "neo4j", "Supplier"));
        }));

        // Same setup under set semantics. Exploding an embedded array repeats the parent fields
        // once per element, which changes multiplicities but not the distinct value set — so the
        // coverage of a dependency on a parent field should move under bag semantics and stay put
        // under set semantics. This entry measures the second half of that claim.
        suite.put("multimodel-set", new Experiment(0.95, 1, config -> {
            config.databaseName = "multimodel-set";
            config.duplicateHandling = Config.DuplicateHandling.UNAWARE;
            config.addTable(new TableSource.Postgres("pg", "public", "customer"));
            config.addTable(new TableSource.Postgres("pg", "public", "nation"));
            config.addTable(new TableSource.Postgres("pg", "public", "region"));
            config.addTable(new TableSource.Mongo("mongo", "shop", "orders"));
            config.addTable(new TableSource.Neo4jLabel("graph", "neo4j", "Part"));
            config.addTable(new TableSource.Neo4jLabel("graph", "neo4j", "Supplier"));
        }));

        // Threshold sweep. The remaining-mass bound should sharpen as rho rises: demanding nearly
        // complete coverage makes a candidate impossible sooner. If that shows up, the bound is
        // exploiting partial-IND semantics rather than just reusing Grace hash partitioning.
        for (double rho : new double[]{1.00, 0.99, 0.95, 0.90, 0.75}) {
            String key = "us-rho" + String.valueOf((int) Math.round(rho * 100));
            suite.put(key, new Experiment(rho, 1, config -> addFolder(config, "US", ".csv")));
        }

        // n-ary sanity check: optimizations targeted at the unary layer must not slow down
        // higher layers, where candidates are generated rather than seeded.
        suite.put("tpch1-nary2", new Experiment(0.93, 2, config -> {
            config.separator = '|';
            config.inputFileHasHeader = false;
            addFolder(config, "TPC-H 1", ".tbl");
        }));

        return suite;
    }

    /**
     * Registers every file with the given extension directly inside {@code DATA_ROOT/folder}.
     * Scanning beats the hard-coded name lists in {@link Config.Dataset}, which have drifted out
     * of sync with what is actually on disk (T2D lists ~800 tables, 669 exist).
     */
    private static void addFolder(Config config, String folder, String extension) {
        Path root = Path.of(DATA_ROOT, folder);
        try (Stream<Path> files = Files.list(root)) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(extension))
                    .sorted()
                    .forEach(path -> config.addTable(new TableSource.File(path)));
        } catch (IOException e) {
            throw new IllegalStateException("cannot list dataset folder " + root, e);
        }
        if (config.tableSources.isEmpty()) {
            throw new IllegalStateException("no " + extension + " files under " + root);
        }
    }

    // ------------------------------------------------------------- bookkeeping

    private record Experiment(double threshold, int maxNary, Consumer<Config> configure) {
    }

    private static final class Run {
        long totalMillis;
        /** {@link #totalMillis} minus result serialization — the figure variants are compared on. */
        long algoMillis;
        long outputMillis;
        long chunkMillis;
        long sortMillis;
        long mergeMillis;
        long validateMillis;
        long pruneMillis;
        long samplingMillis;
        long inMemoryMillis;
        long partitionedMillis;
        long peakHeapMb;
        long prePruned;
        long samplePruned;
        long removedByBound;
        long partitions;
        long partitionDepth;
        int relations;
        int attributes;
        long candidatesLayer1;
        long pinds;
        String digest;
    }

    /** Polls heap usage on a background thread; good enough to catch order-of-magnitude changes. */
    private static final class PeakHeapSampler {
        private volatile boolean running = true;
        private volatile long peakBytes;
        private Thread thread;

        void start() {
            thread = new Thread(() -> {
                Runtime runtime = Runtime.getRuntime();
                while (running) {
                    long used = runtime.totalMemory() - runtime.freeMemory();
                    if (used > peakBytes) peakBytes = used;
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        void stop() throws InterruptedException {
            running = false;
            thread.join(2000);
        }

        long peakMb() {
            return peakBytes / (1024 * 1024);
        }
    }

    private static String digestOf(Path canonicalFolder) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (Stream<Path> files = Files.list(canonicalFolder)) {
            List<Path> canonical = files.filter(p -> p.getFileName().toString().endsWith("_canonical.txt")).sorted().toList();
            for (Path path : canonical) {
                digest.update(Files.readAllBytes(path));
            }
        }
        StringBuilder hex = new StringBuilder();
        for (byte b : digest.digest()) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    private static long countPINDs(Path canonicalFolder) throws IOException {
        long total = 0;
        try (Stream<Path> files = Files.list(canonicalFolder)) {
            for (Path path : files.filter(p -> p.getFileName().toString().endsWith("_canonical.txt")).toList()) {
                try (Stream<String> lines = Files.lines(path)) {
                    total += lines.count();
                }
            }
        }
        return total;
    }

    /** Reads the newest {@code <executionName>_<epoch>.json} metadata file as flat key/value pairs. */
    private static Map<String, String> latestMetadata(Path resultFolder, String executionName) throws IOException {
        Path newest = null;
        try (Stream<Path> files = Files.list(resultFolder)) {
            for (Path path : files.filter(p -> p.getFileName().toString().startsWith(executionName + "_")).toList()) {
                if (newest == null || Files.getLastModifiedTime(path).compareTo(Files.getLastModifiedTime(newest)) > 0) {
                    newest = path;
                }
            }
        }
        Map<String, String> result = new LinkedHashMap<>();
        if (newest == null) return result;

        String content = Files.readString(newest).trim();
        content = content.substring(1, content.length() - 1); // strip outer braces

        // Flat, machine-generated JSON: split on commas that are not inside a list or a string.
        int depth = 0;
        boolean inString = false;
        StringBuilder field = new StringBuilder();
        List<String> fields = new ArrayList<>();
        for (char c : content.toCharArray()) {
            if (c == '"') inString = !inString;
            if (!inString && (c == '[')) depth++;
            if (!inString && (c == ']')) depth--;
            if (c == ',' && depth == 0 && !inString) {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        fields.add(field.toString());

        for (String entry : fields) {
            int colon = entry.indexOf(':');
            if (colon < 0) continue;
            String key = entry.substring(0, colon).trim().replace("\"", "");
            String value = entry.substring(colon + 1).trim();
            result.put(key, value);
        }
        return result;
    }

    /** Sums a JSON list of numbers such as {@code [12, 34]}; missing or null yields 0. */
    private static long sumOfList(String list) {
        if (list == null || list.equals("null") || list.isBlank()) return 0;
        String stripped = list.replace("[", "").replace("]", "").trim();
        if (stripped.isEmpty()) return 0;
        long sum = 0;
        for (String part : stripped.split(",")) {
            sum += parseLong(part.trim(), 0);
        }
        return sum;
    }

    private static long firstOfList(String list) {
        if (list == null || list.equals("null") || list.isBlank()) return 0;
        String stripped = list.replace("[", "").replace("]", "").trim();
        if (stripped.isEmpty()) return 0;
        return parseLong(stripped.split(",")[0].trim(), 0);
    }

    private static long parseLong(String value, long fallback) {
        try {
            return Long.parseLong(value.trim());
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    private static StringBuilder appendString(StringBuilder builder, String key, String value) {
        return builder.append('"').append(key).append("\":\"").append(value).append('"');
    }

    private static void clearDirectory(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
            return;
        }
        try (Stream<Path> files = Files.list(directory)) {
            for (Path path : files.toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
