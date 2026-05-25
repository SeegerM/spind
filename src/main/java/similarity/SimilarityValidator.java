package similarity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.PINDList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Discovers partial similarity inclusion dependencies (psINDs).
 *
 * Replaces SPIND's {@code Sorter → Merger → Validator} chain when {@code config.similarityMode}
 * is not {@code NONE}. Implements SAWFISH Algorithm 3 (paper §4.5) with the partial-sIND
 * relaxation layered on top: a candidate keeps surviving while its accumulated violations stay
 * within {@code (1 − config.threshold) * |distinct dep values|}.
 *
 * Parallelism mirrors {@link core.Spind}'s existing pool sizing — one task per referenced
 * column, each task fully owning its inverted index.
 */
public class SimilarityValidator {

    private static final Logger logger = LoggerFactory.getLogger(SimilarityValidator.class);

    private final SimilarityMeasure measure;
    private final Config config;

    public SimilarityValidator(SimilarityMeasure measure, Config config) {
        this.measure = measure;
        this.config = config;
    }

    /**
     * Validates surviving candidate (dep, ref) pairs and writes psINDs into
     * {@code attributes[depId].referenced} via {@link PINDList}, matching the structure that
     * {@link io.Output#storePINDs} already consumes.
     */
    public void validate(
            Map<Integer, LengthBucketedColumn> columns,
            Map<Integer, Set<Integer>> survivingByRef,
            Attribute[] attributes) throws InterruptedException {

        // (refId, depId) → accumulated violations for that pair.
        Map<Integer, Map<Integer, Long>> violationsByRef = new HashMap<>();
        Map<Integer, Set<Integer>> finalSurvivors = new HashMap<>();

        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, config.PARALLEL));
        List<RefTask> tasks = new ArrayList<>();
        for (Map.Entry<Integer, Set<Integer>> entry : survivingByRef.entrySet()) {
            tasks.add(new RefTask(entry.getKey(), entry.getValue(), columns));
        }

        List<RefResult> results = new ArrayList<>(tasks.size());
        for (var future : pool.invokeAll(tasks)) {
            try {
                results.add(future.get());
            } catch (Exception e) {
                logger.error("Validation task failed", e);
            }
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);

        for (RefResult r : results) {
            if (r == null || r.survivors.isEmpty()) continue;
            finalSurvivors.put(r.refId, r.survivors);
            violationsByRef.put(r.refId, r.violations);
        }

        // Materialise psINDs into the existing PINDList structure so io/Output works unchanged.
        emitPSINDs(attributes, columns, finalSurvivors, violationsByRef);
    }

    private void emitPSINDs(
            Attribute[] attributes,
            Map<Integer, LengthBucketedColumn> columns,
            Map<Integer, Set<Integer>> survivingByRef,
            Map<Integer, Map<Integer, Long>> violationsByRef) {

        // Group results dep-major so each Attribute gets one PINDList build pass.
        Map<Integer, List<int[]>> byDep = new HashMap<>();   // depId → list of {refId, violations}
        for (var refEntry : survivingByRef.entrySet()) {
            int refId = refEntry.getKey();
            Map<Integer, Long> violations = violationsByRef.getOrDefault(refId, Collections.emptyMap());
            for (int depId : refEntry.getValue()) {
                long v = violations.getOrDefault(depId, 0L);
                byDep.computeIfAbsent(depId, k -> new ArrayList<>())
                        .add(new int[]{refId, (int) Math.min(Integer.MAX_VALUE, v)});
            }
        }

        int totalEmitted = 0;
        for (var depEntry : byDep.entrySet()) {
            int depId = depEntry.getKey();
            LengthBucketedColumn depCol = columns.get(depId);
            long cap = capFor(depCol);
            PINDList list = new PINDList();
            // Sort by refId for deterministic output.
            depEntry.getValue().sort((a, b) -> Integer.compare(a[0], b[0]));
            for (int[] pair : depEntry.getValue()) {
                list.add(pair[0], cap);
            }
            // Now walk in order again and set the violations counter via the public API.
            PINDList.PINDIterator it = list.elementIterator();
            for (int[] pair : depEntry.getValue()) {
                PINDList.PINDElement element = it.next();
                if (pair[1] > 0) element.violate(pair[1]);
                attributes[pair[0]].setNumReferencedBy(attributes[pair[0]].getNumReferencedBy() + 1);
                totalEmitted++;
            }
            attributes[depId].setReferenced(list);
        }

        logger.info("Discovered {} psIND candidate pairs ({}: τ={}, threshold={})",
                totalEmitted, measure.name(),
                config.similarityMode == Config.SimilarityMode.EDIT_DISTANCE
                        ? String.valueOf(config.editDistanceThreshold)
                        : String.valueOf(config.normalizedThreshold),
                config.threshold);
    }

    private long capFor(LengthBucketedColumn dep) {
        return (long) ((1.0 - config.threshold) * dep.distinctValueCount());
    }

    // ------------------------------------------------------------------------------------
    // Per-ref-column validation
    // ------------------------------------------------------------------------------------

    private class RefTask implements java.util.concurrent.Callable<RefResult> {
        final int refId;
        final Set<Integer> depCandidates;
        final Map<Integer, LengthBucketedColumn> columns;

        RefTask(int refId, Set<Integer> depCandidates, Map<Integer, LengthBucketedColumn> columns) {
            this.refId = refId;
            this.depCandidates = new HashSet<>(depCandidates);
            this.columns = columns;
        }

        @Override
        public RefResult call() {
            LengthBucketedColumn refCol = columns.get(refId);
            InvertedIndex index = new InvertedIndex(measure, refCol);

            // Per-dep accumulated violations and caps.
            Map<Integer, Long> violations = new HashMap<>();
            Map<Integer, Long> caps = new HashMap<>();
            for (int depId : depCandidates) {
                violations.put(depId, 0L);
                caps.put(depId, capFor(columns.get(depId)));
            }

            // Collect the union of dep lengths across all surviving candidates so we visit each
            // length at most once (the index window only depends on the *current* dep length).
            TreeSet<Integer> depLengths = new TreeSet<>();
            for (int depId : depCandidates) {
                for (Integer l : columns.get(depId).lengthsDescending()) {
                    depLengths.add(l);
                }
            }

            // Iterate longest → shortest (paper §4.3: fewer accidental matches at long lengths).
            Iterator<Integer> lengthIter = depLengths.descendingIterator();
            while (lengthIter.hasNext() && !depCandidates.isEmpty()) {
                int l = lengthIter.next();
                int lo = measure.refLengthLowerBound(l);
                int hi = measure.refLengthUpperBound(l);

                // Slide the window: drop indices outside, load anything inside that ref has.
                index.evictLengthsOutsideWindow(lo, hi);
                for (Integer refLen : refCol.lengthsInRange(lo, hi)) {
                    index.ensureLengthLoaded(refLen);
                }

                // Validate each dep candidate's values at length l.
                Iterator<Integer> depIter = depCandidates.iterator();
                while (depIter.hasNext()) {
                    int depId = depIter.next();
                    LengthBucketedColumn depCol = columns.get(depId);
                    Set<String> depValues = depCol.valuesAtLength(l);
                    if (depValues.isEmpty()) continue;

                    long viol = violations.get(depId);
                    long cap = caps.get(depId);
                    for (String value : depValues) {
                        if (!findSimilar(value, lo, hi, refCol, index)) {
                            viol++;
                            if (viol > cap) {
                                depIter.remove();
                                break;
                            }
                        }
                    }
                    violations.put(depId, viol);
                }
            }

            // Anything remaining in depCandidates survived → emit-able.
            Map<Integer, Long> survivorViolations = new HashMap<>();
            for (int depId : depCandidates) {
                survivorViolations.put(depId, violations.get(depId));
            }
            return new RefResult(refId, depCandidates, survivorViolations);
        }

        private boolean findSimilar(String depValue, int lo, int hi, LengthBucketedColumn refCol, InvertedIndex index) {
            Set<String> seen = new HashSet<>();
            for (Integer refLen : refCol.lengthsInRange(lo, hi)) {
                List<IndexKey> probes = measure.probeKeys(depValue, refLen);
                for (IndexKey probe : probes) {
                    for (String candidate : index.lookup(refLen, probe.slot(), probe.key())) {
                        if (seen.add(candidate) && measure.isSimilar(depValue, candidate)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    private record RefResult(int refId, Set<Integer> survivors, Map<Integer, Long> violations) {}
}
