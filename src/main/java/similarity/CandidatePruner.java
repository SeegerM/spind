package similarity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Drops candidate (dep, ref) pairs that cannot produce a meaningful or feasible psIND before
 * the validator builds any inverted indexes. Conservative — keeps anything that *might* hold
 * under the partial-sIND threshold. The full violation accounting happens in
 * {@link SimilarityValidator}.
 *
 * Rules applied here (paper §4.1, adapted for partial-sIND):
 *  - Empty ref column → no candidates can point at it (any dep value is an unavoidable violation).
 *  - Empty dep column → trivially holds but information-free; drop.
 *  - Reflexive (dep == ref) → drop (always holds, not interesting).
 *  - Either side marked trivial (oversized values OR measure-trivial under §3) → drop.
 *  - Length ranges disjoint beyond similarity window, AND distinct dep count exceeds the
 *    partial violation cap → drop (no possible way to satisfy threshold).
 */
public final class CandidatePruner {

    private static final Logger logger = LoggerFactory.getLogger(CandidatePruner.class);

    private CandidatePruner() {}

    public static Map<Integer, Set<Integer>> prune(
            Map<Integer, LengthBucketedColumn> columns,
            SimilarityMeasure measure,
            Attribute[] attributes,
            Config config) {

        Map<Integer, Set<Integer>> surviving = new HashMap<>();
        int dropped = 0;
        int kept = 0;

        for (Map.Entry<Integer, LengthBucketedColumn> refEntry : columns.entrySet()) {
            int refId = refEntry.getKey();
            LengthBucketedColumn ref = refEntry.getValue();
            if (ref.isEmpty() || ref.isTrivial()) continue;

            Set<Integer> deps = new HashSet<>();
            for (Map.Entry<Integer, LengthBucketedColumn> depEntry : columns.entrySet()) {
                int depId = depEntry.getKey();
                if (depId == refId) { dropped++; continue; }

                LengthBucketedColumn dep = depEntry.getValue();
                if (dep.isEmpty() || dep.isTrivial()) { dropped++; continue; }

                // Length-window overlap: cheapest sanity check. If dep's expanded length range
                // has no intersection with ref's actual length range, every dep value is an
                // unavoidable violation. Drop only if the violation total exceeds the cap.
                int refLo = measure.refLengthLowerBound(dep.minLength());
                int refHi = measure.refLengthUpperBound(dep.maxLength());
                if (refHi < ref.minLength() || refLo > ref.maxLength()) {
                    long cap = (long) ((1.0 - config.threshold) * dep.distinctValueCount());
                    if (dep.distinctValueCount() > cap) {
                        dropped++;
                        continue;
                    }
                }

                deps.add(depId);
                kept++;
            }
            if (!deps.isEmpty()) {
                surviving.put(refId, deps);
            }
        }

        logger.info("Candidate pruning: {} survived, {} dropped (across {} referenced columns)",
                kept, dropped, surviving.size());
        return surviving;
    }
}
