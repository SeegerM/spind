package similarity;

import java.util.ArrayList;
import java.util.List;

/**
 * Edit-distance similarity with PassJoin-style segmentation.
 *
 * Indexing: each referenced value of length {@code l} is split into τ+1 segments. Segments are
 * positioned 0..τ; remainder characters are pushed to the longer segments at the end (matches
 * the paper §4.3 description).
 *
 * Probing: for a dependent value, we generate candidate substrings whose start position is
 * within ±τ of the corresponding ref-side segment start. This is the "loose" PassJoin range —
 * correct by the pigeonhole argument, slightly broader than Li et al.'s position-tightened
 * bound. Empirical impact is small for τ ≤ 2 (the common case for typo discovery).
 */
public class LevenshteinSimilarity implements SimilarityMeasure {

    private final int tau;

    public LevenshteinSimilarity(int tau) {
        if (tau < 0) throw new IllegalArgumentException("tau must be non-negative");
        this.tau = tau;
    }

    public int tau() { return tau; }

    @Override
    public String name() { return "ED(τ=" + tau + ")"; }

    @Override
    public int indexSlots() { return tau + 1; }

    @Override
    public int lengthOf(String value) { return value.length(); }

    @Override
    public boolean isSimilar(String a, String b) {
        return EditDistance.withinThreshold(a, b, tau);
    }

    @Override
    public boolean lengthFeasible(int depLen, int refLen) {
        return Math.abs(depLen - refLen) <= tau;
    }

    @Override
    public int refLengthLowerBound(int depLen) { return Math.max(0, depLen - tau); }

    @Override
    public int refLengthUpperBound(int depLen) { return depLen + tau; }

    @Override
    public boolean isColumnTrivial(LengthBucketedColumn column) {
        if (column.isEmpty()) return true;
        // Paper §3: values with ≤ τ chars are pairwise similar (you can build any τ-char word
        // from any other in ≤ τ edits). A column whose longest value fits this bound produces
        // only "simple" sINDs that carry no real signal — exclude it.
        return column.maxLength() <= tau;
    }

    @Override
    public List<IndexKey> indexKeys(String value) {
        return segmentsAt(value, value.length());
    }

    @Override
    public List<IndexKey> probeKeys(String value, int targetRefLength) {
        int lx = value.length();
        int l = targetRefLength;
        int segments = tau + 1;
        if (l < segments) {
            // Ref value shorter than τ+1: degenerate. Treat ref as a single segment (whole value)
            // and let validation by raw isSimilar() catch it. Returning empty probes for this
            // length means index lookup misses, but the dep value will still violate (correctly)
            // unless a later length bucket matches.
            return List.of();
        }
        int baseLen = l / segments;
        int remainder = l % segments;
        List<IndexKey> probes = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < segments; i++) {
            int segLen = baseLen + (i >= segments - remainder ? 1 : 0);
            // Candidate substring start positions in the dep value: shift by up to ±τ from the
            // ref-side segment start. Clamp to valid substring window.
            int low = Math.max(0, start - tau);
            int high = Math.min(lx - segLen, start + tau);
            for (int s = low; s <= high; s++) {
                probes.add(new IndexKey(i, value.substring(s, s + segLen)));
            }
            start += segLen;
        }
        return probes;
    }

    private List<IndexKey> segmentsAt(String value, int l) {
        int segments = tau + 1;
        if (l < segments) {
            // Too short to segment τ+1 ways. Treat as a single key at slot 0; pairs of such
            // short values are caught by the trivial-column rule anyway.
            return List.of(new IndexKey(0, value));
        }
        int baseLen = l / segments;
        int remainder = l % segments;
        List<IndexKey> keys = new ArrayList<>(segments);
        int start = 0;
        for (int i = 0; i < segments; i++) {
            int segLen = baseLen + (i >= segments - remainder ? 1 : 0);
            keys.add(new IndexKey(i, value.substring(start, start + segLen)));
            start += segLen;
        }
        return keys;
    }
}
