package similarity;

import java.util.List;

/**
 * Hybrid Levenshtein (paper §3, hybrid formula):
 *
 * <pre>
 *   x ∼δ y  =  ED(x,y) ≤ 1            if max(|x|,|y|) · (1 − δ) ≤ 1
 *              NED(x,y) ≤ 1 − δ      otherwise
 * </pre>
 *
 * Phase B — interface-conforming skeleton. Once implemented, expected to be the most useful
 * measure for mixed-length columns: absolute tolerance protects short values from normalization
 * collapse, normalized tolerance scales with longer strings.
 */
public class HybridLevenshtein implements SimilarityMeasure {

    private final double delta;

    public HybridLevenshtein(double delta) {
        if (delta <= 0.0 || delta > 1.0) throw new IllegalArgumentException("delta must be in (0,1]");
        this.delta = delta;
    }

    public double delta() { return delta; }

    @Override
    public String name() { return "Hybrid(δ=" + delta + ")"; }

    @Override
    public int indexSlots() { throw notImplemented(); }

    @Override
    public int lengthOf(String value) {
        throw notImplemented();
    }

    @Override
    public boolean isSimilar(String a, String b) {
        throw notImplemented();
    }

    @Override
    public boolean lengthFeasible(int depLen, int refLen) {
        throw notImplemented();
    }

    @Override
    public int refLengthLowerBound(int depLen) {
        throw notImplemented();
    }

    @Override
    public int refLengthUpperBound(int depLen) {
        throw notImplemented();
    }

    @Override
    public boolean isColumnTrivial(LengthBucketedColumn column) {
        throw notImplemented();
    }

    @Override
    public List<IndexKey> indexKeys(String value) {
        throw notImplemented();
    }

    @Override
    public List<IndexKey> probeKeys(String value, int targetRefLength) {
        throw notImplemented();
    }

    private static UnsupportedOperationException notImplemented() {
        return new UnsupportedOperationException(
                "HybridLevenshtein is a Phase B stub — not yet implemented. "
                        + "Use SimilarityMode.EDIT_DISTANCE for now.");
    }
}
