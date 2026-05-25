package similarity;

import java.util.List;

/**
 * Jaccard set-similarity over tokens (paper §3, §4.4.2). Phase B — interface-conforming
 * skeleton so the mode switch is shape-complete; implementation will use ScanCount validation
 * over a token → parent-strings inverted index.
 */
public class JaccardSimilarity implements SimilarityMeasure {

    private final double delta;

    public JaccardSimilarity(double delta) {
        if (delta <= 0.0 || delta > 1.0) throw new IllegalArgumentException("delta must be in (0,1]");
        this.delta = delta;
    }

    public double delta() { return delta; }

    @Override
    public String name() { return "JAC(δ=" + delta + ")"; }

    @Override
    public int indexSlots() { return 1; }

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
                "JaccardSimilarity is a Phase B stub — not yet implemented. "
                        + "Use SimilarityMode.EDIT_DISTANCE for now.");
    }
}
