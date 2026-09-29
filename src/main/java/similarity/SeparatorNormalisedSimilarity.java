package similarity;

import java.util.List;

/**
 * Equality after folding word separators and case.
 *
 * Two publishers that name the same entity in prose disagree about the character between words far
 * more often than about the words. The Helsinki Person dataset is the clean case: its document side
 * writes a Wikipedia page title as {@code Henry Standing Bear} and its graph side writes the same
 * entity as {@code Henry_Standing_Bear}. The two observations share almost no value --- equality
 * coverage is 0.008907 --- so the correspondence is absent rather than weakly supported, and no
 * threshold on an equality-based inclusion reaches it. Folding the separator takes the same pair to
 * 0.999974, with five titles of 153,134 left over.
 *
 * <p>Edit distance is the wrong instrument here and measurably so. The distance between the two
 * spellings is the number of spaces in the name, so it grows with the string and no fixed radius
 * expresses the equivalence: on this pair τ = 1 reaches 0.675, τ = 2 reaches 0.928 and τ = 4
 * reaches 0.991, still short of 0.9999, while each increment admits unrelated pairs elsewhere. A
 * character equivalence is not a small distance; it is a different relation.</p>
 *
 * <p>The normalisation is deliberately narrow. Only characters that separate words are folded ---
 * space, underscore, hyphen --- together with case, and nothing else is touched, so a column whose
 * values differ in any other way is left exactly where equality left it. Two values that differ
 * only in how many separators they use still differ here, since each separator maps to one
 * character rather than being collapsed: {@code A__B} and {@code A_B} remain distinct, which keeps
 * the relation a normalisation rather than a fuzzy match.</p>
 *
 * <p>Indexing is therefore exact over normalised values, as for the registry-prefix measure, and
 * length bucketing uses the normalised length --- which equals the original length, since folding
 * substitutes rather than deletes.</p>
 */
public class SeparatorNormalisedSimilarity implements SimilarityMeasure {

    /**
     * Folds word separators to one character and lowercases the rest.
     *
     * <p>Hyphen is included with space and underscore because the same three alternate freely in
     * titles and identifiers drawn from prose. Length is preserved, which is what lets the length
     * bucketing stay exact.</p>
     */
    static String normalise(String value) {
        StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == ' ' || c == '_' || c == '-') {
                out.append('_');
            } else {
                out.append(Character.toLowerCase(c));
            }
        }
        return out.toString();
    }

    @Override
    public String name() { return "separator-normalised"; }

    @Override
    public int indexSlots() { return 1; }

    @Override
    public int lengthOf(String value) { return value.length(); }

    @Override
    public boolean isSimilar(String a, String b) { return normalise(a).equals(normalise(b)); }

    @Override
    public boolean lengthFeasible(int depLen, int refLen) { return depLen == refLen; }

    @Override
    public int refLengthLowerBound(int depLen) { return depLen; }

    @Override
    public int refLengthUpperBound(int depLen) { return depLen; }

    @Override
    public boolean isColumnTrivial(LengthBucketedColumn column) {
        // Equality on normalised values is never trivial the way a small edit budget is: two
        // one-character values are similar here only if they fold to the same character.
        return column.isEmpty();
    }

    @Override
    public List<IndexKey> indexKeys(String value) {
        return List.of(new IndexKey(0, normalise(value)));
    }

    @Override
    public List<IndexKey> probeKeys(String value, int targetRefLength) {
        return List.of(new IndexKey(0, normalise(value)));
    }
}
