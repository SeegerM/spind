package similarity;

import java.util.List;

/**
 * Equality after stripping a leading registry prefix from both values.
 *
 * Two publishers that record the same external identifier often disagree only about whether the
 * issuing registry's prefix is part of it. MovieLens writes an IMDb identifier as {@code 0114709}
 * and Wikidata writes the same identifier as {@code tt0114709}; the two observations share no
 * value at all, so the inclusion between them is not weakly supported but absent, and no threshold
 * on an equality-based inclusion reaches it.
 *
 * <p>Edit distance does reach it — a two-character insertion is within τ = 2 — and doing it that
 * way is a mistake. Consecutive numeric identifiers are one edit apart, so on an identifier column
 * edit distance manufactures agreement: at τ = 1 the MovieLens-to-TMDB reference rises from 0.9859
 * to 1.0000 and the Helsinki citation endpoint from 0.8460 to 0.9992, absorbing the very
 * partialities those instances are reported for. A relation loose enough to repair a prefix is
 * loose enough to erase a real reference gap.</p>
 *
 * <p>This measure is deliberately narrow instead. It strips one leading run of letters and then
 * compares for equality, so it repairs exactly the difference it is named for and leaves every
 * column without such a prefix exactly where equality left it. Measured on the MovieLens instance
 * it recovers the variant at 0.8712 against edit distance 2's 0.8715 -- the three extra matches
 * being neighbouring identifiers of the kind above -- and it does so in a fraction of the time,
 * because stripping is linear where the segmented index is not.</p>
 *
 * <p>Indexing is therefore trivial: one slot holding the stripped value, which makes the inverted
 * index an exact-match index over normalised values. Length bucketing uses the stripped length so
 * that a prefixed and an unprefixed spelling of one identifier land in the same bucket.</p>
 */
public class RegistryPrefixSimilarity implements SimilarityMeasure {

    /** Longest prefix stripped. Registry tags are short; a longer run is a word, not a tag. */
    private static final int MAX_PREFIX = 4;

    /**
     * Drops one leading run of ASCII letters, when it is short enough to be a registry tag and
     * something remains after it.
     *
     * <p>Both conditions matter. Without the length bound every alphabetic value would be reduced
     * to its tail; without the second, a purely alphabetic value would be reduced to the empty
     * string and every such value would collide with every other.</p>
     */
    static String strip(String value) {
        int i = 0;
        while (i < value.length() && i < MAX_PREFIX && isAsciiLetter(value.charAt(i))) {
            i++;
        }
        if (i == 0 || i == value.length()) {
            return value;
        }
        // Only a prefix followed by something non-alphabetic reads as a registry tag; "tt0114709"
        // qualifies and "Toyota" does not.
        return isAsciiLetter(value.charAt(i)) ? value : value.substring(i);
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    @Override
    public String name() { return "registry-prefix"; }

    @Override
    public int indexSlots() { return 1; }

    @Override
    public int lengthOf(String value) { return strip(value).length(); }

    @Override
    public boolean isSimilar(String a, String b) { return strip(a).equals(strip(b)); }

    @Override
    public boolean lengthFeasible(int depLen, int refLen) { return depLen == refLen; }

    @Override
    public int refLengthLowerBound(int depLen) { return depLen; }

    @Override
    public int refLengthUpperBound(int depLen) { return depLen; }

    @Override
    public boolean isColumnTrivial(LengthBucketedColumn column) {
        // Equality on normalised values is never trivial the way a small edit budget is: two
        // one-character values are similar here only if they are equal. An empty column carries
        // nothing either way.
        return column.isEmpty();
    }

    @Override
    public List<IndexKey> indexKeys(String value) {
        return List.of(new IndexKey(0, strip(value)));
    }

    @Override
    public List<IndexKey> probeKeys(String value, int targetRefLength) {
        return List.of(new IndexKey(0, strip(value)));
    }
}
