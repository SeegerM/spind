package similarity;

import java.util.List;

/**
 * Strategy for similarity-based IND discovery — abstracts over the choice of measure
 * (Levenshtein ED, Jaccard, hybrid NED). The validator and inverted index work in terms of
 * this interface alone, so adding a new measure is a matter of dropping in a new implementation
 * and registering it in {@link SimilarityMeasureFactory}.
 */
public interface SimilarityMeasure {

    /** Short tag used for logging and output. */
    String name();

    /**
     * @return the number of distinct slot positions used by this measure. Levenshtein/PassJoin
     *         uses τ+1 slots; Jaccard uses 1. The {@link InvertedIndex} allocates one map per
     *         slot per length bucket.
     */
    int indexSlots();

    /**
     * @return the measure-specific "length" of a value — character count for edit-distance
     *         measures, token count for Jaccard. Used to bucket values.
     */
    int lengthOf(String value);

    /** True iff {@code a} and {@code b} are similar under this measure's configured threshold. */
    boolean isSimilar(String a, String b);

    /**
     * Cheap pre-filter: could a dep value of length {@code depLen} possibly match a ref value of
     * length {@code refLen}? Used by {@link CandidatePruner} on min/max column lengths.
     */
    boolean lengthFeasible(int depLen, int refLen);

    /** Inclusive lower bound on ref lengths that could match a dep value of this length. */
    int refLengthLowerBound(int depLen);

    /** Inclusive upper bound on ref lengths that could match a dep value of this length. */
    int refLengthUpperBound(int depLen);

    /**
     * @return true if the column is trivial under this measure and should be excluded.
     *         e.g. paper §3: in ED mode columns where every value has ≤ τ chars are "simple
     *         sINDs" and carry no real information.
     */
    boolean isColumnTrivial(LengthBucketedColumn column);

    /** Inverted-index entries for a single referenced value (one per slot). */
    List<IndexKey> indexKeys(String value);

    /**
     * Probe keys for a single dependent value. {@code targetRefLength} is the length of the
     * referenced bucket currently being probed — measures that index by segment position use it
     * to align substring offsets (PassJoin).
     */
    List<IndexKey> probeKeys(String value, int targetRefLength);
}
