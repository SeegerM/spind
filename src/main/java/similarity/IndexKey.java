package similarity;

/**
 * An entry placed into (or used to probe) a {@link InvertedIndex}.
 *
 * For Levenshtein/PassJoin, {@code slot} is the segment position 0..τ.
 * For Jaccard, {@code slot} is always 0 (single token namespace per length).
 */
public record IndexKey(int slot, String key) {
}
