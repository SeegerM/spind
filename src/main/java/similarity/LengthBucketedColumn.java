package similarity;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * A single column's distinct values grouped by their length (chars for ED/Hybrid, tokens for
 * Jaccard). Built once per attribute by {@link ColumnPreprocessor} and consumed by the
 * similarity validator.
 *
 * Length-bucketing is the central trick that makes SAWFISH's sliding length window cheap:
 * we only ever materialize inverted indexes for buckets within the active window.
 */
public class LengthBucketedColumn {

    private final int attributeId;
    private final int relationId;
    /** length → distinct values of that length. */
    private final NavigableMap<Integer, Set<String>> buckets = new TreeMap<>();
    private long totalValueCount;        // includes duplicates
    private long distinctValueCount;     // unique across all buckets
    private long nullValueCount;
    private int minLength = Integer.MAX_VALUE;
    private int maxLength = Integer.MIN_VALUE;
    private boolean trivial;             // marked by SimilarityMeasure.isColumnTrivial()
    private String trivialReason;

    public LengthBucketedColumn(int attributeId, int relationId) {
        this.attributeId = attributeId;
        this.relationId = relationId;
    }

    /** Add a single value (already null-checked). length is measure-specific. */
    public void add(int length, String value) {
        totalValueCount++;
        Set<String> bucket = buckets.computeIfAbsent(length, k -> new HashSet<>());
        if (bucket.add(value)) {
            distinctValueCount++;
            if (length < minLength) minLength = length;
            if (length > maxLength) maxLength = length;
        }
    }

    public void addNull() {
        totalValueCount++;
        nullValueCount++;
    }

    public int attributeId() { return attributeId; }
    public int relationId() { return relationId; }
    public long totalValueCount() { return totalValueCount; }
    public long distinctValueCount() { return distinctValueCount; }
    public long nullValueCount() { return nullValueCount; }
    public int minLength() { return distinctValueCount == 0 ? 0 : minLength; }
    public int maxLength() { return distinctValueCount == 0 ? 0 : maxLength; }
    public boolean isEmpty() { return distinctValueCount == 0; }
    public boolean isTrivial() { return trivial; }
    public String trivialReason() { return trivialReason; }

    public void markTrivial(String reason) {
        this.trivial = true;
        this.trivialReason = reason;
    }

    /** Distinct values at exactly the given length, or empty. */
    public Set<String> valuesAtLength(int length) {
        Set<String> bucket = buckets.get(length);
        return bucket == null ? Collections.emptySet() : bucket;
    }

    /** Lengths present in the column, in descending order. */
    public Iterable<Integer> lengthsDescending() {
        return buckets.descendingKeySet();
    }

    /** Lengths present within [loInclusive, hiInclusive]. */
    public Collection<Integer> lengthsInRange(int loInclusive, int hiInclusive) {
        return buckets.subMap(loInclusive, true, hiInclusive, true).keySet();
    }
}
