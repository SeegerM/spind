package similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-referenced-column inverted index, materialized lazily one length bucket at a time.
 *
 * Layout: {@code length → slot → key → list of parent values}. Lookups join on
 * (length, slot, key) and return the candidate ref-side values to validate via the measure's
 * full {@code isSimilar()} check.
 *
 * Lifetime: built lazily by {@link #ensureLengthLoaded(int)}; old buckets are discarded by
 * {@link #evictLengthsOutsideWindow(int, int)} as the sliding length window in
 * {@link SimilarityValidator} moves on.
 */
public class InvertedIndex {

    private final SimilarityMeasure measure;
    private final LengthBucketedColumn refColumn;
    private final int slots;
    private final Map<Integer, Map<String, List<String>>[]> byLength = new HashMap<>();

    public InvertedIndex(SimilarityMeasure measure, LengthBucketedColumn refColumn) {
        this.measure = measure;
        this.refColumn = refColumn;
        this.slots = measure.indexSlots();
    }

    public void ensureLengthLoaded(int length) {
        if (byLength.containsKey(length)) return;
        if (refColumn.valuesAtLength(length).isEmpty()) {
            byLength.put(length, emptyBuckets());
            return;
        }
        byLength.put(length, buildForLength(length));
    }

    public void evictLengthsOutsideWindow(int loInclusive, int hiInclusive) {
        byLength.keySet().removeIf(l -> l < loInclusive || l > hiInclusive);
    }

    /** Look up matching ref parent values at (length, slot, key). Returns empty if no entry. */
    public List<String> lookup(int length, int slot, String key) {
        Map<String, List<String>>[] arr = byLength.get(length);
        if (arr == null || slot < 0 || slot >= arr.length) return List.of();
        Map<String, List<String>> map = arr[slot];
        if (map == null) return List.of();
        List<String> parents = map.get(key);
        return parents == null ? List.of() : parents;
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>>[] emptyBuckets() {
        Map<String, List<String>>[] arr = new Map[slots];
        for (int i = 0; i < slots; i++) arr[i] = new HashMap<>();
        return arr;
    }

    private Map<String, List<String>>[] buildForLength(int length) {
        Map<String, List<String>>[] arr = emptyBuckets();
        for (String value : refColumn.valuesAtLength(length)) {
            for (IndexKey ik : measure.indexKeys(value)) {
                int slot = ik.slot();
                if (slot < 0 || slot >= slots) continue; // defensive
                arr[slot].computeIfAbsent(ik.key(), k -> new ArrayList<>()).add(value);
            }
        }
        return arr;
    }
}
