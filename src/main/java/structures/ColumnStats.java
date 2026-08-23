package structures;

import java.util.Arrays;

/**
 * Per-column value statistics collected during the chunking pass, indexed by global attribute id.
 *
 * Chunking already touches every value of every relation, so gathering counts there is close to
 * free — no additional pass over the data is introduced.
 *
 * {@link #totalValues} and {@link #nullValues} are always exact and cost no memory. Distinct
 * counts do cost memory (one hash set per column), so they are collected under a per-relation
 * budget: once a relation's columns jointly retain more than {@code statsValueBudget} distinct
 * values, distinct tracking for that relation is abandoned and its counts are reported as
 * {@link #UNKNOWN}. Consumers must treat UNKNOWN as "no information" and fall back to doing the
 * full work — never as zero.
 */
public class ColumnStats {

    public static final long UNKNOWN = -1L;

    private final long[] totalValues;
    private final long[] nullValues;
    private final long[] distinctValues;

    public ColumnStats(int attributeCount) {
        this.totalValues = new long[attributeCount];
        this.nullValues = new long[attributeCount];
        this.distinctValues = new long[attributeCount];
        Arrays.fill(this.distinctValues, UNKNOWN);
    }

    /**
     * Copies one relation's locally collected counters into the global, attribute-indexed arrays.
     *
     * @param offset     the relation's attribute id offset
     * @param total      non-null value count per column
     * @param nulls      null count per column
     * @param distinct   distinct non-null value count per column, or {@link #UNKNOWN}
     */
    public void ingest(int offset, long[] total, long[] nulls, long[] distinct) {
        System.arraycopy(total, 0, totalValues, offset, total.length);
        System.arraycopy(nulls, 0, nullValues, offset, nulls.length);
        System.arraycopy(distinct, 0, distinctValues, offset, distinct.length);
    }

    /** Exact number of non-null values in the column. */
    public long totalValues(int attributeId) {
        return totalValues[attributeId];
    }

    /** Exact number of null values in the column. */
    public long nullValues(int attributeId) {
        return nullValues[attributeId];
    }

    /** Distinct non-null value count, or {@link #UNKNOWN} if the column exceeded the budget. */
    public long distinctValues(int attributeId) {
        return distinctValues[attributeId];
    }

    public boolean hasDistinct(int attributeId) {
        return distinctValues[attributeId] != UNKNOWN;
    }

    /** How many columns carry an exact distinct count — reported so a run's pruning power is auditable. */
    public int countedColumns() {
        int counted = 0;
        for (long distinct : distinctValues) {
            if (distinct != UNKNOWN) counted++;
        }
        return counted;
    }
}
