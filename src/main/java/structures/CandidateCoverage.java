package structures;

/**
 * Covered dependent occurrences per surviving candidate, carried across partitions.
 *
 * <p>Whole-dataset validation can keep coverage in a dense scratch array indexed by attribute id
 * and reuse it per dependent, because a dependent's values are all seen in one pass. Partitioned
 * validation sees only a slice of each dependent per partition, so coverage has to persist — and
 * a dense {@code M x M} matrix would be 2.8 GB on WebTables before a single value is read.</p>
 *
 * <p>Instead the counters are keyed to the candidates that actually exist: one slot per surviving
 * (dependent, referenced) edge, laid out parallel to the dependent's candidate array. Memory is
 * O(E), not O(M²). Nothing here needs a reverse attribute-id lookup, because every access walks a
 * dependent's own candidate list in order.</p>
 */
public final class CandidateCoverage {

    private static final int REMOVED = -1;

    private final int[][] referencedIds;
    private final long[][] covered;
    private final int[] liveCount;

    /** Snapshots the current candidate sets. Later removals are recorded here, not in the lists. */
    public CandidateCoverage(Attribute[] attributes) {
        this.referencedIds = new int[attributes.length][];
        this.covered = new long[attributes.length][];
        this.liveCount = new int[attributes.length];

        for (int dependantId = 0; dependantId < attributes.length; dependantId++) {
            PINDList referenced = attributes[dependantId].getReferenced();
            if (referenced == null || referenced.isEmpty()) {
                continue;
            }
            int[] ids = new int[referenced.size()];
            int index = 0;
            PINDList.PINDIterator iterator = referenced.elementIterator();
            while (iterator.hasNext()) {
                ids[index++] = iterator.next().id;
            }
            referencedIds[dependantId] = ids;
            covered[dependantId] = new long[ids.length];
            liveCount[dependantId] = ids.length;
        }
    }

    public boolean hasCandidates(int dependantId) {
        return referencedIds[dependantId] != null && liveCount[dependantId] > 0;
    }

    public int slots(int dependantId) {
        return referencedIds[dependantId] == null ? 0 : referencedIds[dependantId].length;
    }

    /** Referenced attribute id at a slot, or -1 once the candidate has been ruled out. */
    public int referencedId(int dependantId, int slot) {
        return referencedIds[dependantId][slot];
    }

    public long covered(int dependantId, int slot) {
        return covered[dependantId][slot];
    }

    public void addCoverage(int dependantId, int slot, long amount) {
        covered[dependantId][slot] += amount;
    }

    public void remove(int dependantId, int slot) {
        referencedIds[dependantId][slot] = REMOVED;
        liveCount[dependantId]--;
    }

    public int liveCount(int dependantId) {
        return liveCount[dependantId];
    }

    /**
     * Writes the outcome back onto the candidate lists: candidates removed here are dropped, and
     * survivors get their exact violation count, so downstream reporting of the partial degree is
     * unchanged.
     *
     * @param dependantSize total dependent occurrences (or distinct values, duplicate-unaware)
     */
    public void applyTo(int dependantId, Attribute[] attributes, long dependantSize) {
        PINDList referenced = attributes[dependantId].getReferenced();
        if (referenced == null) {
            return;
        }
        int[] ids = referencedIds[dependantId];
        if (ids == null) {
            return;
        }
        // The list and the snapshot are still in the same order — nothing removes from the list
        // while partitioned validation runs — so a single walk lines them up.
        int slot = 0;
        PINDList.PINDIterator iterator = referenced.elementIterator();
        while (iterator.hasNext() && slot < ids.length) {
            PINDList.PINDElement element = iterator.next();
            if (ids[slot] == REMOVED) {
                iterator.remove();
            } else {
                long violations = dependantSize - covered[dependantId][slot];
                if (violations > 0) {
                    element.violate(violations);
                }
            }
            slot++;
        }
        if (referenced.isEmpty()) {
            attributes[dependantId].setReferenced(null);
        }
    }
}
