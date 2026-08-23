package structures;

import java.util.Collection;
import java.util.NoSuchElementException;

/**
 * The set of referenced attributes still possible for one dependent attribute, with the violations
 * charged to each so far.
 *
 * <p>Stored as two parallel primitive arrays rather than a linked list of objects. On
 * candidate-heavy inputs this is the dominant allocation in the whole algorithm — WebTables opens
 * with roughly 348 million candidate pairs — and one node per pair costs an object header, an int,
 * two longs and a pointer, about 44 bytes, versus 12 bytes here. Just as importantly it is one
 * allocation per dependent attribute instead of one per candidate, which is what actually shows up
 * in the profile.</p>
 *
 * <p>The violation budget moved from the element to the list. Every element of a list always
 * carried the identical cap — it is derived from the dependent attribute alone — so storing it per
 * element was pure duplication, another 8 bytes per candidate.</p>
 *
 * <p><b>Iteration contract.</b> {@link PINDIterator#next()} returns a flyweight positioned on the
 * current entry, not a fresh object, so a returned {@link PINDElement} is only valid until the next
 * call to {@code next()}. Every caller uses it strictly inside the loop body, which is what makes
 * the zero-allocation iteration possible. Do not retain one.</p>
 */
public class PINDList {

    private static final int TOMBSTONE = -1;
    private static final int INITIAL_CAPACITY = 8;

    private int[] ids = new int[0];
    private long[] violations = new long[0];
    private long violationCap;

    /** Slots written so far, tombstones included. */
    private int used;
    /** Live entries. */
    private int size;
    /** Removed entries still occupying a slot. */
    private int dead;

    public PINDList() {
    }

    public PINDList(Collection<Integer> seed, int except) {
        ids = new int[Math.max(INITIAL_CAPACITY, seed.size())];
        violations = new long[ids.length];
        for (int value : seed) {
            if (value != except) {
                add(value);
            }
        }
    }

    public int size() {
        return size;
    }

    /**
     * Check if there are still items remaining.
     *
     * @return True if there is no item in the list, False otherwise
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /** The violation budget shared by every candidate in this list. */
    public long violationCap() {
        return violationCap;
    }

    public void setViolationCap(long violationCap) {
        this.violationCap = violationCap;
    }

    public void add(int value) {
        if (used == ids.length) {
            grow();
        }
        ids[used] = value;
        violations[used] = 0L;
        used++;
        size++;
    }

    /**
     * @param violationsLeft the shared budget for this list; callers pass the same value for every
     *                       entry, and it is normally overwritten by
     *                       {@link Candidates#calculateViolations(Attribute[])} before use.
     */
    public void add(int value, long violationsLeft) {
        setViolationCap(violationsLeft);
        add(value);
    }

    /**
     * Use this function to iterate over the list and conditionally remove items if necessary.
     *
     * @return an PINDIterator which yields all PINDElements in the list one after another.
     */
    public PINDIterator elementIterator() {
        // Removals leave tombstones so they stay O(1) and preserve order, but the streaming
        // validator re-iterates a dependent's whole list once per value group, so every dead slot
        // is re-walked thousands of times before it would ever be reclaimed. Compacting at a
        // quarter dead keeps the walks short while staying amortized O(1) per removal, since a
        // compaction costs O(used) and only follows used/4 removals.
        if (dead > 16 && dead * 4 > used) {
            compact();
        }
        return new PINDIterator();
    }

    private void compact() {
        int write = 0;
        for (int read = 0; read < used; read++) {
            if (ids[read] != TOMBSTONE) {
                ids[write] = ids[read];
                violations[write] = violations[read];
                write++;
            }
        }
        used = write;
        dead = 0;
    }

    private void grow() {
        int capacity = ids.length == 0 ? INITIAL_CAPACITY : ids.length * 2;
        int[] grownIds = new int[capacity];
        long[] grownViolations = new long[capacity];
        System.arraycopy(ids, 0, grownIds, 0, used);
        System.arraycopy(violations, 0, grownViolations, 0, used);
        ids = grownIds;
        violations = grownViolations;
    }

    /**
     * A view onto one entry of the enclosing list. Reused across iteration steps — see the class
     * comment.
     */
    public class PINDElement {

        /** The referenced attribute's id. Refreshed by each {@link PINDIterator#next()}. */
        public int id;

        private int index;

        /**
         * Use this method to reduce the open violations by some amount
         *
         * @param occurrences The number of occurrences which should be subtracted from the open violations.
         * @return the remaining violations
         */
        public long violate(long occurrences) {
            violations[index] += occurrences;
            return violationCap - violations[index];
        }

        public long getViolations() {
            return violations[index];
        }

        public long violationCap() {
            return violationCap;
        }

        /**
         * Clears the accumulated violations so the candidate can be charged again from scratch.
         * Used after the progressive-sampling pass, whose violations must not be double counted
         * when the same candidate is re-validated against the full data.
         */
        public void resetViolations() {
            violations[index] = 0L;
        }
    }

    public class PINDIterator {

        private final PINDElement element = new PINDElement();
        private int cursor;
        private int current = -1;

        public boolean hasNext() {
            while (cursor < used && ids[cursor] == TOMBSTONE) {
                cursor++;
            }
            return cursor < used;
        }

        public PINDElement next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            current = cursor;
            cursor++;
            element.index = current;
            element.id = ids[current];
            return element;
        }

        public void remove() {
            ids[current] = TOMBSTONE;
            size--;
            dead++;
        }
    }
}
