package structures;

import java.util.Arrays;

/**
 * The set of attributes sharing one value during validation, held as parallel primitive arrays.
 *
 * The baseline path concatenates the {@code "id,occurrences;"} fragments of every relation into a
 * StringBuilder and later splits that string back apart into a {@code HashMap<Integer, Long>}.
 * That happens once per distinct value in the entire dataset — the innermost loop of the whole
 * algorithm — and costs a string copy, an array of substrings, and two boxed objects per
 * attribute. This class parses the fragments straight into {@code int[]}/{@code long[]} instead,
 * so the same information carries no allocation beyond the two arrays.
 *
 * <p>Ids are appended without a duplicate check. That is safe because each attribute belongs to
 * exactly one relation, every relation contributes at most one entry per value (its sorted stream
 * is deduplicated by the merge), and so no id can appear twice in a group. This matches the
 * baseline, which likewise let a later entry overwrite an earlier one for the same id.</p>
 */
public final class AttributeGroup {

    private static final int INITIAL_CAPACITY = 8;

    private int[] ids = new int[INITIAL_CAPACITY];
    private long[] occurrences = new long[INITIAL_CAPACITY];
    private int size;

    public int size() {
        return size;
    }

    public int id(int index) {
        return ids[index];
    }

    public long occurrences(int index) {
        return occurrences[index];
    }

    /**
     * Parses a {@code "id,occurrences;id,occurrences;"} fragment and appends its pairs.
     *
     * Hand-rolled rather than {@code split}: the serialized form is machine-written and always
     * well-formed, so a single scan without intermediate strings suffices. Malformed or empty
     * fragments are skipped, matching the baseline's {@code length != 2} guard.
     */
    public void addSerialized(String serialized) {
        if (serialized == null || serialized.isEmpty()) {
            return;
        }
        int index = 0;
        int length = serialized.length();
        while (index < length) {
            int separator = serialized.indexOf(',', index);
            if (separator < 0) break;
            int terminator = serialized.indexOf(';', separator + 1);
            if (terminator < 0) terminator = length;

            int id = parseInt(serialized, index, separator);
            long occurrence = parseLong(serialized, separator + 1, terminator);
            if (id >= 0 && occurrence >= 0) {
                add(id, occurrence);
            }
            index = terminator + 1;
        }
    }

    private void add(int id, long occurrence) {
        if (size == ids.length) {
            ids = Arrays.copyOf(ids, size * 2);
            occurrences = Arrays.copyOf(occurrences, size * 2);
        }
        ids[size] = id;
        occurrences[size] = occurrence;
        size++;
    }

    /** Keeps only the entries at the given indices, in order. Used to drop irrelevant attributes. */
    public void retain(int newSize) {
        this.size = newSize;
    }

    public void set(int index, int id, long occurrence) {
        ids[index] = id;
        occurrences[index] = occurrence;
    }

    private static int parseInt(String source, int from, int to) {
        if (from >= to) return -1;
        int value = 0;
        for (int i = from; i < to; i++) {
            char c = source.charAt(i);
            if (c < '0' || c > '9') return -1;
            value = value * 10 + (c - '0');
        }
        return value;
    }

    private static long parseLong(String source, int from, int to) {
        if (from >= to) return -1;
        long value = 0;
        for (int i = from; i < to; i++) {
            char c = source.charAt(i);
            if (c < '0' || c > '9') return -1;
            value = value * 10 + (c - '0');
        }
        return value;
    }
}
