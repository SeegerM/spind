package io;

import java.util.Arrays;

/**
 * The attributes one value occurs in, with per-attribute occurrence counts.
 *
 * <p>Replaces the {@code HashMap<Integer, Long>} the original sorter kept per distinct value. That
 * inner map is allocated once for every distinct value in the dataset — tens of millions of them on
 * TPC-H 5 — and almost always holds a single-digit number of entries, so a hash table plus boxed
 * keys and values is far more machinery than the job needs. Two small primitive arrays cost one
 * allocation and no boxing.</p>
 *
 * <p>Lookup is a backward linear scan. Entries hold as many attributes as contain the value, which
 * is small in practice, and consecutive rows of a chunk hit the same attribute, so the match is
 * usually the last slot.</p>
 */
final class ValueEntry {

    private static final int INITIAL_CAPACITY = 2;

    int[] ids = new int[INITIAL_CAPACITY];
    long[] occurrences = new long[INITIAL_CAPACITY];
    int size;

    /**
     * Adds one occurrence of the value in an attribute.
     *
     * @return true if this is the first occurrence of the value in that attribute
     */
    boolean add(int attributeId) {
        return add(attributeId, 1L);
    }

    /**
     * Adds {@code count} occurrences of the value in an attribute.
     *
     * @return true if this is the first occurrence of the value in that attribute
     */
    boolean add(int attributeId, long count) {
        for (int i = size - 1; i >= 0; i--) {
            if (ids[i] == attributeId) {
                occurrences[i] += count;
                return false;
            }
        }
        if (size == ids.length) {
            ids = Arrays.copyOf(ids, size * 2);
            occurrences = Arrays.copyOf(occurrences, size * 2);
        }
        ids[size] = attributeId;
        occurrences[size] = count;
        size++;
        return true;
    }

    int[] trimmedIds() {
        return Arrays.copyOf(ids, size);
    }
}
