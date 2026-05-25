package similarity;

import java.util.Arrays;

/**
 * Bounded Levenshtein edit distance with early abort.
 *
 * Uses banded dynamic programming: only the (2τ+1)-wide diagonal band is computed, which
 * makes runtime O(τ·min(|a|,|b|)) instead of O(|a|·|b|). When the entire current row exceeds
 * τ, we know the final distance must be > τ and return τ+1 immediately. This matches the
 * "tighter bounds" optimization referenced in the SAWFISH paper §4.4.1 (Li et al.).
 *
 * The exact distance is only meaningful when the return value ≤ τ. A return of τ+1 means
 * "definitely greater than τ" — the caller should treat it as a non-match without further
 * inspection.
 */
public final class EditDistance {

    private EditDistance() {}

    /**
     * @param a   first string
     * @param b   second string
     * @param tau maximum allowed edit distance (≥ 0)
     * @return the edit distance if ≤ τ, else τ+1
     */
    public static int computeWithLimit(String a, String b, int tau) {
        if (tau < 0) throw new IllegalArgumentException("tau must be non-negative");
        int n = a.length();
        int m = b.length();
        if (Math.abs(n - m) > tau) return tau + 1;
        if (n == 0) return Math.min(m, tau + 1);
        if (m == 0) return Math.min(n, tau + 1);

        // Make sure a is the longer string for slightly more predictable band math (optional).
        int sentinel = tau + 1;

        int[] prev = new int[m + 1];
        int[] curr = new int[m + 1];
        Arrays.fill(prev, sentinel);
        Arrays.fill(curr, sentinel);

        // Row 0: distance from empty prefix of `a` to first j chars of `b` is j (for j ≤ τ).
        for (int j = 0; j <= Math.min(m, tau); j++) {
            prev[j] = j;
        }

        for (int i = 1; i <= n; i++) {
            int from = Math.max(1, i - tau);
            int to = Math.min(m, i + tau);

            Arrays.fill(curr, sentinel);
            // First column is only valid while i ≤ τ (deleting i chars from a).
            if (i <= tau) curr[0] = i;

            int rowMin = curr[0];
            for (int j = from; j <= to; j++) {
                int cost = (a.charAt(i - 1) == b.charAt(j - 1)) ? 0 : 1;
                int del = prev[j] + 1;       // delete a[i-1]
                int ins = curr[j - 1] + 1;   // insert b[j-1]
                int sub = prev[j - 1] + cost;
                int best = Math.min(del, Math.min(ins, sub));
                if (best > sentinel) best = sentinel;
                curr[j] = best;
                if (best < rowMin) rowMin = best;
            }

            if (rowMin > tau) return sentinel;

            int[] tmp = prev;
            prev = curr;
            curr = tmp;
        }

        int result = prev[m];
        return result > tau ? sentinel : result;
    }

    /** Convenience wrapper: true iff the distance between a and b is ≤ τ. */
    public static boolean withinThreshold(String a, String b, int tau) {
        return computeWithLimit(a, b, tau) <= tau;
    }
}
