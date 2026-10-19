package structures;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Restricts discovery to a given list of candidate INDs (e.g. proposed foreign keys).
 *
 * <p>File format, one candidate per line, tab-separated:
 * {@code dependentTable  depCol1,depCol2  referencedTable  refCol1,refCol2} (columns in corresponding
 * order). Every unary component of a composite candidate is admitted too, because SPIND builds n-ary
 * candidates from valid unary ones. Only candidates in the list are validated, and only attributes that
 * take part in a remaining candidate are read and sorted.</p>
 *
 * <p>Enabled with {@code -Dspind.candidates=<file>}; without it SPIND is unchanged.</p>
 */
public final class CandidateRestriction {

    private final Set<String> allowed = new HashSet<>();
    private final String[] tableNames;
    private final String[][] columnNames;

    private CandidateRestriction(String[] tableNames, String[][] columnNames) {
        this.tableNames = tableNames;
        this.columnNames = columnNames;
    }

    public static CandidateRestriction load(String file, String[] tableNames, String[][] columnNames) throws IOException {
        CandidateRestriction r = new CandidateRestriction(tableNames, columnNames);
        for (String line : Files.readAllLines(Path.of(file), StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\t");
            String[] dep = f[1].split(",");
            String[] ref = f[3].split(",");
            if (dep.length != ref.length) {
                throw new IllegalArgumentException("column counts differ: " + line);
            }
            List<String> pairs = new ArrayList<>();
            for (int i = 0; i < dep.length; i++) {
                pairs.add(norm(dep[i]) + "=" + norm(ref[i]));
                r.allowed.add(key(norm(f[0]), norm(f[2]), List.of(norm(dep[i]) + "=" + norm(ref[i]))));
            }
            r.allowed.add(key(norm(f[0]), norm(f[2]), pairs));
        }
        return r;
    }

    public int size() {
        return allowed.size();
    }

    private static String norm(String s) {
        return s.trim().toLowerCase(Locale.ROOT);
    }

    private static String key(String depTable, String refTable, List<String> pairs) {
        List<String> p = new ArrayList<>(pairs);
        p.sort(null);
        return depTable + "|" + refTable + "|" + String.join(",", p);
    }

    private String key(Attribute dep, Attribute ref) {
        List<String> pairs = new ArrayList<>();
        for (int i = 0; i < dep.containedColumns.length; i++) {
            pairs.add(norm(columnNames[dep.getRelationId()][dep.containedColumns[i]]) + "="
                    + norm(columnNames[ref.getRelationId()][ref.containedColumns[i]]));
        }
        return key(norm(tableNames[dep.getRelationId()]), norm(tableNames[ref.getRelationId()]), pairs);
    }

    /** Removes every candidate not in the list; returns the number of candidates kept. */
    public long restrict(Attribute[] attributes) {
        long kept = 0;
        for (Attribute dep : attributes) {
            if (dep == null || dep.getReferenced() == null) {
                continue;
            }
            PINDList.PINDIterator it = dep.getReferenced().elementIterator();
            while (it.hasNext()) {
                int refId = it.next().id;
                if (!allowed.contains(key(dep, attributes[refId]))) {
                    it.remove();
                } else {
                    kept++;
                }
            }
            if (dep.getReferenced().isEmpty()) {
                dep.setReferenced(null);
            }
        }
        return kept;
    }

    /** Attributes that are the dependent or the referenced side of a remaining candidate. */
    public boolean[] involved(Attribute[] attributes) {
        boolean[] keep = new boolean[attributes.length];
        for (Attribute dep : attributes) {
            if (dep == null || dep.getReferenced() == null) {
                continue;
            }
            keep[dep.getId()] = true;
            PINDList.PINDIterator it = dep.getReferenced().elementIterator();
            while (it.hasNext()) {
                keep[it.next().id] = true;
            }
        }
        return keep;
    }
}
