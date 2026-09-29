package io;

import runner.Config;
import runner.ConnectionRegistry;
import structures.Attribute;
import structures.RelationMetadata;
import structures.PINDList;
import structures.SortJob;
import structures.TableSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * Dependency-free regression checks for the CSV boundary used by multi-model exports.
 *
 * <p>This is a plain main rather than a JUnit test because SPIND intentionally has no test
 * framework dependency.  Maven compiles it during {@code test-compile}; any failed assertion exits
 * non-zero when the class is run.</p>
 */
public final class CsvRelationalInputRegressionTest {

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("spind-csv-regression-");
        roundTripRfcCsv(directory);
        rejectMalformedSuffix(directory);
        reportCoverageFromDependentSize();
        System.out.println("CSV regression checks passed");
    }

    private static void reportCoverageFromDependentSize() {
        PINDList candidates = new PINDList();
        candidates.add(1);
        candidates.setViolationBudget(34, 69);
        PINDList.PINDElement candidate = candidates.elementIterator().next();
        candidate.violate(34);
        double actual = Output.getPartialDegree(new Config(0.5), candidate);
        double expected = 35.0 / 69.0;
        if (Math.abs(actual - expected) > 1e-12) {
            throw new AssertionError("expected exact coverage " + expected + " but got " + actual);
        }
    }

    private static void roundTripRfcCsv(Path directory) throws Exception {
        Path source = directory.resolve("source.csv");
        Files.writeString(source, """
                id,title
                1,"ends in \\"
                2,"a ""quoted"" title"
                """);

        Config config = config(directory);
        RelationMetadata relation = new RelationMetadata(
                0, 0, new TableSource.File(source, "fixture"), config,
                new ConnectionRegistry(new Properties()));
        relation.call();

        List<Attribute> attributes = List.of(
                new Attribute(0, 0, new int[]{0}),
                new Attribute(1, 0, new int[]{1}));
        SortJob chunk = new SortJob(relation.chunks.getFirst(), attributes, 0, 10, 10,
                config, null, 1);

        try (CsvRelationalInput input = new CsvRelationalInput(chunk, config)) {
            assertRow(input.next(), "1", "ends in \\");
            assertRow(input.next(), "2", "a \"quoted\" title");
            if (input.hasNext()) {
                throw new AssertionError("unexpected third row after RFC CSV round-trip");
            }
        }
    }

    private static void rejectMalformedSuffix(Path directory) throws Exception {
        Path source = directory.resolve("malformed.csv");
        Files.writeString(source, "id,title\n1,valid\n2,\"unterminated\n");
        Config config = config(directory);

        try (CsvRelationalInput input = new CsvRelationalInput(source, config)) {
            try {
                input.next();
                throw new AssertionError("malformed CSV was treated as end-of-input");
            } catch (IllegalStateException expected) {
                if (!expected.getMessage().contains("Could not parse CSV input")) {
                    throw expected;
                }
            }
        }
    }

    private static Config config(Path directory) {
        Config config = new Config(0.5);
        config.tempFolder = directory.toString();
        config.inputFileHasHeader = true;
        config.nullString = "";
        config.fileEscape = '\0'; // MetaLevelExporter emits RFC 4180, not backslash-escaped CSV.
        return config;
    }

    private static void assertRow(String[] row, String... expected) {
        if (!java.util.Arrays.equals(row, expected)) {
            throw new AssertionError("expected " + java.util.Arrays.toString(expected)
                    + " but got " + java.util.Arrays.toString(row));
        }
    }
}
