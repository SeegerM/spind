package similarity;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.RelationMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads the CSV chunk temp files produced by {@link RelationMetadata#call()} and
 * groups each column's distinct values by length, producing one {@link LengthBucketedColumn}
 * per unary attribute.
 *
 * No new I/O path is introduced — the chunks already exist on disk after SPIND's standard
 * initialization phase. We just open them with a bare {@link CSVReader} configured exactly as
 * the writer was, so any source (CSV, Postgres, Mongo, Neo4j) flows through identically.
 */
public final class ColumnPreprocessor {

    private static final Logger logger = LoggerFactory.getLogger(ColumnPreprocessor.class);

    private ColumnPreprocessor() {}

    public static Map<Integer, LengthBucketedColumn> preprocess(
            RelationMetadata[] relations,
            Attribute[] attributes,
            SimilarityMeasure measure,
            Config config) throws IOException, CsvValidationException {

        Map<Integer, LengthBucketedColumn> result = new HashMap<>(attributes.length);

        // Initialise empty buckets for every unary attribute.
        for (Attribute attribute : attributes) {
            result.put(attribute.getId(), new LengthBucketedColumn(attribute.getId(), attribute.getRelationId()));
        }

        // One pass per relation reads each chunk once and dispatches columns to all owning attributes.
        for (RelationMetadata relation : relations) {
            int offset = relation.offset;
            int numColumns = relation.columnNames.length;

            for (Path chunkPath : relation.chunks) {
                try (CSVReader reader = openChunk(chunkPath, config)) {
                    String[] row;
                    while ((row = reader.readNext()) != null) {
                        if (row.length < numColumns) continue; // safety: ragged row
                        for (int col = 0; col < numColumns; col++) {
                            int attributeId = offset + col;
                            LengthBucketedColumn bucket = result.get(attributeId);
                            if (bucket == null) continue;
                            ingest(bucket, row[col], measure, config);
                        }
                    }
                }
            }
        }

        // Final measure-specific trivial flag (paper §3): mark columns whose content is fully
        // trivial under the chosen measure (e.g. all values ≤ τ chars in ED mode).
        for (LengthBucketedColumn col : result.values()) {
            if (!col.isTrivial() && measure.isColumnTrivial(col)) {
                col.markTrivial("trivial under " + measure.name());
            }
        }

        long nonEmpty = result.values().stream().filter(c -> !c.isEmpty()).count();
        logger.info("Preprocessed {} attributes ({} non-empty) for similarity discovery",
                result.size(), nonEmpty);
        return result;
    }

    private static void ingest(LengthBucketedColumn bucket, String rawValue, SimilarityMeasure measure, Config config) {
        if (rawValue == null || rawValue.equals(config.nullString)) {
            bucket.addNull();
            return;
        }
        // Paper §3: drop columns containing any value above the configured length cap. We mark
        // here and let CandidatePruner drop, rather than aborting mid-ingest, so totals stay
        // meaningful for logging.
        if (rawValue.length() > config.maxValueLength) {
            bucket.markTrivial("contains value longer than " + config.maxValueLength + " chars");
            return;
        }
        bucket.add(measure.lengthOf(rawValue), rawValue);
    }

    private static CSVReader openChunk(Path chunkPath, Config config) throws IOException {
        BufferedReader reader = Files.newBufferedReader(chunkPath);
        return new CSVReaderBuilder(reader)
                .withCSVParser(new CSVParserBuilder()
                        .withSeparator(config.separator)
                        .withQuoteChar(config.quoteChar)
                        // RelationMetadata writes chunks as RFC 4180: a backslash is data and an
                        // embedded quote is doubled. Leaving OpenCSV's default backslash escape in
                        // place makes a value ending in one swallow its closing delimiter, which
                        // is the same defect that silently truncated a relation on the exact path.
                        // Fixing that reader and not this one left the similarity path broken in
                        // the same way -- it fails loudly here only because a malformed line
                        // reaches the end of a chunk rather than the end of the file.
                        .withEscapeChar(ICSVParser.NULL_CHARACTER)
                        .build())
                .build();
    }
}
