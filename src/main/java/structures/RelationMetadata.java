package structures;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import io.RelationalInput;
import io.RelationalInputFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import runner.ConnectionRegistry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Relational Metadata includes the relations name, the column names and the paths to the relation chunks. All of these
 * attributes a loaded/constructed once, before the actual algorithm starts. The Metadata is kept in main memory the
 * whole time and is required to load each layer and for the creation of a human-readable output.
 */
public class RelationMetadata implements Callable<Void>, Comparable<RelationMetadata> {

    public final List<Path> chunks;
    public final int id;
    public final int offset;
    public final RelationalInput relationalInput;
    private final Config config;
    private final Logger logger = LoggerFactory.getLogger(RelationMetadata.class);
    private final long size;
    public String[] columnNames;
    public List<Attribute> connectedAttributes;

    /**
     * Whether chunk files were written for this relation. When false the relation was left
     * unchunked and must be read from its source instead.
     */
    public boolean chunked = true;

    /** Per-column counters filled by {@link #call()} when {@link Config#collectColumnStats} is on. */
    public long[] columnTotalValues;
    public long[] columnNullValues;
    public long[] columnDistinctValues;

    public RelationMetadata(int relationId, int relationOffset, TableSource source, Config config, ConnectionRegistry registry) throws IOException, CsvValidationException {
        this.chunks = new ArrayList<>();
        this.config = config;
        this.id = relationId;
        this.offset = relationOffset;
        this.size = sizeOf(source);
        this.relationalInput = RelationalInputFactory.open(source, config, registry);
        this.columnNames = relationalInput.getHeader();
    }

    /** Raw input size in bytes, or 0 for sources that cannot report one cheaply. */
    public long inputSizeBytes() {
        return size;
    }

    private static long sizeOf(TableSource source) throws IOException {
        if (source instanceof TableSource.File file) {
            return Files.size(file.path());
        }
        // DB-backed sources: no cheap size signal — fall back to 0 (chunking order becomes insertion order).
        return 0L;
    }

    /**
     * Splits the input horizontally into chunks. Splitting and merging the chunks enables more multi-threading
     * opportunities.
     */
    @Override
    public Void call() throws Exception {
        long sTime = System.currentTimeMillis();
        int maxSize = Math.max(10, config.CHUNK_SIZE / relationalInput.getHeader().length);
        int chunkNum = 0;
        Path chunkPath = Path.of(config.tempFolder + File.separator + "r_" + id + "_c_" + chunkNum + ".txt");
        BufferedWriter chunkWriter = Files.newBufferedWriter(chunkPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        ICSVWriter csvWriter = new CSVWriterBuilder(chunkWriter).withSeparator(config.separator).withQuoteChar(config.quoteChar).withEscapeChar(config.fileEscape).build();
        chunks.add(chunkPath);
        int chunkSize = 0;

        StatsCollector stats = config.collectColumnStats ? new StatsCollector(columnNames.length, config) : null;

        while (relationalInput.hasNext()) {
            String[] row = relationalInput.next();
            if (stats != null) stats.observe(row);
            csvWriter.writeNext(row);
            if (++chunkSize >= maxSize) {
                chunkWriter.close();

                // open next chunk writer if there are still lines left
                if (relationalInput.hasNext()) {
                    chunkNum++;
                    chunkSize = 0;
                    chunkPath = Path.of(config.tempFolder + File.separator + "r_" + id + "_c_" + chunkNum + ".txt");
                    chunkWriter = Files.newBufferedWriter(chunkPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                    csvWriter = new CSVWriterBuilder(chunkWriter).withSeparator(config.separator).withQuoteChar(config.quoteChar).withEscapeChar(config.fileEscape).build();
                    chunks.add(chunkPath);
                }
            }
        }
        chunkWriter.close();
        relationalInput.close();
        if (stats != null) stats.publishTo(this);
        logger.debug("Finished relation" + this.id + " (" + (System.currentTimeMillis() - sTime) + "ms)");
        return null;
    }

    /**
     * Accumulates per-column counts for one relation while its rows stream past.
     *
     * Distinct tracking is abandoned wholesale for the relation once the shared budget is
     * exhausted, rather than per column: a partial picture across columns invites the mistake of
     * reading "few distinct values" off a column whose set was simply dropped early.
     */
    private static final class StatsCollector {

        private final Config config;
        private final long[] total;
        private final long[] nulls;
        private final Set<String>[] distinct;
        private long retained;
        private boolean overBudget;

        @SuppressWarnings("unchecked")
        StatsCollector(int columns, Config config) {
            this.config = config;
            this.total = new long[columns];
            this.nulls = new long[columns];
            this.distinct = new Set[columns];
            for (int i = 0; i < columns; i++) {
                this.distinct[i] = new HashSet<>();
            }
        }

        void observe(String[] row) {
            // Once the budget is gone the relation can no longer contribute a distinct count, and
            // a distinct count is the only thing the pruning bound can use. Counting the remaining
            // rows would be pure overhead — and on a multi-GB relation that overhead is a
            // per-cell string comparison over billions of cells, which measurably outweighs the
            // pruning it can never enable. Bail out entirely instead.
            if (overBudget) {
                return;
            }
            int columns = Math.min(row.length, total.length);
            for (int column = 0; column < columns; column++) {
                String value = row[column];
                // Mirrors CsvRelationalInput's chunk-reading null rule: the configured null
                // string becomes an actual null everywhere except EQUALITY mode, where all
                // nulls are deliberately treated as one ordinary value.
                boolean isNull = value == null
                        || (value.equals(config.nullString) && config.nullHandling != Config.NullHandling.EQUALITY);
                if (isNull) {
                    nulls[column]++;
                    continue;
                }
                total[column]++;
                if (distinct[column].add(value) && ++retained > config.statsValueBudget) {
                    overBudget = true;
                    Arrays.fill(distinct, null); // release the sets immediately
                    return; // the counts for this row are already incomplete; abandon them
                }
            }
        }

        /**
         * Publishes the counters, or nothing but UNKNOWN if collection was abandoned. Abandoning
         * stops mid-relation, so the totals gathered up to that point describe a prefix of the
         * data, not the relation — reporting them as if they were complete would silently corrupt
         * every violation budget derived from them.
         */
        void publishTo(RelationMetadata relation) {
            relation.columnTotalValues = new long[total.length];
            relation.columnNullValues = new long[total.length];
            relation.columnDistinctValues = new long[total.length];
            for (int column = 0; column < total.length; column++) {
                relation.columnTotalValues[column] = overBudget ? ColumnStats.UNKNOWN : total[column];
                relation.columnNullValues[column] = overBudget ? ColumnStats.UNKNOWN : nulls[column];
                relation.columnDistinctValues[column] = overBudget ? ColumnStats.UNKNOWN : distinct[column].size();
            }
        }
    }

    @Override
    public int compareTo(RelationMetadata o) {
        return Long.compare(o.size, this.size);
    }
}
