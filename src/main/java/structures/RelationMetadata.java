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
import java.util.List;
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

    public RelationMetadata(int relationId, int relationOffset, TableSource source, Config config, ConnectionRegistry registry) throws IOException, CsvValidationException {
        this.chunks = new ArrayList<>();
        this.config = config;
        this.id = relationId;
        this.offset = relationOffset;
        this.size = sizeOf(source);
        this.relationalInput = RelationalInputFactory.open(source, config, registry);
        this.columnNames = relationalInput.getHeader();
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

        while (relationalInput.hasNext()) {
            csvWriter.writeNext(relationalInput.next());
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
        logger.debug("Finished relation" + this.id + " (" + (System.currentTimeMillis() - sTime) + "ms)");
        return null;
    }

    @Override
    public int compareTo(RelationMetadata o) {
        return Long.compare(o.size, this.size);
    }
}
