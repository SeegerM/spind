package io;

import java.io.Closeable;
import java.io.IOException;

/**
 * Streams rows of a relation as String arrays during the initial chunking pass.
 *
 * The surface here is intentionally minimal — the attribute-combination logic used during
 * sort/merge runs against re-read CSV chunks and lives on {@link CsvRelationalInput}, not
 * here. Database-backed implementations (Postgres, Mongo, Neo4j) only need to deliver rows
 * for the initial pass; downstream phases read the chunked CSV temp files.
 */
public interface RelationalInput extends Closeable {

    /** @return true if {@link #next()} will return another row. */
    boolean hasNext();

    /** @return the values of the next row, or {@code null} if exhausted. */
    String[] next();

    /** @return the column names in row order. */
    String[] getHeader();

    @Override
    void close() throws IOException;
}
