package structures;

import java.nio.file.Path;

/**
 * Describes where a single table's rows come from. Implementations are immutable records that
 * carry only the addressing information; credentials are resolved separately via the
 * connection registry.
 */
public sealed interface TableSource {

    /** Human-readable name used in logs and result output. */
    String displayName();

    /** CSV (or otherwise delimited) file on disk. */
    record File(Path path, String displayName) implements TableSource {
        public File(Path path) {
            this(path, path.getFileName().toString().replaceFirst("[.][^.]+$", ""));
        }
    }

    /** A PostgreSQL table referenced by schema + name, using a named connection. */
    record Postgres(String connectionName, String schema, String table) implements TableSource {
        @Override
        public String displayName() {
            return schema == null || schema.isEmpty() ? table : schema + "." + table;
        }
    }

    /** A MongoDB collection (treated as a table; nested fields flattened). */
    record Mongo(String connectionName, String database, String collection) implements TableSource {
        @Override
        public String displayName() {
            return database + "." + collection;
        }
    }

    /** A Neo4j node label (treated as a table; properties become columns). */
    record Neo4jLabel(String connectionName, String database, String label) implements TableSource {
        @Override
        public String displayName() {
            return database == null || database.isEmpty() ? label : database + ":" + label;
        }
    }
}
