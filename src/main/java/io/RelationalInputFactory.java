package io;

import com.opencsv.exceptions.CsvValidationException;
import runner.Config;
import runner.ConnectionRegistry;
import structures.TableSource;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Dispatches a {@link TableSource} to the appropriate {@link RelationalInput} implementation.
 * Also computes a best-effort size estimate used only for chunking sort order.
 */
public final class RelationalInputFactory {

    private RelationalInputFactory() {}

    public static RelationalInput open(TableSource source, Config config, ConnectionRegistry registry)
            throws IOException, CsvValidationException {
        return switch (source) {
            case TableSource.File file -> new CsvRelationalInput(file.path(), config);
            case TableSource.Postgres pg -> openPostgres(pg, config, registry);
            case TableSource.Mongo mongo -> new MongoRelationalInput(
                    registry.getMongo(mongo.connectionName()), mongo.database(), mongo.collection(), config);
            case TableSource.Neo4jLabel neo -> new Neo4jRelationalInput(
                    registry.getNeo4j(neo.connectionName()), neo.database(), neo.label(), config);
        };
    }

    private static RelationalInput openPostgres(TableSource.Postgres pg, Config config, ConnectionRegistry registry)
            throws IOException {
        try {
            return new PostgresRelationalInput(registry.getPostgres(pg.connectionName()), pg.schema(), pg.table(), config);
        } catch (SQLException e) {
            throw new IOException("Failed to open Postgres source " + pg.displayName(), e);
        }
    }
}
