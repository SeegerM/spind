package io;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import runner.Config;
import runner.ConnectionRegistry.Neo4jConnection;

import java.io.IOException;
import java.util.Map;
import java.util.TreeSet;

/**
 * Streams nodes of a single Neo4j label as rows. Node properties become columns.
 *
 * Header is discovered via {@code db.schema.nodeTypeProperties()}. Missing properties on a
 * node are emitted as the configured null string. Property values are stringified.
 */
public class Neo4jRelationalInput implements RelationalInput {

    private final Config config;
    private final Driver driver;
    private final Session session;
    private final Result result;
    private final String[] header;

    private Record nextRecord;

    public Neo4jRelationalInput(Neo4jConnection conn, String databaseName, String label, Config config) {
        this.config = config;
        this.driver = GraphDatabase.driver(conn.uri(), AuthTokens.basic(conn.user(), conn.password()));

        SessionConfig sessionConfig = (databaseName == null || databaseName.isEmpty())
                ? SessionConfig.defaultConfig()
                : SessionConfig.forDatabase(databaseName);
        this.session = driver.session(sessionConfig);

        this.header = discoverHeader(label);
        this.result = session.run("MATCH (n:`" + label.replace("`", "``") + "`) RETURN n");
        advance();
    }

    private String[] discoverHeader(String label) {
        TreeSet<String> propertyNames = new TreeSet<>();
        Result schemaResult = session.run(
                "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName " +
                        "WHERE $label IN nodeLabels AND propertyName IS NOT NULL " +
                        "RETURN DISTINCT propertyName",
                Map.of("label", label));
        while (schemaResult.hasNext()) {
            propertyNames.add(schemaResult.next().get("propertyName").asString());
        }
        // Fallback: if the schema procedure returns nothing (e.g. no privilege), sample nodes for keys.
        if (propertyNames.isEmpty()) {
            Result sample = session.run(
                    "MATCH (n:`" + label.replace("`", "``") + "`) RETURN keys(n) AS k LIMIT 1000");
            while (sample.hasNext()) {
                for (Object key : sample.next().get("k").asList()) {
                    propertyNames.add(key.toString());
                }
            }
        }
        return propertyNames.toArray(new String[0]);
    }

    private void advance() {
        nextRecord = result.hasNext() ? result.next() : null;
    }

    @Override
    public boolean hasNext() {
        return nextRecord != null;
    }

    @Override
    public String[] next() {
        Node node = nextRecord.get(0).asNode();
        String[] row = new String[header.length];
        for (int i = 0; i < header.length; i++) {
            Value value = node.get(header[i]);
            row[i] = (value == null || value.isNull()) ? config.nullString : value.asObject().toString();
        }
        advance();
        return row;
    }

    @Override
    public String[] getHeader() {
        return header;
    }

    @Override
    public void close() throws IOException {
        if (session != null) session.close();
        if (driver != null) driver.close();
    }
}
