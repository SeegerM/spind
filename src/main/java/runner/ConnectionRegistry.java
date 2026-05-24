package runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Loads connection details for external data sources from {@code connections.properties}.
 *
 * Default location is {@code ./connections.properties} in the working directory; override with
 * the {@code -Dspind.connections=&lt;path&gt;} JVM property. The file is .gitignored.
 *
 * Property schema:
 * <pre>
 *   postgres.&lt;name&gt;.url=jdbc:postgresql://host:5432/db
 *   postgres.&lt;name&gt;.user=...
 *   postgres.&lt;name&gt;.password=...
 *   mongo.&lt;name&gt;.uri=mongodb://user:pass@host:27017
 *   neo4j.&lt;name&gt;.uri=bolt://host:7687
 *   neo4j.&lt;name&gt;.user=...
 *   neo4j.&lt;name&gt;.password=...
 * </pre>
 */
public final class ConnectionRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionRegistry.class);
    private static final String DEFAULT_PATH = "connections.properties";
    private static final String SYSTEM_PROPERTY = "spind.connections";

    private final Properties properties;

    public ConnectionRegistry(Properties properties) {
        this.properties = properties;
    }

    public static ConnectionRegistry loadDefault() {
        String override = System.getProperty(SYSTEM_PROPERTY);
        Path path = Path.of(override == null ? DEFAULT_PATH : override);
        Properties props = new Properties();
        if (!Files.exists(path)) {
            logger.info("No connections.properties at {} — only file-based sources will work", path.toAbsolutePath());
            return new ConnectionRegistry(props);
        }
        try (InputStream in = Files.newInputStream(path)) {
            props.load(in);
            logger.info("Loaded connections from {}", path.toAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + path.toAbsolutePath(), e);
        }
        return new ConnectionRegistry(props);
    }

    public PostgresConnection getPostgres(String name) {
        return new PostgresConnection(
                require("postgres." + name + ".url"),
                require("postgres." + name + ".user"),
                require("postgres." + name + ".password"));
    }

    public MongoConnection getMongo(String name) {
        return new MongoConnection(require("mongo." + name + ".uri"));
    }

    public Neo4jConnection getNeo4j(String name) {
        return new Neo4jConnection(
                require("neo4j." + name + ".uri"),
                require("neo4j." + name + ".user"),
                require("neo4j." + name + ".password"));
    }

    private String require(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Missing required connection property: " + key);
        }
        return value;
    }

    public record PostgresConnection(String url, String user, String password) {}
    public record MongoConnection(String uri) {}
    public record Neo4jConnection(String uri, String user, String password) {}
}
