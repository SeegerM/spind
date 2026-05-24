package runner;

import com.opencsv.exceptions.CsvValidationException;
import core.Spind;
import structures.TableSource;

import java.io.IOException;

public class MixedSourceRunner {

    public static void main(String[] args) throws IOException, InterruptedException, CsvValidationException {

        long startTime = System.currentTimeMillis();

        Config config = new Config(1.0);
        config.maxNary = 1;
        config.databaseName = "mixed";

        // --- PostgreSQL tables ---
        // Connection name "pg" + schema "public" + table list. Each becomes one relation.
        config.addTable(new TableSource.Postgres("pg", "public", "customers"));
        config.addTable(new TableSource.Postgres("pg", "public", "orders"));

        // --- MongoDB collections ---
        // Connection name "mongo" + database "shop" + collection name. Nested fields are
        // flattened with dot-notation; the first array per document is exploded into rows.
        config.addTable(new TableSource.Mongo("mongo", "shop", "reviews"));
        config.addTable(new TableSource.Mongo("mongo", "shop", "sessions"));

        // --- Neo4j node labels ---
        // Connection name "graph" + database "neo4j" + label. One relation per label;
        // node properties become columns.
        config.addTable(new TableSource.Neo4jLabel("graph", "neo4j", "Product"));
        config.addTable(new TableSource.Neo4jLabel("graph", "neo4j", "Person"));

        // Run discovery. The output JSON in ./statistics references each relation by its
        // displayName(): e.g. "public.customers", "shop.reviews", "neo4j:Product".
        Spind spind = new Spind(config);
        spind.execute();

        System.out.println("Mixed-source execution took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
