package runner;

import com.opencsv.exceptions.CsvValidationException;
import core.Spind;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException, InterruptedException, CsvValidationException {

        long startTime = System.currentTimeMillis();

        Config config = new Config(0.93);

        config.maxNary = 1;

        if (args.length == 4) {
            config.CHUNK_SIZE = Integer.parseInt(args[0]);
            config.SORT_SIZE = Integer.parseInt(args[1]);
            config.MERGE_SIZE = Integer.parseInt(args[2]);
            config.VALIDATION_SIZE = Integer.parseInt(args[3]);
            System.out.println("Used args to set variables");
        }

        config.setDataset("path_to_folder");

        // Examples for the database-backed connectors. Credentials are resolved from
        // connections.properties (see connections.properties.example at the repo root).
        //
        // config.setDatasetFromPostgres("local", "public", "customer", "orders", "lineitem");
        // config.setDatasetFromMongo("local", "movies", "films", "actors");
        // config.setDatasetFromNeo4j("local", "neo4j", "Movie", "Person");
        //
        // Mixed-source runs: addTable can be called repeatedly with any TableSource variant.
        // config.addTable(new structures.TableSource.Postgres("local", "public", "users"));
        // config.addTable(new structures.TableSource.File(java.nio.file.Path.of("F:/data/extra.csv")));

        Spind spind = new Spind(config);
        spind.execute();

        System.out.println("Execution took: " + (System.currentTimeMillis() - startTime) + "ms");

    }
}
