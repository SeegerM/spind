package runner;

import com.opencsv.exceptions.CsvValidationException;
import core.Spind;
import structures.TableSource;

import java.io.IOException;

/**
 * Example runner showing how to discover partial similarity inclusion dependencies (psINDs).
 *
 * SPIND's classic mode finds INDs where dependent values appear <i>exactly</i> in the
 * referenced column. With {@link Config#similarityMode} set to anything other than
 * {@link Config.SimilarityMode#NONE}, SPIND instead runs a SAWFISH-style pipeline that finds
 * INDs where dependent values appear <i>similarly</i> in the referenced column under a
 * configurable similarity measure. The partial-threshold ({@code Config(threshold)}) still
 * caps allowed violations, so the output is partial-sIND ("psIND").
 *
 * Currently implemented measures:
 *  - {@link Config.SimilarityMode#EDIT_DISTANCE} — absolute Levenshtein, threshold {@code τ}.
 *  - {@link Config.SimilarityMode#JACCARD} and {@link Config.SimilarityMode#HYBRID} are
 *    interface-conforming stubs (Phase B); selecting them will throw at measure construction.
 *
 * Similarity discovery is unary only and composes with any of SPIND's data sources (CSV
 * folder, Postgres, Mongo, Neo4j) — the validator runs after the same chunking step.
 */
public class SimilarityRunner {

    public static void main(String[] args) throws IOException, InterruptedException, CsvValidationException {

        long startTime = System.currentTimeMillis();

        // 0.5 threshold means: a psIND holds if at least 50% of distinct dep values have a
        // similar match in the ref column. Tighten toward 1.0 for stricter results.
        Config config = new Config(0.8);

        // --- Switch on similarity mode ---
        config.similarityMode = Config.SimilarityMode.EDIT_DISTANCE;
        config.editDistanceThreshold = 1;            // τ = 1 edit (catches single-char typos)
        config.maxValueLength = 50;                  // skip very long values (paper §3 default)
        config.maxNary = 1;

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

        new Spind(config).execute();

        System.out.println("Similarity execution took: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
