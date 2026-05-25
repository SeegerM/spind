package runner;

import com.opencsv.exceptions.CsvValidationException;
import core.Spind;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * End-to-end smoke test for similarity-based psIND discovery against the SAWFISH paper's
 * soccer example (Table 1). Reads two tiny CSVs under {@code data/soccer/}, runs
 * {@link Spind} in edit-distance mode (τ=1), and prints the resulting psINDs.
 *
 * Run from the project root:
 *   {@code mvn -q exec:java -Dexec.classpathScope=test -Dexec.mainClass=runner.SoccerSmokeTest}
 *
 * Expected output includes psINDs:
 *   - {@code goalie[club] ⊆_ED1 results[name]} (every misspelled club finds an ED-1 match)
 *   - {@code results[name] ⊆_ED1 goalie[club]}
 */
public class SoccerSmokeTest {

    public static void main(String[] args) throws IOException, InterruptedException, CsvValidationException {
        Path projectRoot = Path.of(System.getProperty("user.dir"));
        Path tempFolder = projectRoot.resolve("spind_temp");
        Path resultFolder = projectRoot.resolve("statistics");
        Files.createDirectories(tempFolder);
        Files.createDirectories(resultFolder);

        Config config = new Config(0.5); // allow up to 50% violations
        config.folderPath = projectRoot.resolve("data").toString();
        config.tempFolder = tempFolder.toString();
        config.resultFolder = resultFolder.toString();

        config.similarityMode = Config.SimilarityMode.EDIT_DISTANCE;
        config.editDistanceThreshold = 1;
        config.maxValueLength = 50;
        config.maxNary = 1;

        config.setDataset("soccer");

        new Spind(config).execute();

        System.out.println("Smoke test done. Inspect " + resultFolder + "/1-ary_pINDs.json");
    }
}
