package runner;

import core.Spind;
import structures.TableSource;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Command-line entry point for SPIND that takes all settings as arguments.
 *
 * <p>{@link Runner} hard-codes its dataset path and threshold. This runner takes them as
 * arguments and does not change discovery itself. It passes the CSV files of the folder
 * explicitly instead of calling {@code Config.setDataset}, which would also add any
 * subdirectories of the folder.</p>
 *
 * <pre>
 *   java -cp spind.jar runner.SemfdRunner &lt;folder&gt; &lt;resultFolder&gt; &lt;tempFolder&gt;
 *                                        &lt;threshold&gt; &lt;maxNary&gt; &lt;separator&gt; [nullString]
 * </pre>
 *
 * <p>{@code threshold} 1.0 discovers exact INDs; below it, partial ones. {@code separator}
 * is a single character, or the word {@code semicolon} / {@code tab} where a shell makes
 * the literal awkward.</p>
 */
public class SemfdRunner {

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("usage: SemfdRunner <folder> <resultFolder> <tempFolder> "
                    + "<threshold> <maxNary> <separator> [nullString]");
            System.exit(2);
        }

        Path folder = Path.of(args[0]);
        double threshold = Double.parseDouble(args[3]);
        int maxNary = Integer.parseInt(args[4]);
        char separator = parseSeparator(args[5]);

        Config config = new Config(threshold);
        config.maxNary = maxNary;
        config.separator = separator;
        config.resultFolder = args[1];
        config.tempFolder = args[2];
        config.writeResults = true;
        config.executionName = "semfd-crosscheck";
        if (args.length >= 7) {
            config.nullString = args[6];
        }
        // Optional 8th argument: "nofilter" disables the bloom filter, for isolating
        // whether a missing IND is a filter artefact or a real verdict.
        if (args.length >= 8 && args[7].equalsIgnoreCase("nofilter")) {
            config.useFilter = false;
            config.refineFilter = false;
        }

        File[] files = folder.toFile().listFiles(
                (dir, name) -> name.toLowerCase().endsWith(".csv"));
        if (files == null || files.length == 0) {
            System.err.println("no .csv files in " + folder.toAbsolutePath());
            System.exit(3);
        }
        Arrays.sort(files);
        config.databaseName = folder.getFileName().toString();
        for (File file : files) {
            config.addTable(new TableSource.File(file.toPath()));
        }

        System.out.println("tables: " + config.tableNames.length
                + "  threshold: " + threshold
                + "  maxNary: " + maxNary
                + "  separator: '" + separator + "'"
                + "  nullString: '" + config.nullString + "'"
                + "  nullHandling: " + config.nullHandling
                + "  duplicateHandling: " + config.duplicateHandling
                + "  useFilter: " + config.useFilter);

        long start = System.currentTimeMillis();
        new Spind(config).execute();
        System.out.println("done in " + (System.currentTimeMillis() - start) + "ms");
    }

    private static char parseSeparator(String raw) {
        return switch (raw.toLowerCase()) {
            case "semicolon", "semi" -> ';';
            case "tab" -> '\t';
            case "comma" -> ',';
            case "pipe" -> '|';
            default -> raw.charAt(0);
        };
    }
}
