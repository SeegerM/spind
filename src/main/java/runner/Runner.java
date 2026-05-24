package runner;

import com.opencsv.exceptions.CsvValidationException;
import core.Spind;

import java.io.IOException;
import java.util.Arrays;

public class Runner {

    public static void main(String[] args) throws IOException, InterruptedException, CsvValidationException {
        long startTime = 0;
        if (args.length < 3) {
            Config config = new Config(1.0);
            config.inputFileHasHeader = true;
            config.fileEnding = ".csv";
            config.separator = ',';
            String datasetName = "T2D";
            //String datasetName = "TestIND ACNH";
            config.setDataset("F:\\metaserve\\io\\data\\" + datasetName);
            //config.setDataset("F:\\metaserve\\io\\D43AC575-BDBA-40C8-B18B-3F1ADA922492-TPC-H-Tool\\out\\" + "TPCH1Gen");
            //config.PARALLEL = 6;
            if(datasetName.equals("Musicbrainz")) {
                config.fileEnding = "";
                config.nullString = "\\N";
                config.quoteChar = '\0';
            }
            System.out.println("PARALLEL: " + config.PARALLEL);
            startTime = System.currentTimeMillis();
            Spind spind = new Spind(config);
            spind.execute();
        } else {
            // Parse arguments
            String datasetName = args[0];
            char separator = args[1].charAt(0);
            String fileEnding = String.valueOf(args[2]);
            boolean inputFileHasHeader = Boolean.parseBoolean(args[3]);
            double threshold = Double.parseDouble(args[4]);
            int maxNary = Integer.parseInt(args[5]);
            int VALIDATION_SIZE = Integer.parseInt(args[6]);
            int MERGE_SIZE = Integer.parseInt(args[7]);
            int CHUNK_SIZE = Integer.parseInt(args[8]);
            int SORT_SIZE = Integer.parseInt(args[9]);
            int PARALLEL = Integer.parseInt(args[10]);
            int NUMBER_OF_FILES = Integer.parseInt(args[11]);
            System.out.println("Starting with args: " + datasetName + " " + separator + " " + fileEnding + " " + inputFileHasHeader + " " + threshold);
            startTime = System.currentTimeMillis();

            // Create and configure the Config object
            Config config = new Config(threshold);
            config.maxNary = maxNary;
            config.inputFileHasHeader = inputFileHasHeader;
            config.fileEnding = fileEnding;
            config.separator = separator;
            if(datasetName.equals("Musicbrainz")) {
                config.fileEnding = "";
                config.nullString = "\\N";
                config.quoteChar = '\0';
            }
            if (PARALLEL != -1)
                config.PARALLEL = PARALLEL;
            config.VALIDATION_SIZE = VALIDATION_SIZE;
            config.MERGE_SIZE = MERGE_SIZE;
            config.CHUNK_SIZE = CHUNK_SIZE;
            config.SORT_SIZE = SORT_SIZE;

            //config.setDataset("F:\\metaserve\\io\\D43AC575-BDBA-40C8-B18B-3F1ADA922492-TPC-H-Tool\\out\\" + datasetName);
            config.setDataset("F:\\metaserve\\io\\data\\" + datasetName, NUMBER_OF_FILES);
            Spind spind = new Spind(config);
            spind.execute();
        }

        System.out.println("Execution took: " + (System.currentTimeMillis() - startTime) + "ms");

    }

}
