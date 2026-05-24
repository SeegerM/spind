package io;

import com.google.common.hash.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.Attribute;
import structures.MergeJob;
import structures.SortJob;
import structures.SortResult;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The sorter is responsible for the creation of one pre-processed file per table. The files consist of as many lines as
 * there are unique values in the whole relational input. The lines are ordered lexicographically by the value which
 * they are associated with. Further each line carries information in which attribute (Combinations) the value is
 * present and also how often it is present in these.
 *
 * @noinspection ALL
 */
public class Sorter {
    private final int maxMapSize;
    private final long minKeepCount;
    HashMap<String, HashMap<Integer, Long>> values;
    Logger logger;
    int currentSize;
    int spillCount;
    List<Path> spilledFiles;

    /**
     * To initialize a sorter, only the maximum map size is required. The constructor will initialize the value map and set the currentSize to 0.
     *
     * @param maxMapSize The maximal summed number of attribute (combinations) to be stored in the nested layer. This value should be as high as possible without risking memory
     *                   overflows for the best possible performance.
     */
    public Sorter(int maxMapSize, long minKeepCount) {
        this.maxMapSize = maxMapSize;
        this.minKeepCount = minKeepCount;
        values = new HashMap<>();
        currentSize = 0;
        logger = LoggerFactory.getLogger(Sorter.class);
    }

    /**
     * This method processes a sort job. It will first deduplicate the values of a given chunk using a HashMap, while keeping track of with attributes are connected to which
     * value. If the HashMap surpasses the maxMapSize or the end of the input is reached, the keys of the map are sorted and a file will be written to disk. The File has the
     * structure:
     * [Value1]
     * [Serialized Attributes of Value1]
     * [Value2]
     * ....
     *
     * @param sortJob carries information regarding the input path, the connected attributes and the relation, that the chunk is associated with.
     * @param config  carries information on how to parse the chunk file correctly.
     * @param filter  If the layer is at least two, the filter is used to disregard "non-informational" values.
     * @param layer   The current layer, equal to the dimension of the connected attributes.
     * @return A Tuple including a MergeJob and the connected attributes.
     */
    public SortResult process(SortJob sortJob, Config config, BloomFilter<Integer> filter, int layer) {
        spillCount = 0;
        spilledFiles = new ArrayList<>();

        CsvRelationalInput input;
        try {
            input = new CsvRelationalInput(sortJob, config);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        while (input.hasNext()) {
            input.updateAttributeCombinations(filter, layer);
            for (Attribute attribute : input.attributes) {
                String value = attribute.getCurrentValue();

                if (value == null) {
                    attribute.getMetadata().nullEntries++;
                    continue;
                }

                Map<Integer, Long> valueMap = values.computeIfAbsent(value, v -> new HashMap<>());

                if (1L == valueMap.compute(attribute.getId(), (k, v) -> v == null ? 1L : ++v)) {
                    if (++currentSize > maxMapSize) {
                        spill(sortJob.chunkPath(), false);
                    }
                }
            }
        }
        // if there are value which have not been written yet, we need to save them before ending the job
        if (!values.isEmpty()) {
            spill(sortJob.chunkPath(), true);
        }
        // close the input reader
        try {
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new SortResult(new MergeJob(spilledFiles, sortJob.relationId(), null, false), input.attributes);
    }

    /**
     * Will spill the current state to disk and clean the used memory
     *
     * @param chunkPath the path to which the file should be written.
     */
    private void spill(Path chunkPath, boolean isFinal) {
        spillCount++;
        Path spillPath = Path.of(chunkPath + "_" + spillCount + ".txt");
        toDisk(spillPath, isFinal);
        // keep track of all files that had
        spilledFiles.add(spillPath);
    }

    /**
     * Writes a processed output file
     *
     * @param outputPath The path to which the file is written. Will overwrite an existing file.
     * @param isFinal    A flag to indicate if the data to spill is the final action of the sorter.
     */
    private void toDisk(Path outputPath, boolean isFinal) {
        try {
            BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

            values.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                try {
                    writer.write(entry.getKey());
                    writer.newLine(); // separate the value and the serialized attributes by a new line
                    entry.getValue().forEach((k, v) -> {
                        try {
                            writer.write(String.valueOf(k));
                            writer.write(','); // attribute-occurrence separator
                            writer.write(String.valueOf(v));
                            writer.write(';'); // attribute-attribute separator
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            writer.close();

            values = new HashMap<>();
            currentSize = 0;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
