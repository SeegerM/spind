package io;

import runner.Config;
import structures.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class Output {

    String resultFolder;

    public Output(String resultFolder) {
        this.resultFolder = resultFolder;
    }

    private static double getPartialDegree(Config config, PINDList.PINDElement next) {
        long violations = next.getViolations();
        long capViolations = next.violationCap();

        // the partial degree can be calculated using the threshold and the leftover violations
        double partialDegree = 1.0;
        if (capViolations > 0) {
            partialDegree = (double) violations / capViolations;
            partialDegree = 1.0 - partialDegree + config.threshold * partialDegree;
        }
        return partialDegree;
    }

    /**
     * Given the currently validated pINDs, a human-readable file will be created.
     *
     * @param inputs     The input files
     * @param attributes The current attribute index
     * @param layer      The current layer. Needed to save the pINDs to layer-based files
     * @throws IOException If something goes wrong during output writing
     */
    public void storePINDs(RelationMetadata[] inputs, Attribute[] attributes, int layer, Config config) throws IOException {

        if (!config.writeResults) {
            return;
        }

        BufferedWriter outputWriter = Files.newBufferedWriter(Path.of(this.resultFolder + File.separator + layer + "-ary_pINDs.json"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        outputWriter.write("[");
        boolean notFirst = false;
        // structure the output by relations
        for (RelationMetadata input : inputs) {

            if (notFirst) {
                outputWriter.write(",");
            }

            outputWriter.write("{ \"relation\":" + "\"" + config.tableNames[input.id] + "\",");

            // we first save information regarding the attributes in the dataset
            outputWriter.write("\"attributes\":" + Arrays.toString(input.columnNames).replace(", ", "\",\"").replace("[", "[\""). replace("]", "\"]") + ",");

            // afterward, we save the pINDs which are dependent on the current relation
            boolean notFirstAttribute = false;
            for (Attribute attribute : Arrays.stream(attributes).filter(attribute -> attribute.getRelationId() == input.id).toList()) {
                if (attribute.getReferenced() == null || attribute.getReferenced().isEmpty()) {
                    continue;
                }

                if (notFirstAttribute) {
                    outputWriter.write(",");
                } else {
                    outputWriter.write("\"pINDs\":[");
                }
                notFirstAttribute = true;

                outputWriter.write("{ \"dependant\":" + buildAttributeString(input, attribute) + ",");
                outputWriter.write("\"referenced\":[");

                PINDList.PINDIterator pindIterator = attribute.getReferenced().elementIterator();
                while (pindIterator.hasNext()) {
                    PINDList.PINDElement next = pindIterator.next();
                    int referencedId = next.id;
                    double partialDegree = getPartialDegree(config, next);
                    outputWriter.write("{\"partialDegree\":" + partialDegree + ",");
                    outputWriter.write("\"relation\":\"" + config.tableNames[attributes[referencedId].getRelationId()] + "\",");
                    outputWriter.write("\"attribute\":" + buildAttributeString(inputs[attributes[referencedId].getRelationId()], attributes[referencedId]));
                    outputWriter.write("}");
                    if (pindIterator.hasNext()) {
                        outputWriter.write(",");
                    }

                }
                outputWriter.write("]}");
            }
            if (notFirstAttribute) {
                outputWriter.write("]");
            }
            outputWriter.write("}");
            notFirst = true;
        }
        outputWriter.write("]");
        outputWriter.close();
    }

    /**
     * Writes the discovered pINDs of one layer as a canonical, sorted, one-line-per-pIND file.
     *
     * The regular JSON output lists pINDs in whatever order attributes and PINDLists happen to
     * be built in, which legitimately differs between algorithm variants. This dump is sorted
     * and therefore comparable byte-for-byte, so a benchmark can assert that an optimization
     * changed the runtime but not the result.
     */
    public void storeCanonicalPINDs(RelationMetadata[] inputs, Attribute[] attributes, int layer, Config config, String canonicalFolder) throws IOException {

        List<String> lines = new ArrayList<>();
        for (Attribute attribute : attributes) {
            if (attribute.getReferenced() == null || attribute.getReferenced().isEmpty()) {
                continue;
            }
            String dependant = config.tableNames[attribute.getRelationId()] + "." + buildAttributeString(inputs[attribute.getRelationId()], attribute);

            PINDList.PINDIterator pindIterator = attribute.getReferenced().elementIterator();
            while (pindIterator.hasNext()) {
                PINDList.PINDElement next = pindIterator.next();
                Attribute referencedAttribute = attributes[next.id];
                String referenced = config.tableNames[referencedAttribute.getRelationId()] + "."
                        + buildAttributeString(inputs[referencedAttribute.getRelationId()], referencedAttribute);
                // six decimals: enough to catch a real change in violation accounting, loose
                // enough to ignore double-rounding noise
                lines.add(dependant + " -> " + referenced + " @" + String.format(Locale.ROOT, "%.6f", getPartialDegree(config, next)));
            }
        }
        Collections.sort(lines);

        Files.createDirectories(Path.of(canonicalFolder));
        try (BufferedWriter writer = Files.newBufferedWriter(Path.of(canonicalFolder + File.separator + layer + "-ary_canonical.txt"),
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    private String buildAttributeString(RelationMetadata input, Attribute attribute) {
        StringBuilder dependantString = new StringBuilder();
        dependantString.append("[");
        for (int column : attribute.getContainedColumns()) {
            dependantString.append("\"").append(input.columnNames[column]).append("\",");
        }
        dependantString.delete(dependantString.length() - 1, dependantString.length()).append("]");
        return dependantString.toString();
    }

    /**
     * @param config
     * @param clock
     * @param metrics
     * @throws IOException
     */
    public void storeMetadata(Config config, Clock clock, Metrics metrics) throws IOException {
        BufferedWriter outputWriter = Files.newBufferedWriter(Path.of(this.resultFolder + File.separator + config.executionName + "_" + (System.currentTimeMillis() / 1000) + ".json"), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);

        outputWriter.write("{" + "\"Dataset\":\"" + config.databaseName + '"' + ",\"threshold\":" + config.threshold + ",\"parallelism\":" + config.PARALLEL + ",\"max n-ary\":" + config.maxNary + ",\"relations\":" + config.tableNames.length + ",\"attributes\":" + metrics.layerAttributes.get(0) + ",\"unary\":" + metrics.unary + ",\"n-ary\":" + metrics.nary + ",\"attributes_per_layer\":" + metrics.layerAttributes + ",\"candidates_per_layer\":" + metrics.layerCandidates + ",\"pINDs_per_layer\":" + metrics.layerPINDs + ",\"CHUNK_SIZE\":" + config.CHUNK_SIZE + ",\"SORT_SIZE\":" + config.SORT_SIZE + ",\"MERGE_SIZE\":" + config.MERGE_SIZE + ",\"VALIDATION_SIZE\":" + config.VALIDATION_SIZE + ",\"total_time\":" + clock.stop("total") + ",\"chunk_times\":" + clock.measures.get("chunking") + ",\"output_times\":" + clock.measures.get("output") + ",\"preprune_times\":" + clock.measures.get("preprune") + ",\"sort_times\":" + clock.measures.get("sorting") + ",\"merge_times\":" + clock.measures.get("merging") + ",\"validate_times\":" + clock.measures.get("validation") + ",\"generate_times\":" + clock.measures.get("generateNext") + ",\"CHUNK_FILES\":" + metrics.chunkFiles + ",\"SORT_FILES\":" + metrics.sortFiles + ",\"MERGE_FILES\":" + metrics.mergeFiles + ",\"use_filter\":" + config.useFilter + ",\"refine_filter\":" + config.refineFilter + ",\"pre_pruned\":" + metrics.prePruned + ",\"sample_pruned\":" + metrics.samplePruned + ",\"sampling_times\":" + clock.measures.get("sampling") + ",\"inmemory_times\":" + clock.measures.get("inmemory") + ",\"partitioned_times\":" + clock.measures.get("partitioned") + ",\"partitions\":" + metrics.partitions + ",\"partition_depth\":" + metrics.partitionDepth + ",\"largest_partition\":" + metrics.largestPartition + ",\"removed_by_bound\":" + metrics.removedByBound + ",\"partition_write_ms\":" + metrics.partitionWriteMillis + ",\"partition_load_ms\":" + metrics.partitionLoadMillis + ",\"partition_accumulate_ms\":" + metrics.partitionAccumulateMillis + "}");
        outputWriter.close();
    }
}
