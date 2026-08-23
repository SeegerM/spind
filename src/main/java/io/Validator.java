package io;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;
import structures.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Validator {
    Config config;
    Logger logger;
    Attribute[] attributeIndex;
    Candidates candidates;
    List<ValidationReader> readers;

    public Validator(Config config, Candidates candidates, int validationSize) throws IOException {
        this.config = config;
        this.attributeIndex = candidates.current;
        this.candidates = candidates;
        initReaders(validationSize);
        logger = LoggerFactory.getLogger(Validator.class);
    }

    /**
     * Validates candidates against a sample of the data.
     *
     * A violation observed on a sample is a real violation, so a candidate that already exceeds
     * its full-data budget here cannot hold on the complete data either and is safely dropped.
     * The converse does not apply, which is why nothing is concluded from survival — survivors go
     * on to the full pass. Null and global-uniqueness pruning are skipped because both read
     * metadata that describes only the sample.
     */
    public void validateSample(ColumnStats stats, BloomFilter<Integer> filter) {
        candidates.calculateViolations(stats);
        if (config.useFastValidation) {
            parallelPruneFast(1, filter);
        } else {
            parallelPrune(1, filter);
        }
    }

    public BloomFilter<Integer> validate(int layer, BloomFilter<Integer> filter) {

        candidates.calculateViolations(attributeIndex);

        candidates.pruneNull(attributeIndex);

        if (layer > 1) {
            candidates.pruneGlobalUnique(attributeIndex);
        }

        if (config.useFilter && config.refineFilter) {
            filter = BloomFilter.create(Funnels.integerFunnel(), 100_000_000, 0.05);
        }

        if (config.useFastValidation) {
            parallelPruneFast(layer, filter);
        } else {
            parallelPrune(layer, filter);
        }

        return filter;
    }

    /**
     * Same algorithm as {@link #parallelPrune}, with the per-value string round-trip removed.
     *
     * The baseline concatenates each relation's serialized attribute fragment into a StringBuilder
     * per value, then re-parses that string into a boxed {@code HashMap<Integer, Long>}. Here the
     * fragments are parsed once, directly into an {@link AttributeGroup}'s primitive arrays, and
     * candidate pruning reads those arrays. Pruning decisions are unchanged.
     */
    private void parallelPruneFast(int layer, BloomFilter<Integer> filter) {
        while (!readers.isEmpty()) {
            String maxValue = updateReaders(); // parallel
            HashMap<String, AttributeGroup> valueGroupMap = new HashMap<>();

            // single
            for (ValidationReader reader : readers) {
                Entry next = reader.queue.poll();
                while (next != null && next.getValue().compareTo(maxValue) <= 0) {
                    valueGroupMap.computeIfAbsent(next.getValue(), k -> new AttributeGroup())
                            .addSerialized(next.getSerializedAttributes());
                    next = reader.queue.poll();
                }
                if (next != null) {
                    // if the most recent value is bigger than the biggest safe value, we re-add it to the front of the queue.
                    reader.queue.addFirst(next);
                }
            }

            // parallel
            valueGroupMap.entrySet().stream().parallel().map(group -> {
                AttributeGroup attributeGroup = group.getValue();
                boolean onlyRef = true;
                int kept = 0;
                for (int index = 0; index < attributeGroup.size(); index++) {
                    int attributeId = attributeGroup.id(index);
                    Attribute next = attributeIndex[attributeId];
                    if (next.getReferenced() != null) {
                        // If there is at least one attribute from a dependent side, we need to prune the group
                        onlyRef = false;
                    } else if (next.getNumReferencedBy() == 0) {
                        // This attribute neither references another attribute nor is it referenced by any attribute.
                        continue;
                    }
                    attributeGroup.set(kept++, attributeId, attributeGroup.occurrences(index));
                }
                attributeGroup.retain(kept);

                if (onlyRef) {
                    // if we only find attribute that occur as a reference (or are irrelevant) we can skip pruning.
                    return null;
                }
                if (layer == 1) {
                    return new FastValidationTuple(attributeGroup, new int[]{group.getKey().hashCode()});
                } else if (config.refineFilter) {
                    return new FastValidationTuple(attributeGroup, hashesOf(group.getKey(), layer));
                } else {
                    return new FastValidationTuple(attributeGroup, null);
                }
            }).filter(Objects::nonNull).forEach(validationTuple -> {
                if (config.useFilter && (layer == 1 || config.refineFilter)) {
                    synchronized (filter) {
                        for (int hash : validationTuple.hashes()) {
                            filter.put(hash);
                        }
                    }
                }
                candidates.prune(validationTuple.attributeGroup());
            });
            cleanReaders();
        }
    }

    /** Splits an n-ary combined value back into its per-column hashes, as {@link #parallelPrune} does inline. */
    private int[] hashesOf(String raw, int layer) {
        int[] hashes = new int[layer];
        int lengthEnc = raw.indexOf('|') + 1;
        String[] lengths = raw.substring(0, lengthEnc - 1).split(":");
        assert lengths.length == layer - 1;
        for (int i = 0; i < layer - 1; i++) {
            int valueLength = Integer.parseInt(lengths[i]);
            String s = raw.substring(lengthEnc, lengthEnc + valueLength);
            hashes[i] = s.hashCode();
            lengthEnc += valueLength;
        }
        hashes[layer - 1] = raw.substring(lengthEnc).hashCode();
        return hashes;
    }

    private record FastValidationTuple(AttributeGroup attributeGroup, int[] hashes) {
    }

    private void parallelPrune(int layer, BloomFilter<Integer> filter) {
        while (!readers.isEmpty()) {
            String maxValue = updateReaders(); // parallel
            HashMap<String, StringBuilder> valueGroupMap = new HashMap<>();

            // single
            for (ValidationReader reader : readers) {
                Entry next = reader.queue.poll();
                while (next != null && next.getValue().compareTo(maxValue) <= 0) {
                    String connectedAttributes = next.getSerializedAttributes();
                    valueGroupMap.compute(next.getValue(), (k, v) -> v == null ? new StringBuilder(connectedAttributes) : v.append(connectedAttributes));
                    next = reader.queue.poll();
                }
                if (next != null) {
                    // if the most recent value is bigger than the biggest safe value, we re-add it to the front of the queue.
                    reader.queue.addFirst(next);
                }
            }

            // parallel
            valueGroupMap.entrySet().stream().parallel().map(group -> {
                HashMap<Integer, Long> valueGroup = buildAttributeMap(group.getValue().toString());
                boolean onlyRef = true;
                Iterator<Integer> keyIterator = valueGroup.keySet().iterator();
                while (keyIterator.hasNext()) {
                    Attribute next = attributeIndex[keyIterator.next()];
                    if (next.getReferenced() != null) {
                        // If there is at least one attribute from a dependent side, we need to prune the group
                        onlyRef = false;
                    } else if (next.getNumReferencedBy() == 0) {
                        // This attribute neither references another attribute nor is it referenced by any attribute.
                        keyIterator.remove();
                    }
                }
                if (onlyRef) {
                    // if we only find attribute that occur as a reference (or are irrelevant) we can skip pruning.
                    return null;
                }
                if (layer == 1) {
                    return new ValidationTuple(valueGroup, new int[]{group.getKey().hashCode()});
                } else if (config.refineFilter) {
                    String raw = group.getKey();
                    int[] hashes = new int[layer];
                    int lengthEnc = raw.indexOf('|') + 1;
                    String[] lengths = raw.substring(0, lengthEnc - 1).split(":");
                    assert lengths.length == layer - 1;
                    for (int i = 0; i < layer - 1; i++) {
                        int valueLength = Integer.parseInt(lengths[i]);
                        String s = raw.substring(lengthEnc, lengthEnc + valueLength);
                        hashes[i] = s.hashCode();
                        lengthEnc += valueLength;
                    }
                    String s = raw.substring(lengthEnc);
                    hashes[layer - 1] = s.hashCode();

                    return new ValidationTuple(valueGroup, hashes);
                } else {
                    return new ValidationTuple(valueGroup, null);
                }

            }).filter(Objects::nonNull).forEach(validationTuple -> {
                if (config.useFilter && (layer == 1 || config.refineFilter)) {
                    synchronized (filter) {
                        for (int hash : validationTuple.hashes()) {
                            filter.put(hash);
                        }
                    }
                }
                candidates.prune(validationTuple.attributeGroup());
            });
            cleanReaders();
        }
    }

    /**
     * updates every reader that was used in last value group
     */
    private String updateReaders() {
        // load the new values in parallel
        return readers.stream().parallel().map(ValidationReader::update).min(String::compareTo).orElse(null);
    }

    private void cleanReaders() {
        Iterator<ValidationReader> it = readers.iterator();
        while (it.hasNext()) {
            ValidationReader next = it.next();
            if (next.finished && next.queue.isEmpty()) {
                next.close();
                it.remove();
            }
        }
    }

    private void initReaders(int validationSize) throws IOException {
        List<Integer> relations = Arrays.stream(attributeIndex).mapToInt(Attribute::getRelationId).distinct().boxed().toList();
        readers = new ArrayList<>();
        for (int relation : relations) {
            String relationPath = config.tempFolder + File.separator + "relation_" + relation + ".txt";
            // in a rare edge case, a relation file might not exist. This can happen if the relation is only used in dependant sides of all-null references and the filter masks
            // all values. Therefor the sorting process finishes without a single value (which is correct) and no relation is created while merging.
            if (Files.exists(Path.of(relationPath))) readers.add(new ValidationReader(relationPath, validationSize));
        }
    }

    private HashMap<Integer, Long> buildAttributeMap(String serializedAttributes) {
        HashMap<Integer, Long> connectedAttributes = new HashMap<>();
        String[] attributes = serializedAttributes.split(";");
        for (String attribute : attributes) {
            String[] idOccurrenceTuple = attribute.split(",");
            if (idOccurrenceTuple.length != 2) {
                continue;
            }
            connectedAttributes.put(Integer.valueOf(idOccurrenceTuple[0]), Long.valueOf(idOccurrenceTuple[1]));
        }
        return connectedAttributes;
    }
}
