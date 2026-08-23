package structures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.Config;

import java.util.*;

/**
 * Manages candidate creation and pruning.
 */
public class Candidates {
    private final Config config;
    private final Logger logger = LoggerFactory.getLogger(Candidates.class);
    public Attribute[] current;
    HashMap<Integer, HashMap<Integer, HashMap<Integer, List<Integer>>>> unary;
    private int nextAttributeId;
    /** Scratch membership marker for {@link #prune(AttributeGroup)}; reused across calls. */
    private BitSet membership;

    public Candidates(Config config) {
        this.config = config;
    }

    /**
     * Prunes the current candidates
     *
     * @param valueGroup the group of attributes which all share some value.
     */
    public synchronized void prune(Map<Integer, Long> valueGroup) {
        for (int dependantAttributeId : valueGroup.keySet()) {
            if (current[dependantAttributeId].getReferenced() == null) {
                // the attribute does not depend on any other attribute.
                continue;
            }
            long occurrences;
            if (config.duplicateHandling == Config.DuplicateHandling.AWARE) {
                occurrences = valueGroup.get(dependantAttributeId);
            } else {
                occurrences = 1L;
            }
            PINDList.PINDIterator referenced = current[dependantAttributeId].getReferenced().elementIterator();
            while (referenced.hasNext()) {
                PINDList.PINDElement referencedAttribute = referenced.next();
                // if the valueGroup includes the referenced Attribute: no violation
                if (valueGroup.containsKey(referencedAttribute.id)) continue;

                // not null since we iterate over the key set
                if (referencedAttribute.violate(occurrences) < 0L) {
                    referenced.remove();
                    current[referencedAttribute.id].numReferencedBy--;
                }
            }
            if (current[dependantAttributeId].getReferenced().isEmpty()) {
                current[dependantAttributeId].setReferenced(null);
            }
        }
    }

    /**
     * Primitive-array counterpart of {@link #prune(Map)}.
     *
     * <p>Membership ("is the referenced attribute also in this value group?") is answered through a
     * reusable bit set rather than a hash lookup. Attribute ids are dense, the method is already
     * serialized on {@code this}, and only the bits actually set need clearing, so the bit set
     * costs nothing per call and removes the boxing the map lookup required.</p>
     */
    public synchronized void prune(AttributeGroup valueGroup) {
        if (membership == null || membership.size() < current.length) {
            membership = new BitSet(current.length);
        }
        for (int index = 0; index < valueGroup.size(); index++) {
            membership.set(valueGroup.id(index));
        }

        for (int index = 0; index < valueGroup.size(); index++) {
            int dependantAttributeId = valueGroup.id(index);
            if (current[dependantAttributeId].getReferenced() == null) {
                // the attribute does not depend on any other attribute.
                continue;
            }
            long occurrences = config.duplicateHandling == Config.DuplicateHandling.AWARE
                    ? valueGroup.occurrences(index)
                    : 1L;

            PINDList.PINDIterator referenced = current[dependantAttributeId].getReferenced().elementIterator();
            while (referenced.hasNext()) {
                PINDList.PINDElement referencedAttribute = referenced.next();
                // if the valueGroup includes the referenced Attribute: no violation
                if (membership.get(referencedAttribute.id)) continue;

                if (referencedAttribute.violate(occurrences) < 0L) {
                    referenced.remove();
                    current[referencedAttribute.id].numReferencedBy--;
                }
            }
            if (current[dependantAttributeId].getReferenced().isEmpty()) {
                current[dependantAttributeId].setReferenced(null);
            }
        }

        for (int index = 0; index < valueGroup.size(); index++) {
            membership.clear(valueGroup.id(index));
        }
    }

    /**
     * Using the current candidates, it produces a set of new candidates for the next layer.
     *
     * @return A list of all Attributes, which are present in at least one candidate pair.
     */
    public Attribute[] generateNextLayer(Attribute[] attributes, RelationMetadata[] relationMetadata, int layer) {
        HashMap<String, Set<String>> lookUp;
        if (layer == 1) {
            // safe unary attributes
            storeUnary(attributes, relationMetadata);
            lookUp = new HashMap<>();
        } else {
            lookUp = createLookUps(attributes);
        }
        // generate next layer by the method proposed in the BINDER paper
        HashMap<Attribute, Integer> nextAttributes = new HashMap<>();
        HashMap<Integer, PINDList> nextCandidates = new HashMap<>();
        this.nextAttributeId = 0;
        Arrays.stream(current).parallel().forEach(naryDepAttribute -> {

            int naryDepId = naryDepAttribute.id;

            if (current[naryDepId].getReferenced() == null) {
                return;
            }


            if (naryDepAttribute.getMetadata().totalValues == 0) {
                return;
            }

            int depRelationId = naryDepAttribute.getRelationId();
            // condition 1: the expansion needs to be from the same relation
            for (int unaryDepColumn : unary.get(depRelationId).keySet()) {
                // condition 2/3: the position of the expansion must be greater than all already contained ids
                if (unaryDepColumn <= Arrays.stream(naryDepAttribute.getContainedColumns()).max().orElse(-1)) {
                    continue;
                }

                PINDList.PINDIterator naryRef = current[naryDepId].getReferenced().elementIterator();
                while (naryRef.hasNext()) {
                    int naryRefId = naryRef.next().id;
                    Attribute naryRefAttribute = attributes[naryRefId];
                    int refRelationId = naryRefAttribute.getRelationId();

                    // condition 1: the referenced expansion needs to be from the same relation as the nary referenced attribute
                    if (!unary.get(depRelationId).get(unaryDepColumn).containsKey(refRelationId)) {
                        continue;
                    }

                    for (int unaryRefColumn : unary.get(depRelationId).get(unaryDepColumn).get(refRelationId)) {

                        // condition 3: the expansion cannot be in the set that should be expanded.
                        if (Arrays.stream(naryRefAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefColumn)) {
                            continue;
                        }

                        // the two expansions cannot overlap
                        if (refRelationId == depRelationId) {
                            if (Arrays.stream(naryDepAttribute.getContainedColumns()).anyMatch(x -> x == unaryRefColumn)) {
                                continue;
                            }
                            if (Arrays.stream(naryRefAttribute.getContainedColumns()).anyMatch(x -> x == unaryDepColumn)) {
                                continue;
                            }
                            if (unaryRefColumn == unaryDepColumn) {
                                continue;
                            }
                        }

                        int[] dependantColumns = Arrays.copyOf(naryDepAttribute.containedColumns, layer + 1);
                        dependantColumns[layer] = unaryDepColumn;

                        int[] referencedColumns = Arrays.copyOf(naryRefAttribute.containedColumns, layer + 1);
                        referencedColumns[layer] = unaryRefColumn;

                        if (layer > 1 && !possibleCandidate(dependantColumns, referencedColumns, depRelationId, refRelationId, lookUp, layer)) {
                            continue;
                        }

                        // valid candidate found
                        synchronized (nextAttributes) {
                            synchronized (nextCandidates) {
                                Attribute dependant = generateAttribute(naryDepAttribute, unaryDepColumn, nextAttributes);

                                Attribute referenced = generateAttribute(naryRefAttribute, unaryRefColumn, nextAttributes);

                                PINDList referencedList = nextCandidates.computeIfAbsent(dependant.getId(), k -> new PINDList());
                                referencedList.add(referenced.getId(), 0); // violations are handled elsewhere
                            }
                        }
                    }

                }
            }
        });
        return constructIndices(nextAttributes, nextCandidates);
    }

    /**
     * This method is called once the set of nextAttributes has been constructed. It handles an id reshuffling such that all attributes which are present on any left-hand side
     * are assigned strictly smaller ids than attributes which are only present in left-hand sides. This process safes some memory and is computationally easy to handle.
     *
     * @param nextAttributes The next attributes as a map Attribute -> Attribute id
     * @param nextCandidates The next candidates as a map dependant id -> List of referenced ids
     * @return The attribute index for the next layer
     */
    private Attribute[] constructIndices(HashMap<Attribute, Integer> nextAttributes, HashMap<Integer, PINDList> nextCandidates) {
        Attribute[] nextAttributeIndex = new Attribute[nextAttributeId];
        for (Attribute attribute : nextAttributes.keySet()) {
            int id = attribute.getId();
            nextAttributeIndex[id] = attribute;
            nextAttributeIndex[id].setReferenced(nextCandidates.getOrDefault(id, null));
        }
        current = nextAttributeIndex;
        return nextAttributeIndex;
    }

    private HashMap<String, Set<String>> createLookUps(Attribute[] attributes) {
        HashMap<String, Set<String>> lookup = new HashMap<>(current.length);
        for (int depId = 0; depId < current.length; depId++) {

            if (current[depId].getReferenced() == null) {
                continue;
            }

            String depString = attributes[depId].toString();
            HashSet<String> depSet = new HashSet<>();
            PINDList.PINDIterator referencedList = current[depId].getReferenced().elementIterator();
            while (referencedList.hasNext()) {
                int refId = referencedList.next().id;
                String refString = attributes[refId].toString();
                depSet.add(refString);
            }
            lookup.put(depString, depSet);
        }
        return lookup;
    }

    private boolean possibleCandidate(int[] dependantColumns, int[] referencedColumns, int depRelationId, int refRelationId, HashMap<String, Set<String>> lookup, int layer) {
        // a candidate can only be valid, if all included partitions of size-1 have been validated.
        // we do not need to skip the last position, since that one is always possible
        for (int skipIndex = 0; skipIndex < layer; skipIndex++) {
            StringBuilder depString = new StringBuilder();
            StringBuilder refString = new StringBuilder();

            depString.append(depRelationId).append(": [");
            refString.append(refRelationId).append(": [");

            int attPointer = 0;
            for (int col = 0; col < layer; col++) {
                if (col == skipIndex) {
                    attPointer++;
                }
                depString.append(dependantColumns[attPointer]).append(", ");
                refString.append(referencedColumns[attPointer]).append(", ");

                attPointer++;
            }
            depString.delete(depString.length() - 2, depString.length()).append("]");
            refString.delete(refString.length() - 2, refString.length()).append("]");

            Set<String> refSet = lookup.get(depString.toString());
            if (refSet == null || !refSet.contains(refString.toString())) {
                /*
                Avoid many calls to the logger
                 logger.debug("Skipped candidate. " + depRelationId + ": " + Arrays.toString(dependantColumns) + " -> " + refRelationId + ": " + Arrays.toString
                 (referencedColumns));
                */
                return false;
            }
        }
        return true;
    }

    public void cleanCandidates() {
        for (Attribute attribute : current) {
            if (attribute.getReferenced() != null && attribute.getReferenced().isEmpty()) {
                attribute.setReferenced(null);
            }
        }
    }

    /**
     * Stores the unary pINDs so that they can be used to expand the n-ary attributes in the higher layers.
     * Unary pINDs are stored as follows:
     * [RelationId] maps to -> [DependantColumn] maps to -> [RelationId of reference] contains list of column numbers.
     *
     * @param attributes       The attribute index of all attributes with size 1.
     * @param relationMetadata Metadata that is used to access the relation offsets
     */
    private void storeUnary(Attribute[] attributes, RelationMetadata[] relationMetadata) {
        unary = new HashMap<>();
        for (int dependentAttribute = 0; dependentAttribute < current.length; dependentAttribute++) {

            if (current[dependentAttribute].getReferenced() == null) {
                continue;
            }

            if (attributes[dependentAttribute].getMetadata().totalValues == 0) {
                // do not safe attributes which are completely empty. They do not carry meaning.
                continue;
            }

            int dependentRelationId = attributes[dependentAttribute].getRelationId();

            HashMap<Integer, HashMap<Integer, List<Integer>>> relationMap = unary.computeIfAbsent(dependentRelationId, k -> new HashMap<>());

            HashMap<Integer, List<Integer>> referredAttributes = new HashMap<>();

            PINDList.PINDIterator referencedIterator = current[dependentAttribute].getReferenced().elementIterator();
            while (referencedIterator.hasNext()) {

                int referencedAttribute = referencedIterator.next().id;

                if (attributes[referencedAttribute].getMetadata().totalValues == 0) {
                    // do not safe attributes which are completely empty. They do not carry meaning.
                    continue;
                }

                int referencedRelationId = attributes[referencedAttribute].getRelationId();
                referredAttributes.computeIfAbsent(referencedRelationId, k -> new ArrayList<>());

                referredAttributes.get(referencedRelationId).add(referencedAttribute - relationMetadata[referencedRelationId].offset);
            }
            relationMap.put(dependentAttribute - relationMetadata[dependentRelationId].offset, referredAttributes);
        }
    }

    private Attribute generateAttribute(Attribute naryAttribute, int unaryExpansionId, HashMap<Attribute, Integer> nextAttributes) {
        Attribute attribute = new Attribute(-1,
                naryAttribute.getRelationId(),
                Arrays.copyOf(naryAttribute.getContainedColumns(), naryAttribute.getContainedColumns().length + 1));
        attribute.getContainedColumns()[naryAttribute.getContainedColumns().length] = unaryExpansionId;

        if (!nextAttributes.containsKey(attribute)) {
            attribute.setId(nextAttributeId);
            nextAttributes.put(attribute, nextAttributeId);
            nextAttributeId++;
        } else {
            attribute.setId(nextAttributes.get(attribute));
        }

        return attribute;
    }

    /**
     * Given the attribute index, this method generates the current candidates.
     *
     * @param attributes the current attribute index
     */
    public void loadUnary(Attribute[] attributes) {
        List<Integer> attributeIds = Arrays.stream(attributes).map(Attribute::getId).toList();
        for (Attribute attribute : attributes) {
            attribute.setReferenced(new PINDList(attributeIds, attribute.id));
        }
        current = attributes;
    }

    /**
     * Seeds the unary candidates, skipping pairs that the column statistics already rule out.
     *
     * <p>For a candidate {@code dep ⊆ ref}, at least {@code distinct(dep) - distinct(ref)} distinct
     * dependent values can have no counterpart in the referenced column — there simply are not
     * enough referenced values to go around. Each such value costs at least one violation (exactly
     * one when counting distinct values, at least one when counting occurrences), so
     * {@code distinct(dep) - distinct(ref)} is a lower bound on the violations the validator will
     * eventually charge. Exceeding the violation budget therefore proves the pIND cannot hold, and
     * the pair never has to be materialized.</p>
     *
     * <p>The bound is only applied when both distinct counts are exact; a column whose count was
     * dropped for exceeding the statistics budget contributes no pruning. Skipping a pair here is
     * equivalent to the validator eliminating it later, so the surviving result is unchanged.</p>
     *
     * @return the number of candidate pairs that were never created
     */
    public long loadUnary(Attribute[] attributes, ColumnStats stats) {
        current = attributes;
        long pruned = 0;
        long prunedByDomain = 0;

        for (Attribute dependant : attributes) {
            int dependantId = dependant.getId();

            long dependantDistinct = stats.distinctValues(dependantId);
            long dependantSize = config.duplicateHandling == Config.DuplicateHandling.UNAWARE
                    ? dependantDistinct
                    : stats.totalValues(dependantId);

            // Without an exact distinct count for the dependent side there is no bound to apply.
            boolean bounded = dependantDistinct != ColumnStats.UNKNOWN && dependantSize != ColumnStats.UNKNOWN;
            // Mirrors calculateViolations so the budget used for pruning is the same one the
            // validator will later enforce.
            long violationCap = bounded ? (long) ((1.0 - config.threshold) * dependantSize) : Long.MAX_VALUE;

            ColumnStats.Domain dependantDomain = stats.domain(dependantId);

            PINDList referenced = new PINDList();
            for (Attribute candidate : attributes) {
                int referencedId = candidate.getId();
                if (referencedId == dependantId) {
                    continue;
                }
                // Two observations are comparable only if they are drawn from the same domain.
                // Without this, a numeric column is included in every textual one that happens to
                // spell its values, which holds and says nothing. An empty column has no domain
                // and is left to the other rules.
                if (config.typedComparison
                        && dependantDomain != ColumnStats.Domain.UNKNOWN
                        && stats.domain(referencedId) != ColumnStats.Domain.UNKNOWN
                        && dependantDomain != stats.domain(referencedId)) {
                    prunedByDomain++;
                    continue;
                }
                if (bounded) {
                    long referencedDistinct = stats.distinctValues(referencedId);
                    if (referencedDistinct != ColumnStats.UNKNOWN
                            && dependantDistinct - referencedDistinct > violationCap) {
                        pruned++;
                        continue;
                    }
                }
                referenced.add(referencedId, 0L);
            }
            dependant.setReferenced(referenced);
        }

        logger.info("Pre-pruned " + pruned + " unary candidates using column statistics ("
                + stats.countedColumns() + "/" + attributes.length + " columns had exact distinct counts).");
        if (config.typedComparison) {
            logger.info("Pre-pruned " + prunedByDomain + " unary candidates whose two sides are "
                    + "drawn from different comparison domains.");
        }
        return pruned + prunedByDomain;
    }

    public void pruneGlobalUnique(Attribute[] attributes) {
        int numPruned = 0;
        for (int dependantId = 0; dependantId < current.length; dependantId++) {

            if (current[dependantId].getReferenced() == null) {
                continue;
            }

            long depGlobalUnique = attributes[dependantId].getMetadata().globalUnique;

            if (depGlobalUnique == 0L) {
                continue;
            }

            PINDList refMap = current[dependantId].getReferenced();
            PINDList.PINDIterator referred = refMap.elementIterator();
            while (referred.hasNext()) {
                if (referred.next().violate(depGlobalUnique) < 0) {
                    referred.remove();
                    numPruned++;
                }
            }
        }
        logger.info("Pruned " + numPruned + " candidates through global uniqueness.");
    }

    public void pruneNull(Attribute[] attributes) {
        int numPruned = 0;
        // in subset and equality mode, we do not need to do anything
        if (config.nullHandling != Config.NullHandling.SUBSET && config.nullHandling != Config.NullHandling.EQUALITY) {

            for (int dependantId = 0; dependantId < current.length; dependantId++) {

                // An attribute that has lost every candidate carries a null list, not an empty one.
                // Only reachable outside SUBSET/EQUALITY mode, which is why this never fired before.
                if (current[dependantId] == null || current[dependantId].getReferenced() == null) {
                    continue;
                }

                long depNull = attributes[dependantId].getMetadata().nullEntries;

                PINDList.PINDIterator referenced = current[dependantId].getReferenced().elementIterator();
                while (referenced.hasNext()) {
                    PINDList.PINDElement ref = referenced.next();
                    long refNull = attributes[ref.id].getMetadata().nullEntries;

                    if (config.nullHandling == Config.NullHandling.FOREIGN) {
                        if (refNull > 0) {
                            // foreign mode does not allow the referenced side to have any nulls
                            referenced.remove();
                            numPruned++;
                        }
                    } else if (config.nullHandling == Config.NullHandling.INEQUALITY) {
                        // Inequality mode: every null is different. Therefor all depNulls are violations
                        if (ref.violate(depNull) < 0) {
                            referenced.remove();
                            numPruned++;
                        }
                    }
                }
            }
        }
        logger.info("Pruned " + numPruned + " candidates through null constraints.");
    }

    /**
     * Assigns violation budgets from the chunk-time column statistics instead of from metadata
     * gathered during sorting.
     *
     * The progressive-sampling pass sees only a fraction of the data, so the metadata it produces
     * would yield budgets that are far too small and would prune valid pINDs. The statistics
     * describe the whole relation, so the budgets computed here are the real ones — the sample
     * simply gets fewer chances to spend them. A dependent column with no exact distinct count in
     * UNAWARE mode gets an unlimited budget, which disables sample pruning for it rather than
     * risking an unsound one.
     */
    public void calculateViolations(ColumnStats stats) {
        for (int dependantId = 0; dependantId < current.length; dependantId++) {

            if (current[dependantId].getReferenced() == null) {
                continue;
            }

            long depSize = config.duplicateHandling == Config.DuplicateHandling.UNAWARE
                    ? stats.distinctValues(dependantId)
                    : stats.totalValues(dependantId);

            long maxViolations = depSize == ColumnStats.UNKNOWN
                    ? Long.MAX_VALUE
                    : (long) ((1.0 - config.threshold) * depSize);

            // One budget for the whole list: it depends only on the dependent attribute.
            PINDList referencedList = current[dependantId].getReferenced();
            referencedList.setViolationCap(maxViolations);

            PINDList.PINDIterator referenced = referencedList.elementIterator();
            while (referenced.hasNext()) {
                current[referenced.next().id].numReferencedBy++;
            }
        }
    }

    /**
     * Undoes everything the sampling pass accumulated, leaving only its candidate removals.
     * Violation counters, reference counts and per-attribute metadata all restart from zero so the
     * full pass measures the data exactly once.
     */
    public void resetAfterSampling(Attribute[] attributes) {
        for (Attribute attribute : attributes) {
            attribute.setNumReferencedBy(0);
            attribute.setMetadata(new Metadata());
            if (attribute.getReferenced() == null) {
                continue;
            }
            PINDList.PINDIterator referenced = attribute.getReferenced().elementIterator();
            while (referenced.hasNext()) {
                referenced.next().resetViolations();
            }
        }
    }

    public void calculateViolations(Attribute[] attributes) {
        for (int dependantId = 0; dependantId < current.length; dependantId++) {

            if (current[dependantId].getReferenced() == null) {
                continue;
            }

            long depSize;

            if (config.duplicateHandling == Config.DuplicateHandling.UNAWARE) {
                depSize = attributes[dependantId].getMetadata().uniqueValues;
            } else {
                depSize = attributes[dependantId].getMetadata().totalValues;
            }

            long maxViolations = (long) ((1.0 - config.threshold) * depSize);

            // One budget for the whole list: it depends only on the dependent attribute.
            PINDList referencedList = current[dependantId].getReferenced();
            referencedList.setViolationCap(maxViolations);

            PINDList.PINDIterator referenced = referencedList.elementIterator();
            while (referenced.hasNext()) {
                current[referenced.next().id].numReferencedBy++;
            }
        }
    }
}
