package similarity;

import runner.Config;

/** Builds the configured {@link SimilarityMeasure} from a {@link Config}. */
public final class SimilarityMeasureFactory {

    private SimilarityMeasureFactory() {}

    public static SimilarityMeasure create(Config config) {
        return switch (config.similarityMode) {
            case NONE -> throw new IllegalStateException(
                    "similarityMode=NONE — caller should branch before constructing a measure.");
            case EDIT_DISTANCE -> new LevenshteinSimilarity(config.editDistanceThreshold);
            case JACCARD -> new JaccardSimilarity(config.normalizedThreshold);
            case HYBRID -> new HybridLevenshtein(config.normalizedThreshold);
        };
    }
}
