package structures;

import java.util.ArrayList;
import java.util.List;

public class Metrics {
    public int chunkFiles;
    public int sortFiles;
    public int mergeFiles;
    public int nary;
    public int unary;
    /** Unary candidates never created because column statistics ruled them out (O1). */
    public long prePruned;
    /** Unary candidates disproved by the sampling pass before the full pass (O3). */
    public long samplePruned;
    /** Hash partitions actually loaded and validated (O5). */
    public int partitions;
    /** Deepest re-partitioning level reached (O5); 0 means nothing needed splitting. */
    public int partitionDepth;
    /** Records in the biggest partition validated (O5). */
    public long largestPartition;
    /** Candidates dropped early by the remaining-mass bound (O5). */
    public long removedByBound;
    /** Time spent reading chunks and spilling them into partitions (O5). */
    public long partitionWriteMillis;
    /** Time spent loading partitions back into memory (O5). */
    public long partitionLoadMillis;
    /** Time spent indexing partitions and accumulating coverage (O5). */
    public long partitionAccumulateMillis;
    public List<Integer> layerAttributes;
    public List<Integer> layerCandidates;
    public List<Integer> layerPINDs;


    public Metrics() {
        layerAttributes = new ArrayList<>();
        layerCandidates = new ArrayList<>();
        layerPINDs = new ArrayList<>();
    }
}