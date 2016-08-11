package io.arabesque.gmlib.motif;

import io.arabesque.aggregation.AggregationStorage;
import io.arabesque.computation.MasterComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.pattern.Pattern;
import org.apache.hadoop.io.LongWritable;

public class MotifMasterComputation extends MasterComputation {

    private static final String MAXSTEP = "arabesque.motif.maxstep";
    private static final int MAXTSTEP_DEFAULT = 5;
    private int PATTERNS_FOUND = 0;

    private int maxstep;

    @Override
    public void init() {
        maxstep = Configuration.get().getInteger(MAXSTEP, MAXTSTEP_DEFAULT);
    }

    @Override
    public void compute() {
        System.out.println("Master computing");

        AggregationStorage<Pattern, LongWritable> aggregationStorage =
                readAggregation(MotifComputation.AGG_MOTIFS);

        System.out.println("Aggregation Storage: " + aggregationStorage);

        if (aggregationStorage.getNumberMappings() > 0) {
            System.out.println("Patterns :");

            for (Pattern pattern : aggregationStorage.getKeys()) {
                System.out.println("P#" + PATTERNS_FOUND + ": [Size: " + pattern.getNumberOfVertices() + "," + pattern + "] : " + aggregationStorage.getValue(pattern));
                PATTERNS_FOUND++;
            }
        } else if (getStep() > 0) {
            System.out.println("Empty.");
        }
    }
}