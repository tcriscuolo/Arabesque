package io.arabesque.gmlib.motif.sampling;

import io.arabesque.aggregation.reductions.LongSumReduction;
import io.arabesque.computation.VertexInducedSamplingComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import org.apache.hadoop.io.LongWritable;

public class MotifSamplingComputation extends VertexInducedSamplingComputation<VertexInducedEmbedding> {
    public static final String AGG_MOTIFS = "motifs";
    private static final String MAXSIZE = "arabesque.motif.maxsize";
    private static final String MAXSTEP = "arabesque.motif.maxiteration";
    private static final String SAMPLESIZE = "arabesque.motif.samplesize";
    private static final int MAXSIZE_DEFAULT = 4;
    private static final int MAXTSTEP_DEFAULT = 10;
    private static final int SAMPLESIZE_DEFAULT = 100;


    private static LongWritable reusableLongWritableUnit = new LongWritable(1);

    private int maxsize;
    private int maxstep;
    private int samplesize;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
        maxstep = Configuration.get().getInteger(MAXSTEP, MAXTSTEP_DEFAULT);
        samplesize = Configuration.get().getInteger(SAMPLESIZE, SAMPLESIZE_DEFAULT);
    }

    @Override
    public void initAggregations() {
        super.initAggregations();

        Configuration conf = Configuration.get();

        conf.registerAggregation(AGG_MOTIFS, conf.getPatternClass(), LongWritable.class, true, new LongSumReduction());
    }

    public boolean shouldModify(VertexInducedEmbedding embedding) {
        return getStep() < maxstep;
    }

    @Override
    public void process(VertexInducedEmbedding embedding) {
        if (embedding.getNumWords() == maxsize) {
            output(embedding);
            map(AGG_MOTIFS, embedding.getPattern(), reusableLongWritableUnit);
        }
    }
}
