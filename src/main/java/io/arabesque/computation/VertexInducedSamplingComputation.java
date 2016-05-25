package io.arabesque.computation;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.embedding.Embedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;

import java.util.Random;

/**
 * Created by carlos on 11/05/16.
 */
public abstract class VertexInducedSamplingComputation<E extends VertexInducedEmbedding> extends VertexInducedComputation<E> {

    private Random r;
    protected int samplesize;

    protected static final String SAMPLESIZE = "arabesque.motif.samplesize";
    protected static final int SAMPLESIZE_DEFAULT = 100;

    @Override
    public void init() {
        super.init();
        r = new Random(0); // TODO: custom seed?
        samplesize = Configuration.get().getInteger(SAMPLESIZE, SAMPLESIZE_DEFAULT);
    }

    @Override
    protected void doModifyFilter(int wordId) {

        //if expansion
        if (!currentEmbedding.existWord(wordId)) {
            currentEmbedding.addWord(wordId);
            if (shouldModify(currentEmbedding)) {
                underlyingExecutionEngine.processModification(currentEmbedding);
            }
            numChildrenEvaluated++;
            process(currentEmbedding);
            currentEmbedding.removeWord(currentEmbedding.getLastWord());
        }
        //if contraction
        else {
            currentEmbedding.removeWord(wordId);
            if (shouldModify(currentEmbedding)) {
                underlyingExecutionEngine.processModification(currentEmbedding);
            }
            numChildrenEvaluated++;
            process(currentEmbedding);
            currentEmbedding.addWord(wordId);
        }

    }

    @Override
    public void filter(E existingEmbedding, IntCollection modificationPoints) {

	int previousDegree = modificationPoints.size();
        if (previousDegree == 0) {
           existingEmbedding.reset();
           return;
        }

        int rdIdx = r.nextInt(previousDegree);
	int nextModification = modificationPoints.toIntArray()[rdIdx];
	int degree = 1;
	
	//if expansion
            if (!currentEmbedding.existWord(nextModification)) {
                currentEmbedding.addWord(nextModification);
		IntCollection modPoints = getPossibleModifications(currentEmbedding);
                degree = modPoints.size();
                currentEmbedding.removeWord(nextModification);
            }
            //if contraction
            else {
                currentEmbedding.removeWord(nextModification);
		IntCollection modPoints = getPossibleModifications(currentEmbedding);
                degree = modPoints.size();
                currentEmbedding.addWord(nextModification);
            }


        double accept = Math.min(1, (double) previousDegree/degree);

	modificationPoints.clear();
	
	if (r.nextDouble() <= accept) 
		modificationPoints.add(nextModification);

    }

    @Override
    public boolean filter(E existingEmbedding, int newWord) {
        return true;
    }

    @Override
    protected IntCollection getPossibleModifications(E embedding) {
        IntCollection possibleModifications = getPossibleExtensions(embedding);
        possibleModifications.addAll(getPossibleContractions(embedding));
        return possibleModifications;
    }

    @Override
    public void handleNoModifications(E embedding) {
        if (filter(embedding)) {
            if (shouldModify(embedding)) {
                underlyingExecutionEngine.processModification(embedding);
            }
            numChildrenEvaluated++;
            process(embedding);
        }
    }
    @Override
    protected IntArrayList getInitialExtensions() {

        IntArrayList initialExtensions = new IntArrayList();
        int numInitialWords = getInitialNumWords();

	int i = 0;
        while (i<samplesize/getNumberPartitions()) {
		initialExtensions.add(r.nextInt(numInitialWords));
		i++;
        }

        return initialExtensions;
    }
}
