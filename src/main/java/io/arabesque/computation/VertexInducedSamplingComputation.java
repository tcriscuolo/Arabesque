package io.arabesque.computation;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.utils.IntIntPair;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.function.IntConsumer;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by carlos on 11/05/16.
 */
public abstract class VertexInducedSamplingComputation<E extends VertexInducedEmbedding> extends VertexInducedComputation<E> {

    protected int embeddingNeighborhoodSize;
    private ArrayList<IntIntPair> degrees;
    private Random r;
    private IntDegreeEmbeddingConsumer intConsumer;

    protected static final String SAMPLESIZE = "arabesque.motif.samplesize";
    protected static final int SAMPLESIZE_DEFAULT = 100;

    class IntDegreeEmbeddingConsumer implements IntConsumer {
        @Override
        public void accept(int wordId) {
            //if expansion
            if (!currentEmbedding.existWord(wordId)) {
                currentEmbedding.addWord(wordId);
                if (shouldModify(currentEmbedding)) {
                    int degree =  getPossibleModifications(currentEmbedding).size();
                    embeddingNeighborhoodSize+=degree;
                    degrees.add(new IntIntPair(wordId, degree));
                }
                currentEmbedding.removeLastWord();
            }
            //if contraction
            else {
                currentEmbedding.removeWord(wordId);
                if (shouldModify(currentEmbedding)) {
                    int degree =  getPossibleModifications(currentEmbedding).size();
                    embeddingNeighborhoodSize+=degree;
                    degrees.add(new IntIntPair(wordId, degree));
                }
                currentEmbedding.addWord(wordId);
            }
        }
    };

    @Override
    public void init() {
        super.init();
        r = new Random();
        degrees = new ArrayList<>();
        intConsumer = new IntDegreeEmbeddingConsumer();
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
            currentEmbedding.removeLastWord();
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
        degrees.clear();
        embeddingNeighborhoodSize = 0;
        modificationPoints.forEach(intConsumer);

        //for the own embedding
        embeddingNeighborhoodSize += modificationPoints.size();
        degrees.add(new IntIntPair(-1, modificationPoints.size()));

        modificationPoints.clear();
        int nextModification = getNextRandomModification();
        if (nextModification != -1)
            modificationPoints.add(nextModification);

    }

    private int getNextRandomModification() {
        //compute next modification
        double prob = r.nextDouble();

        int acc = 0;
        int nextModification=-1;
        for (IntIntPair pair : degrees) {
            acc+=pair.getSecond();
            if (acc/embeddingNeighborhoodSize >= prob)
                nextModification = pair.getFirst();
        }

        return nextModification;
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
        embeddingNeighborhoodSize = 0;
        degrees.clear();

        IntArrayList initalExtensions = new IntArrayList();
        int numInitialWords = getInitialNumWords();

        for (int wordId = 0; wordId < numInitialWords; ++wordId) {
            currentEmbedding.addWord(wordId);
            int degree = getPossibleModifications(currentEmbedding).size();
            embeddingNeighborhoodSize+=degree;
            degrees.add(new IntIntPair(wordId, degree));
            currentEmbedding.removeLastWord();

            initalExtensions.add(wordId);
        }
        initalExtensions.forEach(intConsumer);

        int samplesize = Configuration.get().getInteger(SAMPLESIZE, SAMPLESIZE_DEFAULT);

        for (int i = 0; i<samplesize/getNumberPartitions(); ++i) {
            initalExtensions.add(getNextRandomModification());
        }

        return initalExtensions;
    }
}