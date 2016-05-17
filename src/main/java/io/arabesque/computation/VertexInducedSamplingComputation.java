package io.arabesque.computation;

import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.utils.IntIntPair;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.function.IntConsumer;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by carlos on 11/05/16.
 */
public abstract class VertexInducedSamplingComputation<E extends VertexInducedEmbedding> extends VertexInducedComputation<E> {

    protected int embeddingNeighborhoodSize;

    private Random r;

    @Override
    public void init() {
        super.init();
        r = new Random();
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
        final ArrayList<IntIntPair> degrees = new ArrayList<>();
        embeddingNeighborhoodSize = 0;

        IntConsumer intConsumer = new IntConsumer() {
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
        modificationPoints.forEach(intConsumer);

        //for the own embedding
        embeddingNeighborhoodSize+=modificationPoints.size();
        degrees.add(new IntIntPair(-1, modificationPoints.size()));

        //compute next modification
        double prob = r.nextDouble();

        int acc = 0;
        int nextModification=-1;
        for (IntIntPair pair : degrees) {
            acc+=pair.getSecond();
            if (acc/embeddingNeighborhoodSize >= prob)
                nextModification = pair.getFirst();
        }

        modificationPoints.clear();
        if (nextModification != -1)
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
}