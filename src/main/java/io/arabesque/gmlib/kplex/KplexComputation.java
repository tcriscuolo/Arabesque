package io.arabesque.gmlib.kplex;

import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.utils.collection.IntArrayList;
import org.apache.commons.lang.mutable.Mutable;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.HashMap;
import java.util.Map;


public class KplexComputation extends VertexInducedComputation<VertexInducedEmbedding> {
    private static final String MAXSIZE = "arabesque.kplex.maxsize";
    private static final String K = "arabesque.kplex.k";
    private static final int MAXSIZE_DEFAULT = 2;
    private static final int K_DEFAULT = 1;

    int maxsize;
    int k;

    @Override
    public void init() {
        super.init();
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
        k = Configuration.get().getInteger(K, K_DEFAULT);
    }

    @Override
    public boolean filter(VertexInducedEmbedding embedding) {
        return isKplex(embedding);
    }

    public boolean isKplex(VertexInducedEmbedding embedding) {
        int numVertices = embedding.getNumVertices();

        // Check if it is a trivial k-plex
        if(numVertices <= k) {
            return true;
        } else {

            // Check if the last vertex added to the embedding has a minimum degree
            if(!(embedding.getNumEdgesAddedWithExpansion() >= numVertices - k)) {
                return false;
            }

            // Get embedding vertices and load main graph
            IntArrayList embVertices = embedding.getVertices();
            MainGraph graph = getMainGraph();

            // For each vertex count how many neighbours it has on the embedding
            HashMap<Integer, MutableInt> vertexDegree = new HashMap<Integer, MutableInt>();

            // Initialize HashMap values as 0
            for (int vertexId: embVertices) {
                vertexDegree.put(vertexId, new MutableInt(0));
            }

            for(int i = 0; i < numVertices; i++) {
                int vertexA = embVertices.get(i);

                for (int j = i + 1; j < numVertices; j++) {
                    int vertexB = embVertices.get(j);

                    if(graph.isNeighborVertex(vertexA, vertexB)) {
                        vertexDegree.get(vertexA).increment();
                        vertexDegree.get(vertexB).increment();
                    }
                }

                // Check if it is not a kplex
                if(!(vertexDegree.get(vertexA).toInteger() >= numVertices - k)) {
                    return false;
                }
            }

            return true;
        }
    }


    @Override
    public boolean shouldExpand(VertexInducedEmbedding embedding) {
        return embedding.getNumVertices() < maxsize;
    }


    @Override
    public void process(VertexInducedEmbedding embedding) {
        if (embedding.getNumVertices() == maxsize) {
            output(embedding);
        }
    }
}