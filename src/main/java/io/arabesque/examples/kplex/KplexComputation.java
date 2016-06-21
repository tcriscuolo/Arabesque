package io.arabesque.examples.kplex;

import io.arabesque.computation.VertexInducedComputation;
import io.arabesque.conf.Configuration;
import io.arabesque.embedding.VertexInducedEmbedding;
import io.arabesque.graph.MainGraph;
import io.arabesque.utils.collection.IntArrayList;


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
            // Get embedding vertices and edges

            IntArrayList embVertices = embedding.getVertices();
            //IntArrayList embEdges = embedding.getEdges();
            MainGraph graph = getMainGraph();
            // For each vertex count how many neighbours it has on the embedding
            for(int i = 0; i < embVertices.size(); i++) {
                int neighbourCount = 0;
                int vertexA = embVertices.get(i);
                for (int j = 0; j < embVertices.size(); j++) {
                    int vertexB = embVertices.get(j);

                    if(graph.isNeighborVertex(vertexA, vertexB)) {
                        neighbourCount++;
                    }
                }
                // Check if it is not a kplex
                if(!(neighbourCount >= numVertices - k)) {
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