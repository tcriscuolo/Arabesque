package io.arabesque.utils;

import io.arabesque.graph.MainGraph;
import io.arabesque.embedding.Embedding;

import io.arabesque.utils.collection.IntArrayList;

/**
 * This class is used to find articulation points in a subgraph, i.e., those
 * vertices in which their removal turn the subgraph disconnected.
 *
 * This implementation also garantees that no modifications are made to the
 * vertices and edges arrays.
 */
public class ArticulationPoints {
   static final int NIL = -1;

   // must be set by setEmbedding or individually
   private IntArrayList vertices;
   private IntArrayList edges;

   // are instantiated and reused for embeddings with the same size
   private boolean visited[];
   private int disc[];
   private int low[];
   private int parent[];
   private boolean ap[];

   private MainGraph mainGraph;

   int time;

   public ArticulationPoints(MainGraph mainGraph, int numVertices) {
      this.mainGraph = mainGraph;
      visited = new boolean[numVertices];
      disc = new int[numVertices];
      low = new int[numVertices];
      parent = new int[numVertices];
      ap = new boolean[numVertices];
   }

   private void setFromEmbedding(Embedding embedding) {
      assert embedding.getNumVertices() == visited.length;
      reset();
      this.vertices = embedding.getVertices();
      this.edges = embedding.getEdges();
   }

   public void reset() {
      time = 0;
      for (int u = 0; u < vertices.size(); ++u) {
         parent[u] = NIL;
         visited[u] = false;
         ap[u] = false;
      }
   }

   private void articulationPointsRec(int u) {
      // Count of children in DFS Tree
      int children = 0;

      // Mark the current node as visited
      visited[u] = true;

      // Initialize discovery time and low value
      disc[u] = low[u] = ++time;

      // Go through all vertices aadjacent to this
      for (int v = 0; v < vertices.size(); ++v) {
         if (!mainGraph.isNeighborVertex(vertices.getUnchecked(u),
                  vertices.getUnchecked(v)))
            continue;

         // If v is not visited yet, then make it a child of u
         // in DFS tree and recur for it
         if (!visited[v]) {
            children++;
            parent[v] = u;
            articulationPointsRec(v);

            // Check if the subtree rooted with v has a connection to
            // one of the ancestors of u
            low[u] = Math.min(low[u], low[v]);

            // u is an articulation point in following cases

            // (1) u is root of DFS tree and has two or more chilren.
            if (parent[u] == NIL && children > 1)
               ap[u] = true;

            // (2) If u is not root and low value of one of its child
            // is more than discovery value of u.
            if (parent[u] != NIL && low[v] >= disc[u])
               ap[u] = true;
         }

         // Update low value of u for parent function calls.
         else if (v != parent[u])
            low[u] = Math.min(low[u], disc[v]);
      }
   }

   public boolean[] articulationPoints(Embedding embedding) {
      setFromEmbedding(embedding);
      for (int u = 0; u < vertices.size(); ++u)
         if (visited[u] == false)
            articulationPointsRec(u);

      return ap;
   }

}
