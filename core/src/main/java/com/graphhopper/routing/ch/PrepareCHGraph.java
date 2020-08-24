/*
 *  Licensed to GraphHopper GmbH under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for
 *  additional information regarding copyright ownership.
 *
 *  GraphHopper GmbH licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except in
 *  compliance with the License. You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.graphhopper.routing.ch;

import com.graphhopper.routing.util.AllCHEdgesIterator;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.storage.CHGraph;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.util.EdgeIterator;
import com.graphhopper.util.EdgeIteratorState;

/**
 * Helper adapter api over {@link CHGraph} used for CH preparation.
 */
public class PrepareCHGraph {
    private final CHGraph chGraph;
    private final Weighting weighting;

    public static PrepareCHGraph nodeBased(CHGraph chGraph, Weighting weighting) {
        if (chGraph.getCHConfig().isEdgeBased()) {
            throw new IllegalArgumentException("Expected node-based CHGraph, but was edge-based");
        }
        return new PrepareCHGraph(chGraph, weighting);
    }

    public static PrepareCHGraph edgeBased(CHGraph chGraph, Weighting weighting) {
        if (!chGraph.getCHConfig().isEdgeBased()) {
            throw new IllegalArgumentException("Expected edge-based CHGraph, but was node-based");
        }
        return new PrepareCHGraph(chGraph, weighting);
    }

    private PrepareCHGraph(CHGraph chGraph, Weighting weighting) {
        this.chGraph = chGraph;
        this.weighting = new PrecalculatedWeighting(weighting, chGraph.getBaseGraph());
    }

    public PrepareCHEdgeExplorer createInEdgeExplorer() {
        return PrepareCHEdgeIteratorImpl.inEdges(chGraph.createEdgeExplorer(), weighting);
    }

    public PrepareCHEdgeExplorer createOutEdgeExplorer() {
        return PrepareCHEdgeIteratorImpl.outEdges(chGraph.createEdgeExplorer(), weighting);
    }

    public PrepareCHEdgeExplorer createAllEdgeExplorer() {
        return PrepareCHEdgeIteratorImpl.allEdges(chGraph.createEdgeExplorer(), weighting);
    }

    public PrepareCHEdgeExplorer createOriginalInEdgeExplorer() {
        return PrepareCHEdgeIteratorImpl.inEdges(chGraph.createOriginalEdgeExplorer(), weighting);
    }

    public PrepareCHEdgeExplorer createOriginalOutEdgeExplorer() {
        return PrepareCHEdgeIteratorImpl.outEdges(chGraph.createOriginalEdgeExplorer(), weighting);
    }

    public int getNodes() {
        return chGraph.getNodes();
    }

    public int getEdges() {
        return chGraph.getEdges();
    }

    public int getOriginalEdges() {
        return chGraph.getOriginalEdges();
    }

    public int getLevel(int node) {
        return chGraph.getLevel(node);
    }

    public void setLevel(int node, int level) {
        chGraph.setLevel(node, level);
    }

    public int shortcut(int a, int b, int accessFlags, double weight, int skippedEdge1, int skippedEdge2) {
        return chGraph.shortcut(a, b, accessFlags, weight, skippedEdge1, skippedEdge2);
    }

    public int shortcutEdgeBased(int a, int b, int accessFlags, double weight, int skippedEdge1, int skippedEdge2, int origFirst, int origLast) {
        return chGraph.shortcutEdgeBased(a, b, accessFlags, weight, skippedEdge1, skippedEdge2, origFirst, origLast);
    }

    public int getOtherNode(int edge, int adjNode) {
        return chGraph.getOtherNode(edge, adjNode);
    }

    public NodeAccess getNodeAccess() {
        return chGraph.getBaseGraph().getNodeAccess();
    }

    double getTurnWeight(int inEdge, int viaNode, int outEdge) {
        return weighting.calcTurnWeight(inEdge, viaNode, outEdge);
    }

    public AllCHEdgesIterator getAllEdges() {
        return chGraph.getAllEdges();
    }

    boolean isReadyForContraction() {
        return chGraph.isReadyForContraction();
    }

    /**
     * Disconnects the edges (higher to lower node) via the specified edgeState pointing from lower to
     * higher node.
     * <p>
     *
     * @param edgeState the edge from lower to higher
     */
    public void disconnect(PrepareCHEdgeExplorer explorer, PrepareCHEdgeIterator edgeState) {
        // search edge with opposite direction but we need to know the previousEdge so we cannot simply do:
        // EdgeIteratorState tmpIter = getEdgeIteratorState(edgeState.getEdge(), edgeState.getBaseNode());
        PrepareCHEdgeIterator tmpIter = explorer.setBaseNode(edgeState.getAdjNode());
        int prevEdge = EdgeIterator.NO_EDGE;
        while (tmpIter.next()) {
            // note that we do not disconnect original edges, because we are re-using the base graph for different profiles,
            // even though this is not optimal from a speed performance point of view.
            if (tmpIter.isShortcut() && tmpIter.getEdge() == edgeState.getEdge()) {
                chGraph.disconnectEdge(edgeState.getEdge(), edgeState.getAdjNode(), prevEdge);
                break;
            }

            prevEdge = tmpIter.getEdge();
        }
    }

    private static class PrecalculatedWeighting implements Weighting {
        private final Weighting weighting;
        private final double[] fwdWeights;
        private final double[] bwdWeights;

        public PrecalculatedWeighting(Weighting weighting, Graph graph) {
            this.weighting = weighting;
            fwdWeights = new double[graph.getEdges()];
            bwdWeights = new double[graph.getEdges()];
            AllEdgesIterator iter = graph.getAllEdges();
            while (iter.next()) {
                // todo: for #1835 move the access check into the weighting
                fwdWeights[iter.getEdge()] = iter.get(weighting.getFlagEncoder().getAccessEnc()) ? weighting.calcEdgeWeight(iter, false) : Double.POSITIVE_INFINITY;
                bwdWeights[iter.getEdge()] = iter.getReverse(weighting.getFlagEncoder().getAccessEnc()) ? weighting.calcEdgeWeight(iter, true) : Double.POSITIVE_INFINITY;
            }
        }

        @Override
        public double getMinWeight(double distance) {
            return weighting.getMinWeight(distance);
        }

        @Override
        public double calcEdgeWeight(EdgeIteratorState edgeState, boolean reverse) {
            boolean fwd = edgeState.get(EdgeIteratorState.REVERSE_STATE) == reverse;
            return fwd ? fwdWeights[edgeState.getEdge()] : bwdWeights[edgeState.getEdge()];
        }

        @Override
        public long calcEdgeMillis(EdgeIteratorState edgeState, boolean reverse) {
            return weighting.calcEdgeMillis(edgeState, reverse);
        }

        @Override
        public double calcTurnWeight(int inEdge, int viaNode, int outEdge) {
            return weighting.calcTurnWeight(inEdge, viaNode, outEdge);
        }

        @Override
        public long calcTurnMillis(int inEdge, int viaNode, int outEdge) {
            return weighting.calcTurnMillis(inEdge, viaNode, outEdge);
        }

        @Override
        public boolean hasTurnCosts() {
            return weighting.hasTurnCosts();
        }

        @Override
        public FlagEncoder getFlagEncoder() {
            return weighting.getFlagEncoder();
        }

        @Override
        public String getName() {
            return weighting.getName();
        }
    }
}
