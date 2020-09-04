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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.graphhopper.routing.ev.BooleanEncodedValue;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.storage.Graph;
import com.graphhopper.util.GHUtility;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class PrepareGraph {
    private final int nodes;
    private final int edges;
    private final boolean edgeBased;
    private final TurnCostFunction turnCostFunction;
    private final List<List<PrepareEdge>> outEdges;
    private final List<List<PrepareEdge>> inEdges;
    private final List<List<PrepareOrigEdge>> outOrigEdges;
    private final List<List<PrepareOrigEdge>> inOrigEdges;
    private final IntSet neighborSet;
    private int nextShortcutId;

    public static PrepareGraph nodeBased(int nodes, int edges) {
        return new PrepareGraph(nodes, edges, false, (in, via, out) -> 0);
    }

    public static PrepareGraph edgeBased(int nodes, int edges, TurnCostFunction turnCostFunction) {
        return new PrepareGraph(nodes, edges, true, turnCostFunction);
    }

    /**
     * @param nodes (fixed) number of nodes of the graph
     * @param edges the maximum number of (non-shortcut) edges in this graph. edges-1 is the maximum edge id that may
     *              be used.
     */
    private PrepareGraph(int nodes, int edges, boolean edgeBased, TurnCostFunction turnCostFunction) {
        this.turnCostFunction = turnCostFunction;
        this.nodes = nodes;
        this.edges = edges;
        this.edgeBased = edgeBased;
        outEdges = IntStream.range(0, nodes).<List<PrepareEdge>>mapToObj(i -> new ArrayList<>(0)).collect(toList());
        inEdges = IntStream.range(0, nodes).<List<PrepareEdge>>mapToObj(i -> new ArrayList<>(0)).collect(toList());
        if (edgeBased) {
            outOrigEdges = IntStream.range(0, nodes).<List<PrepareOrigEdge>>mapToObj(i -> new ArrayList<>(0)).collect(toList());
            inOrigEdges = IntStream.range(0, nodes).<List<PrepareOrigEdge>>mapToObj(i -> new ArrayList<>(0)).collect(toList());
        } else {
            outOrigEdges = null;
            inOrigEdges = null;
        }
        neighborSet = new IntHashSet();
        nextShortcutId = edges;
    }

    public static void buildFromGraph(PrepareGraph prepareGraph, Graph graph, Weighting weighting) {
        if (graph.getNodes() != prepareGraph.getNodes())
            throw new IllegalArgumentException("Cannot initialize from given graph. The number of nodes does not match: " +
                    graph.getNodes() + " vs. " + prepareGraph.getNodes());
        if (graph.getEdges() != prepareGraph.getOriginalEdges())
            throw new IllegalArgumentException("Cannot initialize from given graph. The number of edges does not match: " +
                    graph.getEdges() + " vs. " + prepareGraph.getOriginalEdges());
        BooleanEncodedValue accessEnc = weighting.getFlagEncoder().getAccessEnc();
        AllEdgesIterator iter = graph.getAllEdges();
        while (iter.next()) {
            if (iter.get(accessEnc)) {
                double weight = weighting.calcEdgeWeight(iter, false);
                if (Double.isFinite(weight)) {
                    prepareGraph.addEdge(iter.getBaseNode(), iter.getAdjNode(), iter.getEdge(), weight);
                }
            }
            if (iter.getReverse(accessEnc)) {
                double weight = weighting.calcEdgeWeight(iter, true);
                if (Double.isFinite(weight)) {
                    prepareGraph.addEdge(iter.getAdjNode(), iter.getBaseNode(), iter.getEdge(), weight);
                }
            }
        }
        prepareGraph.trimToSize();
        // todo: performance - maybe sort the edges in some clever way?
    }

    public int getNodes() {
        return nodes;
    }

    public int getOriginalEdges() {
        return edges;
    }

    public int getDegree(int node) {
        return outEdges.get(node).size() + inEdges.get(node).size();
    }

    public void addEdge(int from, int to, int edge, double weight) {
        PrepareEdge prepareEdge = new PrepareBaseEdge(edge, from, to, weight);
        outEdges.get(from).add(prepareEdge);
        inEdges.get(to).add(prepareEdge);
        if (edgeBased) {
            int edgeKey = GHUtility.createEdgeKey(from, to, edge, false);
            PrepareOrigEdge prepareOrigEdge = new PrepareOrigEdge(edgeKey, from, to);
            outOrigEdges.get(from).add(prepareOrigEdge);
            inOrigEdges.get(to).add(prepareOrigEdge);
        }
    }

    public int addShortcut(int from, int to, int origEdgeKeyFirst, int origEdgeKeyLast, int skipped1, int skipped2, double weight, int origEdgeCount) {
        PrepareEdge prepareEdge = edgeBased
                ? new EdgeBasedPrepareShortcut(nextShortcutId, from, to, origEdgeKeyFirst, origEdgeKeyLast, weight, skipped1, skipped2, origEdgeCount)
                : new PrepareShortcut(nextShortcutId, from, to, weight, skipped1, skipped2, origEdgeCount);
        outEdges.get(from).add(prepareEdge);
        inEdges.get(to).add(prepareEdge);
        return nextShortcutId++;
    }

    public void trimToSize() {
        outEdges.forEach(l -> ((ArrayList) l).trimToSize());
        inEdges.forEach(l -> ((ArrayList) l).trimToSize());
        if (edgeBased) {
            outOrigEdges.forEach(l -> ((ArrayList) l).trimToSize());
            inOrigEdges.forEach(l -> ((ArrayList) l).trimToSize());
        }
    }

    public PrepareGraphEdgeExplorer createOutEdgeExplorer() {
        return new PrepareGraphEdgeExplorerImpl(outEdges, false);
    }

    public PrepareGraphEdgeExplorer createInEdgeExplorer() {
        return new PrepareGraphEdgeExplorerImpl(inEdges, true);
    }

    public PrepareGraphOrigEdgeExplorer createBaseOutEdgeExplorer() {
        if (!edgeBased)
            throw new IllegalStateException("base out explorer is not available for node-based graph");
        return new PrepareGraphOrigEdgeExplorerImpl(outOrigEdges, false);
    }

    public PrepareGraphOrigEdgeExplorer createBaseInEdgeExplorer() {
        if (!edgeBased)
            throw new IllegalStateException("base in explorer is not available for node-based graph");
        return new PrepareGraphOrigEdgeExplorerImpl(inOrigEdges, true);
    }

    public double getTurnWeight(int inEdge, int viaNode, int outEdge) {
        return turnCostFunction.getTurnWeight(inEdge, viaNode, outEdge);
    }

    public IntContainer disconnect(int node) {
        // we use this neighbor set to guarantee a deterministic order of the returned
        // node ids
        neighborSet.clear();
        IntArrayList neighbors = new IntArrayList(getDegree(node));
        for (PrepareEdge prepareEdge : outEdges.get(node)) {
            if (prepareEdge.getTo() == node)
                continue;
            inEdges.get(prepareEdge.getTo()).removeIf(a -> a == prepareEdge);
            if (neighborSet.add(prepareEdge.getTo()))
                neighbors.add(prepareEdge.getTo());
        }
        for (PrepareEdge prepareEdge : inEdges.get(node)) {
            if (prepareEdge.getFrom() == node)
                continue;
            outEdges.get(prepareEdge.getFrom()).removeIf(a -> a == prepareEdge);
            if (neighborSet.add(prepareEdge.getFrom()))
                neighbors.add(prepareEdge.getFrom());
        }
        outEdges.get(node).clear();
        inEdges.get(node).clear();
        return neighbors;
    }

    public void close() {
        outEdges.clear();
        inEdges.clear();
        if (edgeBased) {
            outOrigEdges.clear();
            inOrigEdges.clear();
        }
    }

    @FunctionalInterface
    public interface TurnCostFunction {
        double getTurnWeight(int inEdge, int viaNode, int outEdge);
    }

    private static class PrepareGraphEdgeExplorerImpl implements PrepareGraphEdgeExplorer, PrepareGraphEdgeIterator {
        private final List<List<PrepareEdge>> prepareEdges;
        private final boolean reverse;
        private List<PrepareEdge> prepareEdgesAtNode;
        private int index;

        PrepareGraphEdgeExplorerImpl(List<List<PrepareEdge>> prepareEdges, boolean reverse) {
            this.prepareEdges = prepareEdges;
            this.reverse = reverse;
        }

        @Override
        public PrepareGraphEdgeIterator setBaseNode(int node) {
            this.prepareEdgesAtNode = prepareEdges.get(node);
            this.index = -1;
            return this;
        }

        @Override
        public boolean next() {
            index++;
            return index < prepareEdgesAtNode.size();
        }

        @Override
        public int getBaseNode() {
            return reverse ? prepareEdgesAtNode.get(index).getTo() : prepareEdgesAtNode.get(index).getFrom();
        }

        @Override
        public int getAdjNode() {
            return reverse ? prepareEdgesAtNode.get(index).getFrom() : prepareEdgesAtNode.get(index).getTo();
        }

        @Override
        public int getPrepareEdge() {
            return prepareEdgesAtNode.get(index).getPrepareEdge();
        }

        @Override
        public boolean isShortcut() {
            return prepareEdgesAtNode.get(index).isShortcut();
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            return prepareEdgesAtNode.get(index).getOrigEdgeKeyFirst();
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return prepareEdgesAtNode.get(index).getOrigEdgeKeyLast();
        }

        @Override
        public int getSkipped1() {
            return prepareEdgesAtNode.get(index).getSkipped1();
        }

        @Override
        public int getSkipped2() {
            return prepareEdgesAtNode.get(index).getSkipped2();
        }

        @Override
        public double getWeight() {
            return prepareEdgesAtNode.get(index).getWeight();
        }

        @Override
        public int getOrigEdgeCount() {
            return prepareEdgesAtNode.get(index).getOrigEdgeCount();
        }

        @Override
        public void setSkippedEdges(int skipped1, int skipped2) {
            prepareEdgesAtNode.get(index).setSkipped1(skipped1);
            prepareEdgesAtNode.get(index).setSkipped2(skipped2);
        }

        @Override
        public void setWeight(double weight) {
            assert Double.isFinite(weight);
            prepareEdgesAtNode.get(index).setWeight(weight);
        }

        @Override
        public void setOrigEdgeCount(int origEdgeCount) {
            prepareEdgesAtNode.get(index).setOrigEdgeCount(origEdgeCount);
        }

        @Override
        public String toString() {
            return index < 0 ? "not_started" : getBaseNode() + "-" + getAdjNode();
        }
    }

    private static class PrepareGraphOrigEdgeExplorerImpl implements PrepareGraphOrigEdgeExplorer, PrepareGraphOrigEdgeIterator {
        private final List<List<PrepareOrigEdge>> edges;
        private final boolean reverse;
        private List<PrepareOrigEdge> edgesAtNode;
        private int index;

        PrepareGraphOrigEdgeExplorerImpl(List<List<PrepareOrigEdge>> edges, boolean reverse) {
            this.edges = edges;
            this.reverse = reverse;
        }

        @Override
        public PrepareGraphOrigEdgeIterator setBaseNode(int node) {
            this.edgesAtNode = edges.get(node);
            this.index = -1;
            return this;
        }

        @Override
        public boolean next() {
            index++;
            return index < edgesAtNode.size();
        }

        @Override
        public int getBaseNode() {
            return reverse ? edgesAtNode.get(index).adjNode : edgesAtNode.get(index).baseNode;
        }

        @Override
        public int getAdjNode() {
            return reverse ? edgesAtNode.get(index).baseNode : edgesAtNode.get(index).adjNode;
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            return reverse ? GHUtility.reverseEdgeKey(edgesAtNode.get(index).edgeKey) : edgesAtNode.get(index).edgeKey;
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return getOrigEdgeKeyFirst();
        }

        @Override
        public String toString() {
            return GHUtility.getEdgeFromEdgeKey(getOrigEdgeKeyFirst()) + ": " + getBaseNode() + "-" + getAdjNode();
        }
    }

    private interface PrepareEdge {
        boolean isShortcut();

        int getPrepareEdge();

        int getFrom();

        int getTo();

        double getWeight();

        int getOrigEdgeKeyFirst();

        int getOrigEdgeKeyLast();

        int getSkipped1();

        int getSkipped2();

        int getOrigEdgeCount();

        void setSkipped1(int skipped1);

        void setSkipped2(int skipped2);

        void setWeight(double weight);

        void setOrigEdgeCount(int origEdgeCount);
    }

    private static class PrepareBaseEdge implements PrepareEdge {
        final int prepareEdge;
        final int from;
        final int to;
        double weight;

        public PrepareBaseEdge(int prepareEdge, int from, int to, double weight) {
            this.prepareEdge = prepareEdge;
            this.from = from;
            this.to = to;
            assert Double.isFinite(weight);
            this.weight = weight;
        }

        @Override
        public boolean isShortcut() {
            return false;
        }

        @Override
        public int getPrepareEdge() {
            return prepareEdge;
        }

        @Override
        public int getFrom() {
            return from;
        }

        @Override
        public int getTo() {
            return to;
        }

        @Override
        public double getWeight() {
            return weight;
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            return GHUtility.createEdgeKey(from, to, prepareEdge, false);
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return GHUtility.createEdgeKey(from, to, prepareEdge, false);
        }

        @Override
        public int getSkipped1() {
            return throwNotAShortcut();
        }

        @Override
        public int getSkipped2() {
            return throwNotAShortcut();
        }

        @Override
        public int getOrigEdgeCount() {
            return 1;
        }

        @Override
        public void setSkipped1(int skipped1) {
            throwNotAShortcut();
        }

        @Override
        public void setSkipped2(int skipped2) {
            throwNotAShortcut();
        }

        @Override
        public void setWeight(double weight) {
            throwNotAShortcut();
        }

        @Override
        public void setOrigEdgeCount(int origEdgeCount) {
            throwNotAShortcut();
        }

        private int throwNotAShortcut() {
            throw new IllegalStateException("This is not a shortcut");
        }
    }

    private static class PrepareShortcut extends PrepareBaseEdge {
        private int skipped1;
        private int skipped2;
        private int origEdgeCount;

        private PrepareShortcut(int prepareEdge, int from, int to, double weight, int skipped1, int skipped2, int origEdgeCount) {
            super(prepareEdge, from, to, weight);
            this.skipped1 = skipped1;
            this.skipped2 = skipped2;
            this.origEdgeCount = origEdgeCount;
        }

        @Override
        public boolean isShortcut() {
            return true;
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            throw new IllegalStateException("Not supported for node-based shortcuts");
        }

        @Override
        public int getOrigEdgeKeyLast() {
            throw new IllegalStateException("Not supported for node-based shortcuts");
        }

        @Override
        public int getSkipped1() {
            return skipped1;
        }

        @Override
        public int getSkipped2() {
            return skipped2;
        }

        @Override
        public int getOrigEdgeCount() {
            return origEdgeCount;
        }

        @Override
        public void setSkipped1(int skipped1) {
            this.skipped1 = skipped1;
        }

        @Override
        public void setSkipped2(int skipped2) {
            this.skipped2 = skipped2;
        }

        @Override
        public void setWeight(double weight) {
            this.weight = weight;
        }

        @Override
        public void setOrigEdgeCount(int origEdgeCount) {
            this.origEdgeCount = origEdgeCount;
        }

        @Override
        public String toString() {
            return from + "-" + to + " " + weight;
        }
    }

    private static class EdgeBasedPrepareShortcut extends PrepareShortcut {
        // we use this subclass to save some memory for node-based where these are not needed
        private final int origEdgeKeyFirst;
        private final int origEdgeKeyLast;

        public EdgeBasedPrepareShortcut(int prepareEdge, int from, int to, int origEdgeKeyFirst, int origEdgeKeyLast,
                                        double weight, int skipped1, int skipped2, int origEdgeCount) {
            super(prepareEdge, from, to, weight, skipped1, skipped2, origEdgeCount);
            this.origEdgeKeyFirst = origEdgeKeyFirst;
            this.origEdgeKeyLast = origEdgeKeyLast;
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            return origEdgeKeyFirst;
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return origEdgeKeyLast;
        }

        @Override
        public String toString() {
            return from + "-" + to + " (" + origEdgeKeyFirst + ", " + origEdgeKeyLast + ") " + weight;
        }
    }

    private static class PrepareOrigEdge {
        private final int edgeKey;
        private final int baseNode;
        private final int adjNode;

        PrepareOrigEdge(int edgeKey, int baseNode, int adjNode) {
            this.edgeKey = edgeKey;
            this.baseNode = baseNode;
            this.adjNode = adjNode;
        }
    }
}
