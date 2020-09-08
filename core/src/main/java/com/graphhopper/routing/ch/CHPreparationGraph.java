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

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import com.graphhopper.routing.ev.BooleanEncodedValue;
import com.graphhopper.routing.ev.DecimalEncodedValue;
import com.graphhopper.routing.ev.TurnCost;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.TurnCostStorage;
import com.graphhopper.util.BitUtil;
import com.graphhopper.util.EdgeIterator;
import com.graphhopper.util.GHUtility;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Graph data structure used for CH preparation. It allows caching weights and edges that are not needed anymore
 * (those adjacent to contracted nodes) can be removed (see {@link #disconnect}.
 */
public class CHPreparationGraph {
    private final int nodes;
    private final int edges;
    private final boolean edgeBased;
    private final TurnCostFunction turnCostFunction;
    private List<List<PrepareEdge>> outEdges;
    private List<List<PrepareEdge>> inEdges;
    private IntSet neighborSet;
    private OrigGraph origGraph;
    private OrigGraph.Builder origGraphBuilder;
    private int nextShortcutId;
    private boolean ready;

    private final int[] turnCostNodes;
    private LongArrayList turnCostEdgePairs;
    private DoubleArrayList turnCosts;

    public static CHPreparationGraph nodeBased(int nodes, int edges) {
        return new CHPreparationGraph(nodes, edges, false, (in, via, out) -> 0);
    }

    public static CHPreparationGraph edgeBased(int nodes, int edges, TurnCostFunction turnCostFunction) {
        return new CHPreparationGraph(nodes, edges, true, turnCostFunction);
    }

    /**
     * @param nodes (fixed) number of nodes of the graph
     * @param edges the maximum number of (non-shortcut) edges in this graph. edges-1 is the maximum edge id that may
     *              be used.
     */
    private CHPreparationGraph(int nodes, int edges, boolean edgeBased, TurnCostFunction turnCostFunction) {
        this.turnCostFunction = turnCostFunction;
        this.nodes = nodes;
        this.edges = edges;
        this.edgeBased = edgeBased;
        outEdges = IntStream.range(0, nodes).<List<PrepareEdge>>mapToObj(i -> new ArrayList<>(0)).collect(toList());
        inEdges = IntStream.range(0, nodes).<List<PrepareEdge>>mapToObj(i -> new ArrayList<>(0)).collect(toList());
        origGraphBuilder = edgeBased ? new OrigGraph.Builder() : null;
        neighborSet = new IntHashSet();
        nextShortcutId = edges;
        turnCostNodes = new int[nodes + 1];
        turnCostEdgePairs = new LongArrayList();
        turnCosts = new DoubleArrayList();
    }

    public static void buildFromGraph(CHPreparationGraph prepareGraph, Graph graph, Weighting weighting) {
        if (graph.getNodes() != prepareGraph.getNodes())
            throw new IllegalArgumentException("Cannot initialize from given graph. The number of nodes does not match: " +
                    graph.getNodes() + " vs. " + prepareGraph.getNodes());
        if (graph.getEdges() != prepareGraph.getOriginalEdges())
            throw new IllegalArgumentException("Cannot initialize from given graph. The number of edges does not match: " +
                    graph.getEdges() + " vs. " + prepareGraph.getOriginalEdges());
        BooleanEncodedValue accessEnc = weighting.getFlagEncoder().getAccessEnc();
        AllEdgesIterator iter = graph.getAllEdges();
        while (iter.next()) {
            double weightFwd = iter.get(accessEnc) ? weighting.calcEdgeWeight(iter, false) : Double.POSITIVE_INFINITY;
            double weightBwd = iter.getReverse(accessEnc) ? weighting.calcEdgeWeight(iter, true) : Double.POSITIVE_INFINITY;
            prepareGraph.addEdge(iter.getBaseNode(), iter.getAdjNode(), iter.getEdge(), weightFwd, weightBwd);
        }
        FlagEncoder encoder = weighting.getFlagEncoder();
        String key = TurnCost.key(encoder.toString());
        if (encoder.hasEncodedValue(key)) {
            DecimalEncodedValue turnCostEnc = encoder.getDecimalEncodedValue(key);
            TurnCostStorage tcStorage = graph.getTurnCostStorage();
            TurnCostStorage.TurnRelationIterator tcIter = tcStorage.getAllTurnRelations();
            int lastNode = -1;
            while (tcIter.next()) {
                int viaNode = tcIter.getViaNode();
                if (viaNode < lastNode)
                    throw new IllegalStateException();
                long edgePair = BitUtil.LITTLE.combineIntsToLong(tcIter.getFromEdge(), tcIter.getToEdge());
                double turnCost = tcIter.getCost(turnCostEnc);
                // todonow: do not forget that this is always infinite currently...
                int index = prepareGraph.turnCostEdgePairs.size();
                prepareGraph.turnCostEdgePairs.add(edgePair);
                prepareGraph.turnCosts.add(turnCost);
                if (viaNode != lastNode) {
                    for (int i = lastNode + 1; i <= viaNode; i++) {
                        prepareGraph.turnCostNodes[i] = index;
                    }
                }
                lastNode = viaNode;
            }
            prepareGraph.turnCostNodes[prepareGraph.turnCostNodes.length - 1] = prepareGraph.turnCostEdgePairs.size();
        }
        prepareGraph.prepareForContraction();
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

    public void addEdge(int from, int to, int edge, double weightFwd, double weightBwd) {
        checkNotReady();
        boolean fwd = Double.isFinite(weightFwd);
        boolean bwd = Double.isFinite(weightBwd);
        if (!fwd && !bwd)
            return;
        if (fwd) {
            int key = edge << 1;
            if (from > to)
                key += 1;
            PrepareEdge prepareEdge = new PrepareBaseEdge(edge, from, to, weightFwd, key);
            outEdges.get(from).add(prepareEdge);
            inEdges.get(to).add(prepareEdge);
        }
        if (bwd) {
            int key = edge << 1;
            if (to > from)
                key += 1;
            PrepareEdge prepareEdge = new PrepareBaseEdge(edge, to, from, weightBwd, key);
            outEdges.get(to).add(prepareEdge);
            inEdges.get(from).add(prepareEdge);
        }
        if (edgeBased)
            origGraphBuilder.addEdge(from, to, edge, fwd, bwd);
    }

    public int addShortcut(int from, int to, int origEdgeKeyFirst, int origEdgeKeyLast, int skipped1, int skipped2, double weight, int origEdgeCount) {
        checkReady();
        PrepareEdge prepareEdge = edgeBased
                ? new EdgeBasedPrepareShortcut(nextShortcutId, from, to, origEdgeKeyFirst, origEdgeKeyLast, weight, skipped1, skipped2, origEdgeCount)
                : new PrepareShortcut(nextShortcutId, from, to, weight, skipped1, skipped2, origEdgeCount);
        outEdges.get(from).add(prepareEdge);
        inEdges.get(to).add(prepareEdge);
        return nextShortcutId++;
    }

    public void prepareForContraction() {
        checkNotReady();
        origGraph = edgeBased ? origGraphBuilder.build() : null;
        origGraphBuilder = null;
        // todo: performance - maybe sort the edges in some clever way?
        ready = true;
    }

    public PrepareGraphEdgeExplorer createOutEdgeExplorer() {
        checkReady();
        return new PrepareGraphEdgeExplorerImpl(outEdges, false);
    }

    public PrepareGraphEdgeExplorer createInEdgeExplorer() {
        checkReady();
        return new PrepareGraphEdgeExplorerImpl(inEdges, true);
    }

    public PrepareGraphOrigEdgeExplorer createOutOrigEdgeExplorer() {
        checkReady();
        if (!edgeBased)
            throw new IllegalStateException("orig out explorer is not available for node-based graph");
        return origGraph.createOutOrigEdgeExplorer();
    }

    public PrepareGraphOrigEdgeExplorer createInOrigEdgeExplorer() {
        checkReady();
        if (!edgeBased)
            throw new IllegalStateException("orig in explorer is not available for node-based graph");
        return origGraph.createInOrigEdgeExplorer();
    }

    public double getTurnWeight(int inEdge, int viaNode, int outEdge) {
//        double checkTW = turnCostFunction.getTurnWeight(inEdge, viaNode, outEdge);
        double res = 0;
        if (!EdgeIterator.Edge.isValid(inEdge) || !EdgeIterator.Edge.isValid(outEdge))
            res = 0;
        else if (inEdge == outEdge) {
            // todonow: no! use u-turn costs!
            res = Double.POSITIVE_INFINITY;
        } else
            for (int i = turnCostNodes[viaNode]; i < turnCostNodes[viaNode + 1]; i++) {
                long l = turnCostEdgePairs.get(i);
                if (inEdge == BitUtil.LITTLE.getIntLow(l) && outEdge == BitUtil.LITTLE.getIntHigh(l)) {
                    res = turnCosts.get(i);
                    break;
                }
            }
//        if (checkTW != res) {
//            throw new IllegalStateException();
//        }
        return res;
    }

    public IntContainer disconnect(int node) {
        checkReady();
        // we use this neighbor set to guarantee a deterministic order of the returned
        // node ids
        neighborSet.clear();
        IntArrayList neighbors = new IntArrayList(getDegree(node));
        for (PrepareEdge prepareEdge : outEdges.get(node)) {
            int adjNode = prepareEdge.getTo();
            if (adjNode == node)
                continue;
            inEdges.get(adjNode).removeIf(a -> a == prepareEdge);
            if (neighborSet.add(adjNode))
                neighbors.add(adjNode);
        }
        for (PrepareEdge prepareEdge : inEdges.get(node)) {
            int adjNode = prepareEdge.getFrom();
            if (adjNode == node)
                continue;
            outEdges.get(adjNode).removeIf(a -> a == prepareEdge);
            if (neighborSet.add(adjNode))
                neighbors.add(adjNode);
        }
        outEdges.set(node, null);
        inEdges.set(node, null);
        return neighbors;
    }

    public void close() {
        checkReady();
        outEdges = null;
        inEdges = null;
        neighborSet = null;
        if (edgeBased)
            origGraph = null;
    }

    private void checkReady() {
        if (!ready)
            throw new IllegalStateException("You need to call prepareForContraction() before calling this method");
    }

    private void checkNotReady() {
        if (ready)
            throw new IllegalStateException("You cannot call this method after calling prepareForContraction()");
    }

    @FunctionalInterface
    public interface TurnCostFunction {
        double getTurnWeight(int inEdge, int viaNode, int outEdge);
    }

    private static class PrepareGraphEdgeExplorerImpl implements PrepareGraphEdgeExplorer, PrepareGraphEdgeIterator {
        private final List<List<PrepareEdge>> prepareEdges;
        private final boolean reverse;
        private List<PrepareEdge> prepareEdgesAtNode;
        private PrepareEdge currEdge;
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
            boolean result = index < prepareEdgesAtNode.size();
            currEdge = result ? prepareEdgesAtNode.get(index) : null;
            return result;
        }

        @Override
        public int getBaseNode() {
            return reverse ? currEdge.getTo() : currEdge.getFrom();
        }

        @Override
        public int getAdjNode() {
            return reverse ? currEdge.getFrom() : currEdge.getTo();
        }

        @Override
        public int getPrepareEdge() {
            return currEdge.getPrepareEdge();
        }

        @Override
        public boolean isShortcut() {
            return currEdge.isShortcut();
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            return currEdge.getOrigEdgeKeyFirst();
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return currEdge.getOrigEdgeKeyLast();
        }

        @Override
        public int getSkipped1() {
            return currEdge.getSkipped1();
        }

        @Override
        public int getSkipped2() {
            return currEdge.getSkipped2();
        }

        @Override
        public double getWeight() {
            return currEdge.getWeight();
        }

        @Override
        public int getOrigEdgeCount() {
            return currEdge.getOrigEdgeCount();
        }

        @Override
        public void setSkippedEdges(int skipped1, int skipped2) {
            currEdge.setSkipped1(skipped1);
            currEdge.setSkipped2(skipped2);
        }

        @Override
        public void setWeight(double weight) {
            assert Double.isFinite(weight);
            currEdge.setWeight(weight);
        }

        @Override
        public void setOrigEdgeCount(int origEdgeCount) {
            currEdge.setOrigEdgeCount(origEdgeCount);
        }

        @Override
        public String toString() {
            return index < 0 ? "not_started" : getBaseNode() + "-" + getAdjNode();
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
        private final int prepareEdge;
        private final int from;
        private final int to;
        private final double weight;
        private final int origKey;

        private PrepareBaseEdge(int prepareEdge, int from, int to, double weight, int origKey) {
            this.prepareEdge = prepareEdge;
            this.from = from;
            this.to = to;
            assert Double.isFinite(weight);
            this.weight = weight;
            this.origKey = origKey;
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
            return origKey;
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return origKey;
        }

        @Override
        public int getSkipped1() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSkipped2() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getOrigEdgeCount() {
            return 1;
        }

        @Override
        public void setSkipped1(int skipped1) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setSkipped2(int skipped2) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWeight(double weight) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOrigEdgeCount(int origEdgeCount) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return from + "-" + to + " (" + origKey + ") " + weight;
        }
    }

    private static class PrepareShortcut implements PrepareEdge {
        private final int prepareEdge;
        private final int from;
        private final int to;
        private double weight;
        private int skipped1;
        private int skipped2;
        private int origEdgeCount;

        private PrepareShortcut(int prepareEdge, int from, int to, double weight, int skipped1, int skipped2, int origEdgeCount) {
            this.prepareEdge = prepareEdge;
            this.from = from;
            this.to = to;
            assert Double.isFinite(weight);
            this.weight = weight;
            this.skipped1 = skipped1;
            this.skipped2 = skipped2;
            this.origEdgeCount = origEdgeCount;
        }

        @Override
        public boolean isShortcut() {
            return true;
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
            return getFrom() + "-" + getTo() + " (" + origEdgeKeyFirst + ", " + origEdgeKeyLast + ") " + getWeight();
        }
    }

    private static class OrigGraph {
        private final IntArrayList firstEdgesByNode;
        private final IntArrayList adjNodes;
        private final IntArrayList edgesAndFlags;

        private OrigGraph(IntArrayList firstEdgesByNode, IntArrayList adjNodes, IntArrayList edgesAndFlags) {
            this.firstEdgesByNode = firstEdgesByNode;
            this.adjNodes = adjNodes;
            this.edgesAndFlags = edgesAndFlags;
        }

        PrepareGraphOrigEdgeExplorer createOutOrigEdgeExplorer() {
            return new OrigEdgeIteratorImpl(this, false);
        }

        PrepareGraphOrigEdgeExplorer createInOrigEdgeExplorer() {
            return new OrigEdgeIteratorImpl(this, true);
        }

        static class Builder {
            private final IntArrayList fromNodes = new IntArrayList();
            private final IntArrayList toNodes = new IntArrayList();
            private final IntArrayList edgesAndFlags = new IntArrayList();
            private int maxFrom = -1;
            private int maxTo = -1;

            void addEdge(int from, int to, int edge, boolean fwd, boolean bwd) {
                fromNodes.add(from);
                toNodes.add(to);
                edgesAndFlags.add(getEdgeWithFlags(edge, fwd, bwd));
                maxFrom = Math.max(maxFrom, from);
                maxTo = Math.max(maxTo, to);

                fromNodes.add(to);
                toNodes.add(from);
                edgesAndFlags.add(getEdgeWithFlags(edge, bwd, fwd));
                maxFrom = Math.max(maxFrom, to);
                maxTo = Math.max(maxTo, from);
            }

            OrigGraph build() {
                int[] sortOrder = IndirectSort.mergesort(0, fromNodes.elementsCount, new IndirectComparator.AscendingIntComparator(fromNodes.buffer));
                sortAndTrim(fromNodes, sortOrder);
                sortAndTrim(toNodes, sortOrder);
                sortAndTrim(edgesAndFlags, sortOrder);
                return new OrigGraph(buildFirstEdgesByNode(), toNodes, edgesAndFlags);
            }

            private int getEdgeWithFlags(int edge, boolean fwd, boolean bwd) {
                // we use only 30 bits for the edge Id and store two access flags along with the same int
                if (edge >= Integer.MAX_VALUE >> 2)
                    throw new IllegalArgumentException("Maximum edge ID exceeded: " + Integer.MAX_VALUE);
                edge <<= 1;
                if (fwd)
                    edge++;
                edge <<= 1;
                if (bwd)
                    edge++;
                return edge;
            }

            private IntArrayList buildFirstEdgesByNode() {
                // it is assumed the edges have been sorted already
                final int numFroms = maxFrom + 1;
                IntArrayList firstEdgesByNode = new IntArrayList(numFroms + 1);
                firstEdgesByNode.elementsCount = numFroms + 1;
                int numEdges = fromNodes.size();
                if (numFroms == 0) {
                    firstEdgesByNode.set(0, numEdges);
                    return firstEdgesByNode;
                }
                int edgeIndex = 0;
                for (int from = 0; from < numFroms; from++) {
                    while (edgeIndex < numEdges && fromNodes.get(edgeIndex) < from) {
                        edgeIndex++;
                    }
                    firstEdgesByNode.set(from, edgeIndex);
                }
                firstEdgesByNode.set(numFroms, numEdges);
                return firstEdgesByNode;
            }

        }
    }

    private static class OrigEdgeIteratorImpl implements PrepareGraphOrigEdgeExplorer, PrepareGraphOrigEdgeIterator {
        private final OrigGraph graph;
        private final boolean reverse;
        private int node;
        private int endEdge;
        private int index;

        public OrigEdgeIteratorImpl(OrigGraph graph, boolean reverse) {
            this.graph = graph;
            this.reverse = reverse;
        }

        @Override
        public PrepareGraphOrigEdgeIterator setBaseNode(int node) {
            this.node = node;
            index = graph.firstEdgesByNode.get(node) - 1;
            endEdge = graph.firstEdgesByNode.get(node + 1);
            return this;
        }

        @Override
        public boolean next() {
            while (true) {
                index++;
                if (index >= endEdge)
                    return false;
                if (hasAccess())
                    return true;
            }
        }

        @Override
        public int getBaseNode() {
            return node;
        }

        @Override
        public int getAdjNode() {
            return graph.adjNodes.get(index);
        }

        @Override
        public int getOrigEdgeKeyFirst() {
            int e = graph.edgesAndFlags.get(index);
            return GHUtility.createEdgeKey(node, getAdjNode(), e >> 2, false);
        }

        @Override
        public int getOrigEdgeKeyLast() {
            return getOrigEdgeKeyFirst();
        }

        private boolean hasAccess() {
            int e = graph.edgesAndFlags.get(index);
            if (reverse) {
                return (e & 0b01) == 0b01;
            } else {
                return (e & 0b10) == 0b10;
            }
        }
    }

    private static void sortAndTrim(IntArrayList arr, int[] sortOrder) {
        arr.buffer = applySortOrder(sortOrder, arr.buffer);
        arr.elementsCount = arr.buffer.length;
    }

    private static int[] applySortOrder(int[] sortOrder, int[] arr) {
        if (sortOrder.length > arr.length) {
            throw new IllegalArgumentException("sort order must not be shorter than array");
        }
        int[] result = new int[sortOrder.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = arr[sortOrder[i]];
        }
        return result;
    }
}
