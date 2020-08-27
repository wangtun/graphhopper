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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.storage.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class PrepareGraph {
    private final Graph graph;
    private final Weighting weighting;
    private int nodes;
    private List<List<Arc>> outArcs;
    private List<List<Arc>> inArcs;
    private int arcs;

    public static PrepareGraph nodeBased(Graph graph, Weighting weighting) {
        return new PrepareGraph(graph, weighting);
    }

    public static PrepareGraph edgeBased(Graph graph, Weighting weighting) {
        return new PrepareGraph(graph, weighting);
    }

    private PrepareGraph(Graph graph, Weighting weighting) {
        this.graph = graph;
        this.weighting = weighting;
    }

    public void initFromGraph() {
        nodes = graph.getNodes();
        outArcs = IntStream.range(0, graph.getNodes()).<List<Arc>>mapToObj(i -> new ArrayList<>(3)).collect(toList());
        inArcs = IntStream.range(0, graph.getNodes()).<List<Arc>>mapToObj(i -> new ArrayList<>(3)).collect(toList());
        AllEdgesIterator iter = graph.getAllEdges();
        while (iter.next()) {
            if (iter.get(weighting.getFlagEncoder().getAccessEnc())) {
                double weight = weighting.calcEdgeWeight(iter, false);
                if (Double.isFinite(weight)) {
                    addEdge(iter.getBaseNode(), iter.getAdjNode(), iter.getEdge(), weight);
                }
            }
            if (iter.getReverse(weighting.getFlagEncoder().getAccessEnc())) {
                double weight = weighting.calcEdgeWeight(iter, true);
                if (Double.isFinite(weight)) {
                    addEdge(iter.getAdjNode(), iter.getBaseNode(), iter.getEdge(), weight);
                }
            }
        }
        arcs = graph.getEdges();
    }

    public int getNodes() {
        return nodes;
    }

    public int getDegree(int node) {
        return outArcs.get(node).size() + inArcs.get(node).size();
    }

    public void addEdge(int from, int to, int edge, double weight) {
        outArcs.get(from).add(Arc.edge(edge, to, edge, weight));
        inArcs.get(to).add(Arc.edge(edge, from, edge, weight));
    }

    public int addShortcut(int from, int to, int origEdgeFirst, int origEdgeLast, int skipped1, int skipped2, double weight, int origEdgeCount) {
        outArcs.get(from).add(Arc.shortcut(arcs, to, origEdgeFirst, origEdgeLast, skipped1, skipped2, weight, origEdgeCount));
        inArcs.get(to).add(Arc.shortcut(arcs, from, origEdgeFirst, origEdgeLast, skipped1, skipped2, weight, origEdgeCount));
        arcs++;
        return arcs - 1;
    }

    public void addOrUpdateShortcut(int from, int to, int skipped1, int skipped2, double weight, int origEdgeCount) {
        boolean exists = false;
        for (Arc arc : outArcs.get(from)) {
            if (arc.adjNode == to && arc.isShortcut()) {
                exists = true;
                if (weight < arc.weight) {
                    arc.weight = weight;
                    arc.skipped1 = skipped1;
                    arc.skipped2 = skipped2;
                    arc.origEdgeCount = origEdgeCount;
                    for (Arc aa : inArcs.get(to)) {
                        if (aa.adjNode == from) {
                            aa.weight = weight;
                            aa.skipped1 = skipped1;
                            aa.skipped2 = skipped2;
                            aa.origEdgeCount = origEdgeCount;
                        }
                    }
                }
            }
        }
        if (!exists)
            addShortcut(from, to, -1, -1, skipped1, skipped2, weight, origEdgeCount);
    }

    public PrepareGraphExplorer createOutEdgeExplorer() {
        return new PrepareGraphExplorerImpl(outArcs);
    }

    public PrepareGraphExplorer createInEdgeExplorer() {
        return new PrepareGraphExplorerImpl(inArcs);
    }

    public IntSet disconnect(int node) {
        IntSet neighbors = new IntHashSet(getDegree(node));
        for (Arc arc : outArcs.get(node)) {
            inArcs.get(arc.adjNode).removeIf(a -> a.adjNode == node);
            neighbors.add(arc.adjNode);
        }
        for (Arc arc : inArcs.get(node)) {
            outArcs.get(arc.adjNode).removeIf(a -> a.adjNode == node);
            neighbors.add(arc.adjNode);
        }
        outArcs.get(node).clear();
        inArcs.get(node).clear();
        return neighbors;
    }

    public interface PrepareGraphExplorer {
        PrepareGraphIterator setBaseNode(int node);
    }

    public interface PrepareGraphIterator {
        boolean next();

        int getBaseNode();

        int getAdjNode();

        int getArc();

        boolean isShortcut();

        int getOrigEdgeFirst();

        int getOrigEdgeLast();

        int getSkipped1();

        int getSkipped2();

        int getEdge();

        double getWeight();

        int getOrigEdgeCount();

        void setSkippedEdges(int skipped1, int skipped2);

        void setWeight(double weight);
    }

    public static class PrepareGraphExplorerImpl implements PrepareGraphExplorer, PrepareGraphIterator {
        private final List<List<Arc>> arcs;
        private int node;
        private int index;

        PrepareGraphExplorerImpl(List<List<Arc>> arcs) {
            this.arcs = arcs;
        }

        @Override
        public PrepareGraphIterator setBaseNode(int node) {
            this.node = node;
            this.index = -1;
            return this;
        }

        @Override
        public boolean next() {
            index++;
            return index < arcs.get(node).size();
        }

        @Override
        public int getBaseNode() {
            return node;
        }

        @Override
        public int getAdjNode() {
            return arcs.get(node).get(index).adjNode;
        }

        @Override
        public int getArc() {
            int arc = arcs.get(node).get(index).arc;
            if (!isShortcut()) {
                assert getEdge() == arc;
            }
            return arc;
        }

        @Override
        public boolean isShortcut() {
            return arcs.get(node).get(index).isShortcut();
        }

        @Override
        public int getOrigEdgeFirst() {
            return arcs.get(node).get(index).origEdgeFirst;
        }

        @Override
        public int getOrigEdgeLast() {
            return arcs.get(node).get(index).origEdgeLast;
        }

        @Override
        public int getSkipped1() {
            return arcs.get(node).get(index).skipped1;
        }

        @Override
        public int getSkipped2() {
            return arcs.get(node).get(index).skipped2;
        }

        @Override
        public int getEdge() {
            assert !isShortcut();
            return arcs.get(node).get(index).origEdgeFirst;
        }

        @Override
        public double getWeight() {
            return arcs.get(node).get(index).weight;
        }

        @Override
        public int getOrigEdgeCount() {
            return arcs.get(node).get(index).origEdgeCount;
        }

        @Override
        public void setSkippedEdges(int skipped1, int skipped2) {
            arcs.get(node).get(index).skipped1 = skipped1;
            arcs.get(node).get(index).skipped2 = skipped2;
        }

        @Override
        public void setWeight(double weight) {
            arcs.get(node).get(index).weight = weight;
        }

        @Override
        public String toString() {
            return getBaseNode() + "-" + getAdjNode();
        }
    }

    public static class Arc {
        private final int arc;
        private final int adjNode;
        private double weight;
        private final int origEdgeFirst;
        private final int origEdgeLast;
        private int skipped1;
        private int skipped2;
        private int origEdgeCount;

        private static Arc edge(int arc, int adjNode, int edge, double weight) {
            return new Arc(arc, adjNode, weight, edge, edge, -1, -1, 1);
        }

        private static Arc shortcut(int arc, int adjNode, int origEdgeFirst, int origEdgeLast, int skipped1, int skipped2, double weight, int origEdgeCount) {
            return new Arc(arc, adjNode, weight, origEdgeFirst, origEdgeLast, skipped1, skipped2, origEdgeCount);
        }

        private Arc(int arc, int adjNode, double weight, int origEdgeFirst, int origEdgeLast, int skipped1, int skipped2, int origEdgeCount) {
            this.arc = arc;
            this.adjNode = adjNode;
            this.weight = weight;
            this.origEdgeFirst = origEdgeFirst;
            this.origEdgeLast = origEdgeLast;
            this.skipped1 = skipped1;
            this.skipped2 = skipped2;
            this.origEdgeCount = origEdgeCount;
        }

        public boolean isShortcut() {
            return skipped1 != -1;
        }

        @Override
        public String toString() {
            return "-- " + adjNode + " (" + origEdgeFirst + ", " + origEdgeLast + ") " + weight;
        }
    }
}
