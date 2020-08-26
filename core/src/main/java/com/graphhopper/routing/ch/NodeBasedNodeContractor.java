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
import com.graphhopper.routing.util.AllCHEdgesIterator;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.util.PMap;
import com.graphhopper.util.StopWatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static com.graphhopper.routing.ch.CHParameters.*;
import static com.graphhopper.util.Helper.nf;

class NodeBasedNodeContractor extends AbstractNodeContractor {
    private final List<Shortcut> shortcuts = new ArrayList<>();
    private final IntArrayList edgeMap = new IntArrayList();
    private final Params params = new Params();
    private PrepareCHEdgeExplorer allEdgeExplorer;
    private NodeBasedWitnessPathSearcher prepareAlgo;
    private int addedShortcutsCount;
    private long dijkstraCount;
    private StopWatch dijkstraSW = new StopWatch();
    // meanDegree is the number of edges / number of nodes ratio of the graph, not really the average degree, because
    // each edge can exist in both directions
    private double meanDegree;
    // temporary counters used for priority calculation
    private int originalEdgesCount;
    private int shortcutsCount;

    NodeBasedNodeContractor(PrepareCHGraph prepareGraph, PrepareGraph pg, PMap pMap) {
        super(prepareGraph, pg);
        extractParams(pMap);
    }

    private void extractParams(PMap pMap) {
        params.edgeDifferenceWeight = pMap.getFloat(EDGE_DIFFERENCE_WEIGHT, params.edgeDifferenceWeight);
        params.originalEdgesCountWeight = pMap.getFloat(ORIGINAL_EDGE_COUNT_WEIGHT, params.originalEdgesCountWeight);
        params.contractedNeighborsWeight = pMap.getFloat(CONTRACTED_NEIGHBORS_WEIGHT, params.contractedNeighborsWeight);
    }

    @Override
    public void initFromGraph() {
        super.initFromGraph();
        edgeMap.resize(prepareGraph.getOriginalEdges());
        for (int i = 0; i < prepareGraph.getOriginalEdges(); i++) {
            edgeMap.set(i, i);
        }
        allEdgeExplorer = prepareGraph.createAllEdgeExplorer();
        prepareAlgo = new NodeBasedWitnessPathSearcher(pg);
    }

    @Override
    public void prepareContraction() {
        // todo: initializing meanDegree here instead of in initFromGraph() means that in the first round of calculating
        // node priorities all shortcut searches are cancelled immediately and all possible shortcuts are counted because
        // no witness path can be found. this is not really what we want, but changing it requires re-optimizing the
        // graph contraction parameters, because it affects the node contraction order.
        // when this is done there should be no need for this method any longer.
        meanDegree = prepareGraph.getEdges() / prepareGraph.getNodes();
    }

    @Override
    public void close() {
        super.close();
        prepareAlgo.close();
    }

    /**
     * Warning: the calculated priority must NOT depend on priority(v) and therefore handleShortcuts should also not
     * depend on the priority(v). Otherwise updating the priority before contracting in contractNodes() could lead to
     * a slowish or even endless loop.
     */
    @Override
    public float calculatePriority(int node) {
        if (isContracted(node))
            throw new IllegalArgumentException("Priority should only be calculated for not yet contracted nodes");

        // # huge influence: the bigger the less shortcuts gets created and the faster is the preparation
        //
        // every adjNode has an 'original edge' number associated. initially it is r=1
        // when a new shortcut is introduced then r of the associated edges is summed up:
        // r(u,w)=r(u,v)+r(v,w) now we can define
        // originalEdgesCount = σ(v) := sum_{ (u,w) ∈ shortcuts(v) } of r(u, w)
        shortcutsCount = 0;
        originalEdgesCount = 0;
        handleShortcuts(node, this::countShortcut);

        // # lowest influence on preparation speed or shortcut creation count
        // (but according to paper should speed up queries)
        //
        // number of already contracted neighbors of v
        int contractedNeighbors = 0;
        PrepareCHEdgeIterator iter = allEdgeExplorer.setBaseNode(node);
        while (iter.next()) {
            if (!iter.isShortcut() && isContracted(iter.getAdjNode())) {
                contractedNeighbors++;
            }
        }

        // from shortcuts we can compute the edgeDifference
        // # low influence: with it the shortcut creation is slightly faster
        //
        // |shortcuts(v)| − |{(u, v) | v uncontracted}| − |{(v, w) | v uncontracted}|
        // meanDegree is used instead of outDegree+inDegree as if one adjNode is in both directions
        // only one bucket memory is used. Additionally one shortcut could also stand for two directions.
        int edgeDifference = shortcutsCount - pg.getDegree(node);

        // according to the paper do a simple linear combination of the properties to get the priority.
        return params.edgeDifferenceWeight * edgeDifference +
                params.originalEdgesCountWeight * originalEdgesCount;
    }

    public void remapSkipEdges() {
        AllCHEdgesIterator iter = prepareGraph.getAllEdges();
        while (iter.next()) {
            if (!iter.isShortcut())
                continue;
            int skip1 = edgeMap.get(iter.getSkippedEdge1());
            int skip2 = edgeMap.get(iter.getSkippedEdge2());
            iter.setSkippedEdges(skip1, skip2);
        }
    }

    @Override
    public void contractNode(int node) {
        shortcuts.clear();
        {
            PrepareGraph.PrepareGraphIterator iter = outEdgeExplorer.setBaseNode(node);
            while (iter.next()) {
                if (!iter.isShortcut())
                    continue;
                shortcuts.add(new Shortcut(iter.getArc(), -1, node, iter.getAdjNode(), iter.getSkipped1(), iter.getSkipped2(),
                        PrepareEncoder.getScFwdDir(), iter.getWeight()));
            }
        }
        {
            PrepareGraph.PrepareGraphIterator iter = inEdgeExplorer.setBaseNode(node);
            while (iter.next()) {
                if (!iter.isShortcut())
                    continue;
                int inOrigEdgeCount = 0;
                int outOrigEdgeCount = 0;
                boolean bidir = false;
                for (Shortcut sc : shortcuts) {
                    if (sc.to == iter.getAdjNode() && Double.doubleToLongBits(sc.weight) == Double.doubleToLongBits(iter.getWeight())) {
                        if (edgeMap.get(sc.skippedEdge1) == edgeMap.get(iter.getSkipped2()) && edgeMap.get(sc.skippedEdge2) == edgeMap.get(iter.getSkipped1())) {
                            if (sc.flags == PrepareEncoder.getScFwdDir()) {
                                sc.flags = PrepareEncoder.getScDirMask();
                                sc.arcBwd = iter.getArc();
                                bidir = true;
                                break;
                            }
                        }
                    }
                }
                if (!bidir) {
                    shortcuts.add(new Shortcut(-1, iter.getArc(), node, iter.getAdjNode(), iter.getSkipped2(), iter.getSkipped1(),
                            PrepareEncoder.getScBwdDir(), iter.getWeight()));
                }
            }
        }
        addedShortcutsCount += writeShortcuts(shortcuts);
        long degree = handleShortcuts(node, this::addOrUpdateShortcut);
        // put weight factor on meanDegree instead of taking the average => meanDegree is more stable
        meanDegree = (meanDegree * 2 + degree) / 3;
        pg.disconnect(node);
    }

    @Override
    public String getStatisticsString() {
        return String.format(Locale.ROOT, "meanDegree: %.2f, dijkstras: %10s, mem: %10s",
                meanDegree, nf(dijkstraCount), prepareAlgo.getMemoryUsageAsString());
    }

    /**
     * Searches for shortcuts and calls the given handler on each shortcut that is found. The graph is not directly
     * changed by this method.
     * Returns the 'degree' of the handler's node (disregarding edges from/to already contracted nodes). Note that
     * here the degree is not the total number of adjacent edges, but only the number of incoming edges
     */
    private long handleShortcuts(int node, ShortcutHandler handler) {
        int maxVisitedNodes = getMaxVisitedNodesEstimate();
        long degree = 0;
        PrepareGraph.PrepareGraphIterator incomingEdges = inEdgeExplorer.setBaseNode(node);
        // collect outgoing nodes (goal-nodes) only once
        while (incomingEdges.next()) {
            int fromNode = incomingEdges.getAdjNode();
            // do not consider loops at the node that is being contracted
            if (fromNode == node)
                continue;

            final double incomingEdgeWeight = incomingEdges.getWeight();
            // this check is important to prevent calling calcMillis on inaccessible edges and also allows early exit
            if (Double.isInfinite(incomingEdgeWeight)) {
                continue;
            }
            // collect outgoing nodes (goal-nodes) only once
            PrepareGraph.PrepareGraphIterator outgoingEdges = outEdgeExplorer.setBaseNode(node);
            // force fresh maps etc as this cannot be determined by from node alone (e.g. same from node but different avoidNode)
            prepareAlgo.clear();
            degree++;
            while (outgoingEdges.next()) {
                int toNode = outgoingEdges.getAdjNode();
                // do not consider loops at the node that is being contracted
                if (toNode == node || fromNode == toNode)
                    continue;

                // Limit weight as ferries or forbidden edges can increase local search too much.
                // If we decrease the correct weight we only explore less and introduce more shortcuts.
                // I.e. no change to accuracy is made.
                double existingDirectWeight = incomingEdgeWeight + outgoingEdges.getWeight();
                if (Double.isNaN(existingDirectWeight))
                    throw new IllegalStateException("Weighting should never return NaN values"
                            + ", in:" + getCoords(incomingEdges, prepareGraph.getNodeAccess()) + ", out:" + getCoords(outgoingEdges, prepareGraph.getNodeAccess()));

                if (Double.isInfinite(existingDirectWeight))
                    continue;

                prepareAlgo.setWeightLimit(existingDirectWeight);
                prepareAlgo.setMaxVisitedNodes(maxVisitedNodes);
                prepareAlgo.ignoreNode(node);

                dijkstraSW.start();
                dijkstraCount++;
                int endNode = prepareAlgo.findEndNode(fromNode, toNode);
                dijkstraSW.stop();

                // compare end node as the limit could force dijkstra to finish earlier
                if (endNode == toNode && prepareAlgo.getWeight(endNode) <= existingDirectWeight)
                    // FOUND witness path, so do not add shortcut
                    continue;

                handler.handleShortcut(fromNode, toNode, existingDirectWeight,
                        outgoingEdges.getArc(), outgoingEdges.getOrigEdgeCount(),
                        incomingEdges.getArc(), incomingEdges.getOrigEdgeCount());
            }
        }
        return degree;
    }

    /**
     * Actually writes the given shortcuts to the graph.
     *
     * @return the actual number of shortcuts that were added to the graph
     */
    private int writeShortcuts(Collection<Shortcut> shortcuts) {
        int tmpNewShortcuts = 0;
        for (Shortcut sc : shortcuts) {
            int scId = prepareGraph.shortcut(sc.from, sc.to, sc.flags, sc.weight, sc.skippedEdge1, sc.skippedEdge2);
            if (sc.flags == PrepareEncoder.getScFwdDir()) {
                if (sc.arc >= edgeMap.size())
                    edgeMap.resize(sc.arc + 1);
                edgeMap.set(sc.arc, scId);
            } else if (sc.flags == PrepareEncoder.getScBwdDir()) {
                if (sc.arcBwd >= edgeMap.size())
                    edgeMap.resize(sc.arcBwd + 1);
                edgeMap.set(sc.arcBwd, scId);
            } else {
                if (sc.arc >= edgeMap.size())
                    edgeMap.resize(sc.arc + 1);
                edgeMap.set(sc.arc, scId);
                if (sc.arcBwd >= edgeMap.size())
                    edgeMap.resize(sc.arcBwd + 1);
                edgeMap.set(sc.arcBwd, scId);
            }
            setOrigEdgeCount(scId, sc.originalEdges);

            tmpNewShortcuts++;
        }
        return tmpNewShortcuts;
    }

    private void countShortcut(int fromNode, int toNode, double existingDirectWeight,
                               int outgoingEdge, int outOrigEdgeCount,
                               int incomingEdge, int inOrigEdgeCount) {
        shortcutsCount++;
        originalEdgesCount += inOrigEdgeCount + outOrigEdgeCount;
    }

    private void addOrUpdateShortcut(int fromNode, int toNode, double existingDirectWeight,
                                     int outgoingEdge, int outOrigEdgeCount,
                                     int incomingEdge, int inOrigEdgeCount) {
        pg.addOrUpdateShortcut(fromNode, toNode, incomingEdge, outgoingEdge, existingDirectWeight, outOrigEdgeCount + inOrigEdgeCount);
    }

    private String getCoords(PrepareCHEdgeIterator edge, NodeAccess na) {
        int base = edge.getBaseNode();
        int adj = edge.getAdjNode();
        return base + "->" + adj + " (" + edge.getEdge() + "); "
                + na.getLat(base) + "," + na.getLon(base) + " -> " + na.getLat(adj) + "," + na.getLon(adj);
    }

    private String getCoords(PrepareGraph.PrepareGraphIterator edge, NodeAccess na) {
        int base = edge.getBaseNode();
        int adj = edge.getAdjNode();
        return base + "->" + adj + " (" + edge.getArc() + "); "
                + na.getLat(base) + "," + na.getLon(base) + " -> " + na.getLat(adj) + "," + na.getLon(adj);
    }

    @Override
    public long getAddedShortcutsCount() {
        return addedShortcutsCount;
    }

    @Override
    public long getDijkstraCount() {
        return dijkstraCount;
    }

    @Override
    public float getDijkstraSeconds() {
        return dijkstraSW.getCurrentSeconds();
    }

    private int getMaxVisitedNodesEstimate() {
        // todo: we return 0 here if meanDegree is < 1, which is not really what we want, but changing this changes
        // the node contraction order and requires re-optimizing the parameters of the graph contraction
        return (int) meanDegree * 100;
    }

    @FunctionalInterface
    private interface ShortcutHandler {
        void handleShortcut(int fromNode, int toNode, double existingDirectWeight,
                            int outgoingEdge, int outOrigEdgeCount,
                            int incomingEdge, int inOrigEdgeCount);
    }

    private static class Shortcut {
        int arc;
        int arcBwd;
        int from;
        int to;
        int skippedEdge1;
        int skippedEdge2;
        double weight;
        int originalEdges;
        int flags = PrepareEncoder.getScFwdDir();

        public Shortcut(int arc, int arcBwd, int from, int to, int skippedEdge1, int skippedEdge2, int flags, double weight) {
            this.arc = arc;
            this.arcBwd = arcBwd;
            this.from = from;
            this.to = to;
            this.skippedEdge1 = skippedEdge1;
            this.skippedEdge2 = skippedEdge2;
            this.flags = flags;
            this.weight = weight;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 23 * hash + from;
            hash = 23 * hash + to;
            return 23 * hash
                    + (int) (Double.doubleToLongBits(this.weight) ^ (Double.doubleToLongBits(this.weight) >>> 32));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass())
                return false;

            final Shortcut other = (Shortcut) obj;
            return this.from == other.from && this.to == other.to &&
                    Double.doubleToLongBits(this.weight) == Double.doubleToLongBits(other.weight);

        }

        @Override
        public String toString() {
            String str;
            if (flags == PrepareEncoder.getScDirMask())
                str = from + "<->";
            else
                str = from + "->";

            return str + to + ", weight:" + weight + " (" + skippedEdge1 + "," + skippedEdge2 + ")";
        }
    }

    public static class Params {
        // default values were optimized for Unterfranken
        private float edgeDifferenceWeight = 10;
        private float originalEdgesCountWeight = 1;
        private float contractedNeighborsWeight = 1;
    }

}
