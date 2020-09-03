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
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.graphhopper.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static com.graphhopper.routing.ch.CHParameters.*;
import static com.graphhopper.util.Helper.nf;

/**
 * This class is used to calculate the priority of or contract a given node in edge-based Contraction Hierarchies as it
 * is required to support turn-costs. This implementation follows the 'aggressive' variant described in
 * 'Efficient Routing in Road Networks with Turn Costs' by R. Geisberger and C. Vetter. Here, we do not store the center
 * node for each shortcut, but introduce helper shortcuts when a loop shortcut is encountered.
 * <p>
 * This class is mostly concerned with triggering the required local searches and introducing the necessary shortcuts
 * or calculating the node priority, while the actual searches for witness paths are delegated to
 * {@link EdgeBasedWitnessPathSearcher}.
 *
 * @author easbar
 */
class EdgeBasedNodeContractor implements NodeContractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeBasedNodeContractor.class);
    private final PrepareGraph prepareGraph;
    private final TurnCostFunction turnCostFunction;
    private PrepareGraph.PrepareGraphExplorer inEdgeExplorer;
    private PrepareGraph.PrepareGraphExplorer outEdgeExplorer;
    private PrepareGraph.PrepareGraphExplorer existingShortcutExplorer;
    private final PrepareShortcutHandler addingShortcutHandler = new AddingPrepareShortcutHandler();
    private final PrepareShortcutHandler countingShortcutHandler = new CountingPrepareShortcutHandler();
    private final ShortcutHandler shortcutHandler;
    private final Params params = new Params();
    private final PMap pMap;
    private PrepareShortcutHandler activeShortcutHandler;
    private final StopWatch dijkstraSW = new StopWatch();
    private final SearchStrategy activeStrategy = new AggressiveStrategy();
    private int[] hierarchyDepths;
    private EdgeBasedWitnessPathSearcher witnessPathSearcher;
    private PrepareGraph.BaseGraphExplorer sourceNodeOrigInEdgeExplorer;
    private PrepareGraph.BaseGraphExplorer targetNodeOrigOutEdgeExplorer;
    private PrepareGraph.BaseGraphExplorer loopAvoidanceInEdgeExplorer;
    private PrepareGraph.BaseGraphExplorer loopAvoidanceOutEdgeExplorer;

    // counts the total number of added shortcuts
    private int addedShortcutsCount;

    // edge counts used to calculate priority
    private int numShortcuts;
    private int numPrevEdges;
    private int numOrigEdges;
    private int numPrevOrigEdges;

    // counters used for performance analysis
    private int numPolledEdges;

    public EdgeBasedNodeContractor(PrepareGraph prepareGraph, EdgeBasedShortcutHandler shortcutHandler, TurnCostFunction turnCostFunction, PMap pMap) {
        this.prepareGraph = prepareGraph;
        this.turnCostFunction = turnCostFunction;
        this.shortcutHandler = shortcutHandler;
        this.pMap = pMap;
        extractParams(pMap);
    }

    private void extractParams(PMap pMap) {
        params.edgeQuotientWeight = pMap.getFloat(EDGE_QUOTIENT_WEIGHT, params.edgeQuotientWeight);
        params.originalEdgeQuotientWeight = pMap.getFloat(ORIGINAL_EDGE_QUOTIENT_WEIGHT, params.originalEdgeQuotientWeight);
        params.hierarchyDepthWeight = pMap.getFloat(HIERARCHY_DEPTH_WEIGHT, params.hierarchyDepthWeight);
    }

    @Override
    public void initFromGraph() {
        inEdgeExplorer = prepareGraph.createInEdgeExplorer();
        outEdgeExplorer = prepareGraph.createOutEdgeExplorer();
        existingShortcutExplorer = prepareGraph.createOutEdgeExplorer();
        witnessPathSearcher = new EdgeBasedWitnessPathSearcher(prepareGraph, turnCostFunction, pMap);
        sourceNodeOrigInEdgeExplorer = prepareGraph.createBaseInEdgeExplorer();
        targetNodeOrigOutEdgeExplorer = prepareGraph.createBaseOutEdgeExplorer();
        loopAvoidanceInEdgeExplorer = prepareGraph.createBaseInEdgeExplorer();
        loopAvoidanceOutEdgeExplorer = prepareGraph.createBaseOutEdgeExplorer();
        hierarchyDepths = new int[prepareGraph.getNodes()];
    }

    @Override
    public void prepareContraction() {
        // not needed
    }

    @Override
    public void finishContraction() {
        shortcutHandler.finishContraction();
    }

    @Override
    public float calculatePriority(int node) {
        activeShortcutHandler = countingShortcutHandler;
        stats().stopWatch.start();
        findAndHandleShortcuts(node);
        stats().stopWatch.stop();
        countPreviousEdges(node);
        // the higher the priority the later (!) this node will be contracted
        float edgeQuotient = numShortcuts / (float) numPrevEdges;
        float origEdgeQuotient = numOrigEdges / (float) numPrevOrigEdges;
        int hierarchyDepth = hierarchyDepths[node];
        float priority = params.edgeQuotientWeight * edgeQuotient +
                params.originalEdgeQuotientWeight * origEdgeQuotient +
                params.hierarchyDepthWeight * hierarchyDepth;
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("node: {}, eq: {} / {} = {}, oeq: {} / {} = {}, depth: {} --> {}",
                    node,
                    numShortcuts, numPrevEdges, edgeQuotient,
                    numOrigEdges, numPrevOrigEdges, origEdgeQuotient,
                    hierarchyDepth, priority);
        return priority;
    }

    @Override
    public IntSet contractNode(int node) {
        activeShortcutHandler = addingShortcutHandler;
        stats().stopWatch.start();
        findAndHandleShortcuts(node);
        shortcutHandler.startContractingNode();
        {
            PrepareGraph.PrepareGraphIterator iter = outEdgeExplorer.setBaseNode(node);
            while (iter.next()) {
                if (!iter.isShortcut())
                    continue;
                shortcutHandler.addShortcut(iter.getPrepareEdge(), node, iter.getAdjNode(),
                        GHUtility.getEdgeFromEdgeKey(iter.getOrigEdgeKeyFirst()), GHUtility.getEdgeFromEdgeKey(iter.getOrigEdgeKeyLast()),
                        iter.getSkipped1(), iter.getSkipped2(), iter.getWeight(), false);
            }
        }
        {
            PrepareGraph.PrepareGraphIterator iter = inEdgeExplorer.setBaseNode(node);
            while (iter.next()) {
                if (!iter.isShortcut())
                    continue;
                // we added loops using the outEdgeExplorer already above
                if (iter.getAdjNode() == node)
                    continue;
                shortcutHandler.addShortcut(iter.getPrepareEdge(), node, iter.getAdjNode(),
                        GHUtility.getEdgeFromEdgeKey(iter.getOrigEdgeKeyFirst()), GHUtility.getEdgeFromEdgeKey(iter.getOrigEdgeKeyLast()),
                        iter.getSkipped1(), iter.getSkipped2(), iter.getWeight(), true);
            }
        }
        shortcutHandler.finishContractingNode();
        // note that we do not disconnect original edges, because we are re-using the base graph for different profiles,
        // even though this is not optimal from a speed performance point of view.
        IntSet neighbors = prepareGraph.disconnect(node);
        updateHierarchyDepthsOfNeighbors(node, neighbors);
        stats().stopWatch.stop();
        return neighbors;
    }

    @Override
    public long getAddedShortcutsCount() {
        return addedShortcutsCount;
    }

    @Override
    public long getDijkstraCount() {
        return witnessPathSearcher.getTotalNumSearches();
    }

    @Override
    public float getDijkstraSeconds() {
        return dijkstraSW.getCurrentSeconds();
    }

    @Override
    public String getStatisticsString() {
        String result =
                "sc-handler-count: " + countingShortcutHandler.getStats() + ", " +
                        "sc-handler-contract: " + addingShortcutHandler.getStats() + ", " +
                        activeStrategy.getStatisticsString();
        activeStrategy.resetStats();
        return result;
    }

    public int getNumPolledEdges() {
        return numPolledEdges;
    }

    private void findAndHandleShortcuts(int node) {
        numPolledEdges = 0;
        activeStrategy.findAndHandleShortcuts(node);
    }

    private void countPreviousEdges(int node) {
        PrepareGraph.PrepareGraphIterator outIter = outEdgeExplorer.setBaseNode(node);
        while (outIter.next()) {
            numPrevEdges++;
            if (!outIter.isShortcut()) {
                numPrevOrigEdges++;
            } else {
                numPrevOrigEdges += outIter.getOrigEdgeCount();
            }
        }

        PrepareGraph.PrepareGraphIterator inIter = inEdgeExplorer.setBaseNode(node);
        while (inIter.next()) {
            // do not consider loop edges a second time
            if (inIter.getBaseNode() == inIter.getAdjNode())
                continue;
            numPrevEdges++;
            if (!inIter.isShortcut()) {
                numPrevOrigEdges++;
            } else {
                numPrevOrigEdges += inIter.getOrigEdgeCount();
            }
        }
    }

    private void updateHierarchyDepthsOfNeighbors(int node, IntSet neighbors) {
        int level = hierarchyDepths[node];
        for (IntCursor n : neighbors) {
            if (n.value == node)
                continue;
            hierarchyDepths[n.value] = Math.max(hierarchyDepths[n.value], level + 1);
        }
    }

    private void handleShortcuts(PrepareCHEntry chEntry, PrepareCHEntry root, int origEdgeCount) {
        LOGGER.trace("Adding shortcuts for target entry {}", chEntry);
        if (root.parent.adjNode == chEntry.adjNode &&
                //here we misuse root.parent.incEdge as first orig edge of the potential shortcut
                !loopShortcutNecessary(
                        chEntry.adjNode, GHUtility.getEdgeFromEdgeKey(root.getParent().incEdgeKey),
                        GHUtility.getEdgeFromEdgeKey(chEntry.incEdgeKey), chEntry.weight)) {
            stats().loopsAvoided++;
            return;
        }
        activeShortcutHandler.handleShortcut(root, chEntry, origEdgeCount);
    }

    /**
     * A given potential loop shortcut is only necessary if there is at least one pair of original in- & out-edges for
     * which taking the loop is cheaper than doing the direct turn. However this is almost always the case, because
     * doing a u-turn at any of the incoming edges is forbidden, i.e. the costs of the direct turn will be infinite.
     */
    private boolean loopShortcutNecessary(int node, int firstOrigEdge, int lastOrigEdge, double loopWeight) {
        // todo: loop avoidance does not seem to work for above mentioned reason, remove it?
        PrepareGraph.BaseGraphIterator inIter = loopAvoidanceInEdgeExplorer.setBaseNode(node);
        while (inIter.next()) {
            PrepareGraph.BaseGraphIterator outIter = loopAvoidanceOutEdgeExplorer.setBaseNode(node);
            double inTurnCost = getTurnCost(inIter.getEdge(), node, firstOrigEdge);
            while (outIter.next()) {
                double totalLoopCost = inTurnCost + loopWeight +
                        getTurnCost(lastOrigEdge, node, outIter.getEdge());
                double directTurnCost = getTurnCost(inIter.getEdge(), node, outIter.getEdge());
                if (totalLoopCost < directTurnCost) {
                    return true;
                }
            }
        }
        LOGGER.info("Loop avoidance -> no shortcut");
        return false;
    }

    private PrepareCHEntry addShortcut(PrepareCHEntry edgeFrom, PrepareCHEntry edgeTo, int origEdgeCount) {
        if (edgeTo.parent.prepareEdge != edgeFrom.prepareEdge) {
            // counting origEdgeCount correctly is tricky with loop shortcuts and this recursion, but it probably does
            // not matter that much
            PrepareCHEntry prev = addShortcut(edgeFrom, edgeTo.getParent(), origEdgeCount);
            return doAddShortcut(prev, edgeTo, origEdgeCount);
        } else {
            return doAddShortcut(edgeFrom, edgeTo, origEdgeCount);
        }
    }

    private PrepareCHEntry doAddShortcut(PrepareCHEntry edgeFrom, PrepareCHEntry edgeTo, int origEdgeCount) {
        int from = edgeFrom.parent.adjNode;
        int adjNode = edgeTo.adjNode;

        final PrepareGraph.PrepareGraphIterator iter = existingShortcutExplorer.setBaseNode(from);
        while (iter.next()) {
            if (!isSameShortcut(iter, adjNode, edgeFrom.getParent().incEdgeKey, edgeTo.incEdgeKey)) {
                // this is some other (shortcut) edge -> we do not care
                continue;
            }
            final double existingWeight = iter.getWeight();
            if (existingWeight <= edgeTo.weight) {
                // our shortcut already exists with lower weight --> do nothing
                PrepareCHEntry entry = new PrepareCHEntry(iter.getPrepareEdge(), iter.getOrigEdgeKeyLast(), adjNode, existingWeight);
                entry.parent = edgeFrom.parent;
                return entry;
            } else {
                // update weight
                iter.setSkippedEdges(edgeFrom.prepareEdge, edgeTo.prepareEdge);
                iter.setWeight(edgeTo.weight);
                iter.setOrigEdgeCount(origEdgeCount);
                PrepareCHEntry entry = new PrepareCHEntry(iter.getPrepareEdge(), iter.getOrigEdgeKeyLast(), adjNode, edgeTo.weight);
                entry.parent = edgeFrom.parent;
                return entry;
            }
        }

        // our shortcut is new --> add it
        // this is a bit of a hack, we misuse incEdgeKey of edgeFrom's parent to store the first orig edge
        int origFirstKey = edgeFrom.getParent().incEdgeKey;
        LOGGER.trace("Adding shortcut from {} to {}, weight: {}, firstOrigEdgeKey: {}, lastOrigEdgeKey: {}",
                from, adjNode, edgeTo.weight, origFirstKey, edgeTo.incEdgeKey);
        int prepareEdge = prepareGraph.addShortcut(from, adjNode, origFirstKey, edgeTo.incEdgeKey, edgeFrom.prepareEdge, edgeTo.prepareEdge, edgeTo.weight, origEdgeCount);
        addedShortcutsCount++;
        // does not matter here
        int incEdgeKey = -1;
        PrepareCHEntry entry = new PrepareCHEntry(prepareEdge, incEdgeKey, edgeTo.adjNode, edgeTo.weight);
        entry.parent = edgeFrom.parent;
        return entry;
    }

    private boolean isSameShortcut(PrepareGraph.PrepareGraphIterator iter, int adjNode, int firstOrigEdgeKey, int lastOrigEdgeKey) {
        return iter.isShortcut()
                && (iter.getAdjNode() == adjNode)
                && (iter.getOrigEdgeKeyFirst() == firstOrigEdgeKey)
                && (iter.getOrigEdgeKeyLast() == lastOrigEdgeKey);
    }

    private double getTurnCost(int inEdge, int node, int outEdge) {
        return turnCostFunction.getTurnWeight(inEdge, node, outEdge);
    }

    private void resetEdgeCounters() {
        numShortcuts = 0;
        numPrevEdges = 0;
        numOrigEdges = 0;
        numPrevOrigEdges = 0;
    }

    @Override
    public void close() {
        witnessPathSearcher.close();
    }

    private Stats stats() {
        return activeShortcutHandler.getStats();
    }

    private interface PrepareShortcutHandler {

        void handleShortcut(PrepareCHEntry edgeFrom, PrepareCHEntry edgeTo, int origEdgeCount);

        Stats getStats();

        String getAction();
    }

    private class AddingPrepareShortcutHandler implements PrepareShortcutHandler {
        private final Stats stats = new Stats();

        @Override
        public void handleShortcut(PrepareCHEntry edgeFrom, PrepareCHEntry edgeTo, int origEdgeCount) {
            addShortcut(edgeFrom, edgeTo, origEdgeCount);
        }

        @Override
        public Stats getStats() {
            return stats;
        }

        @Override
        public String getAction() {
            return "add";
        }
    }

    private class CountingPrepareShortcutHandler implements PrepareShortcutHandler {
        private final Stats stats = new Stats();

        @Override
        public void handleShortcut(PrepareCHEntry edgeFrom, PrepareCHEntry edgeTo, int origEdgeCount) {
            int fromNode = edgeFrom.parent.adjNode;
            int toNode = edgeTo.adjNode;
            int firstOrigEdgeKey = edgeFrom.getParent().incEdgeKey;
            int lastOrigEdgeKey = edgeTo.incEdgeKey;

            // check if this shortcut already exists
            final PrepareGraph.PrepareGraphIterator iter = existingShortcutExplorer.setBaseNode(fromNode);
            while (iter.next()) {
                if (isSameShortcut(iter, toNode, firstOrigEdgeKey, lastOrigEdgeKey)) {
                    // this shortcut exists already, maybe its weight will be updated but we should not count it as
                    // a new edge
                    return;
                }
            }

            // this shortcut is new --> increase counts
            numShortcuts++;
            numOrigEdges += origEdgeCount;
        }

        @Override
        public Stats getStats() {
            return stats;
        }

        @Override
        public String getAction() {
            return "count";
        }
    }

    public static class Params {
        // todo: optimize
        private float edgeQuotientWeight = 1;
        private float originalEdgeQuotientWeight = 3;
        private float hierarchyDepthWeight = 2;
    }

    private static class Stats {
        int nodes;
        long loopsAvoided;
        StopWatch stopWatch = new StopWatch();

        @Override
        public String toString() {
            return String.format(Locale.ROOT,
                    "time: %7.2fs, nodes-handled: %10s, loopsAvoided: %10s",
                    stopWatch.getCurrentSeconds(), nf(nodes), nf(loopsAvoided));
        }
    }

    private interface SearchStrategy {
        void findAndHandleShortcuts(int node);

        String getStatisticsString();

        void resetStats();

    }

    private class AggressiveStrategy implements SearchStrategy {
        private final IntSet sourceNodes = new IntHashSet(10);
        private final IntSet toNodes = new IntHashSet(10);
        private final LongSet addedShortcuts = new LongHashSet();

        @Override
        public String getStatisticsString() {
            return witnessPathSearcher.getStatisticsString();
        }

        @Override
        public void resetStats() {
            witnessPathSearcher.resetStats();
        }

        @Override
        public void findAndHandleShortcuts(int node) {
            LOGGER.trace("Finding shortcuts (aggressive) for node {}, required shortcuts will be {}ed", node, activeShortcutHandler.getAction());
            stats().nodes++;
            resetEdgeCounters();
            addedShortcuts.clear();

            // first we need to identify the possible source nodes from which we can reach the center node
            sourceNodes.clear();
            PrepareGraph.PrepareGraphIterator incomingEdges = inEdgeExplorer.setBaseNode(node);
            while (incomingEdges.next()) {
                int sourceNode = incomingEdges.getAdjNode();
                if (sourceNode == node) {
                    continue;
                }
                boolean isNewSourceNode = sourceNodes.add(sourceNode);
                if (!isNewSourceNode) {
                    continue;
                }
                // for each source node we need to look at every incoming original edge and find the initial entries
                PrepareGraph.BaseGraphIterator origInIter = sourceNodeOrigInEdgeExplorer.setBaseNode(sourceNode);
                while (origInIter.next()) {
                    int numInitialEntries = witnessPathSearcher.initSearch(node, sourceNode, GHUtility.getEdgeFromEdgeKey(origInIter.getOrigEdgeKeyLast()));
                    if (numInitialEntries < 1) {
                        continue;
                    }

                    // now we need to identify all target nodes that can be reached from the center node
                    toNodes.clear();
                    PrepareGraph.PrepareGraphIterator outgoingEdges = outEdgeExplorer.setBaseNode(node);
                    while (outgoingEdges.next()) {
                        int targetNode = outgoingEdges.getAdjNode();
                        if (targetNode == node) {
                            continue;
                        }
                        boolean isNewTargetNode = toNodes.add(targetNode);
                        if (!isNewTargetNode) {
                            continue;
                        }
                        // for each target edge outgoing from a target node we need to check if reaching it requires
                        // a 'bridge-path'
                        PrepareGraph.BaseGraphIterator targetEdgeIter = targetNodeOrigOutEdgeExplorer.setBaseNode(targetNode);
                        while (targetEdgeIter.next()) {
                            dijkstraSW.start();
                            PrepareCHEntry entry = witnessPathSearcher.runSearch(targetNode, GHUtility.getEdgeFromEdgeKey(targetEdgeIter.getOrigEdgeKeyFirst()));
                            dijkstraSW.stop();
                            if (entry == null || Double.isInfinite(entry.weight)) {
                                continue;
                            }
                            PrepareCHEntry root = entry.getParent();
                            while (EdgeIterator.Edge.isValid(root.parent.prepareEdge)) {
                                root = root.getParent();
                            }
                            // removing this 'optimization' improves contraction time, but introduces more
                            // shortcuts (makes slower queries). we are not detecting 'duplicate' shortcuts at a later
                            // stage especially when we are just running with the counting handler.
                            long addedShortcutKey = BitUtil.LITTLE.combineIntsToLong(root.getParent().incEdgeKey, entry.incEdgeKey);
                            if (!addedShortcuts.add(addedShortcutKey))
                                continue;
                            // root parent weight was misused to store initial turn cost here
                            double initialTurnCost = root.getParent().weight;
                            entry.weight -= initialTurnCost;
                            handleShortcuts(entry, root, incomingEdges.getOrigEdgeCount() + outgoingEdges.getOrigEdgeCount());
                        }
                    }
                    numPolledEdges += witnessPathSearcher.getNumPolledEdges();
                }
            }
        }
    }

    public interface ShortcutHandler {
        void startContractingNode();

        void addShortcut(int prepareEdge, int from, int to, int origEdgeFirst, int origEdgeLast, int skipped1, int skipped2, double weight, boolean reverse);

        void finishContractingNode();

        void finishContraction();
    }
}
