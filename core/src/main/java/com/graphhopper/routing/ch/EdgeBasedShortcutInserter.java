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
import com.graphhopper.storage.CHGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * Shortcut handler that inserts the given shortcuts into a CHGraph
 */
public class EdgeBasedShortcutInserter implements EdgeBasedNodeContractor.ShortcutHandler {
    private final CHGraph chGraph;
    private final int origEdges;
    private final List<Shortcut> shortcuts;
    private final IntArrayList shortcutsByPrepareEdges;

    public EdgeBasedShortcutInserter(CHGraph chGraph) {
        this.chGraph = chGraph;
        this.shortcuts = new ArrayList<>();
        this.origEdges = chGraph.getOriginalEdges();
        this.shortcutsByPrepareEdges = new IntArrayList();
    }

    @Override
    public void startContractingNode() {
        shortcuts.clear();
    }

    @Override
    public void addShortcut(int prepareEdge, int from, int to, int origEdgeFirst, int origEdgeLast, int skipped1, int skipped2, double weight, boolean reverse) {
        shortcuts.add(new Shortcut(prepareEdge, from, to, origEdgeFirst, origEdgeLast, skipped1, skipped2, weight, reverse));
    }

    @Override
    public int finishContractingNode() {
        int shortcutCount = 0;
        for (Shortcut sc : shortcuts) {
            int flags = sc.reverse ? PrepareEncoder.getScBwdDir() : PrepareEncoder.getScFwdDir();
            int scId = chGraph.shortcutEdgeBased(sc.from, sc.to, flags,
                    sc.weight, sc.skip1, sc.skip2, sc.origFirst, sc.origLast);
            shortcutCount++;
            setShortcutForPrepareEdge(sc.prepareEdge, scId);
        }
        return shortcutCount;
    }

    @Override
    public void finishContraction() {
        // during contraction the skip1/2 edges of shortcuts refer to the prepare edge-ids *not* the CHGraph (shortcut)
        // ids (because they are not known before the insertion) -> we need to re-map these ids here
        AllCHEdgesIterator iter = chGraph.getAllEdges();
        while (iter.next()) {
            if (!iter.isShortcut())
                continue;
            int skip1 = getShortcutForArc(iter.getSkippedEdge1());
            int skip2 = getShortcutForArc(iter.getSkippedEdge2());
            iter.setSkippedEdges(skip1, skip2);
        }
    }

    private void setShortcutForPrepareEdge(int prepareEdge, int shortcut) {
        int index = prepareEdge - origEdges;
        if (index >= shortcutsByPrepareEdges.size())
            shortcutsByPrepareEdges.resize(index + 1);
        shortcutsByPrepareEdges.set(index, shortcut);
    }

    private int getShortcutForArc(int prepareEdge) {
        if (prepareEdge < origEdges)
            return prepareEdge;
        int index = prepareEdge - origEdges;
        return shortcutsByPrepareEdges.get(index);
    }

    private static class Shortcut {
        private final int prepareEdge;
        private final int from;
        private final int to;
        private final int origFirst;
        private final int origLast;
        private final int skip1;
        private final int skip2;
        private final double weight;
        private final boolean reverse;

        public Shortcut(int prepareEdge, int from, int to, int origFirst, int origLast, int skip1, int skip2, double weight, boolean reverse) {
            this.prepareEdge = prepareEdge;
            this.from = from;
            this.to = to;
            this.origFirst = origFirst;
            this.origLast = origLast;
            this.skip1 = skip1;
            this.skip2 = skip2;
            this.weight = weight;
            this.reverse = reverse;
        }

        @Override
        public String toString() {
            return from + "-" + origFirst + "..." + origLast + "-" + to + " (" + skip1 + "," + skip2 + ")";
        }
    }
}
