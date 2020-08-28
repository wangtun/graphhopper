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

class NodeBasedShortcutInserter {
    private final CHGraph chGraph;
    private final List<Shortcut> shortcuts;
    private final IntArrayList shortcutsByArcs;

    NodeBasedShortcutInserter(CHGraph chGraph) {
        this.chGraph = chGraph;
        this.shortcuts = new ArrayList<>();
        int origEdges = chGraph.getOriginalEdges();
        shortcutsByArcs = new IntArrayList(origEdges);
        for (int i = 0; i < origEdges; i++) {
            setShortcutForArc(i, i);
        }
    }

    public void startContractingNode() {
        shortcuts.clear();
    }

    public void addShortcut(int arc, int arcBwd, int node, int adjNode, int skipped1, int skipped2, int flags, double weight) {
        shortcuts.add(new Shortcut(arc, arcBwd, node, adjNode, skipped1, skipped2, flags, weight));
    }

    public void addShortcutWithUpdate(int arc, int arcBwd, int node, int adjNode, int skipped1, int skipped2, int flags, double weight) {
        // FOUND shortcut
        // but be sure that it is the only shortcut in the collection
        // and also in the graph for u->w. If existing AND identical weight => update setProperties.
        // Hint: shortcuts are always one-way due to distinct level of every node but we don't
        // know yet the levels so we need to determine the correct direction or if both directions
        boolean bidir = false;
        for (Shortcut sc : shortcuts) {
            if (sc.to == adjNode && Double.doubleToLongBits(sc.weight) == Double.doubleToLongBits(weight)) {
                if (getShortcutForArc(sc.skippedEdge1) == getShortcutForArc(skipped1) && getShortcutForArc(sc.skippedEdge2) == getShortcutForArc(skipped2)) {
                    if (sc.flags == PrepareEncoder.getScFwdDir()) {
                        sc.flags = PrepareEncoder.getScDirMask();
                        sc.arcBwd = arcBwd;
                        bidir = true;
                        break;
                    }
                }
            }
        }
        if (!bidir) {
            shortcuts.add(new Shortcut(-1, arcBwd, node, adjNode, skipped1, skipped2, flags, weight));
        }
    }

    /**
     * Actually writes the given shortcuts to the graph.
     *
     * @return the actual number of shortcuts that were added to the graph
     */
    public int finishContractingNode() {
        int shortcutCount = 0;
        for (Shortcut sc : shortcuts) {
            int scId = chGraph.shortcut(sc.from, sc.to, sc.flags, sc.weight, sc.skippedEdge1, sc.skippedEdge2);
            if (sc.flags == PrepareEncoder.getScFwdDir()) {
                setShortcutForArc(sc.arc, scId);
            } else if (sc.flags == PrepareEncoder.getScBwdDir()) {
                setShortcutForArc(sc.arcBwd, scId);
            } else {
                setShortcutForArc(sc.arc, scId);
                setShortcutForArc(sc.arcBwd, scId);
            }
            shortcutCount++;
        }
        return shortcutCount;
    }

    public void finishContraction() {
        AllCHEdgesIterator iter = chGraph.getAllEdges();
        while (iter.next()) {
            if (!iter.isShortcut())
                continue;
            int skip1 = getShortcutForArc(iter.getSkippedEdge1());
            int skip2 = getShortcutForArc(iter.getSkippedEdge2());
            iter.setSkippedEdges(skip1, skip2);
        }
    }

    private void setShortcutForArc(int arc, int shortcut) {
        if (arc >= shortcutsByArcs.size())
            shortcutsByArcs.resize(arc + 1);
        shortcutsByArcs.set(arc, shortcut);
    }

    private int getShortcutForArc(int arc) {
        return shortcutsByArcs.get(arc);
    }

    private static class Shortcut {
        int arc;
        int arcBwd;
        int from;
        int to;
        int skippedEdge1;
        int skippedEdge2;
        double weight;
        int flags;

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
        public String toString() {
            String str;
            if (flags == PrepareEncoder.getScDirMask())
                str = from + "<->";
            else
                str = from + "->";

            return str + to + ", weight:" + weight + " (" + skippedEdge1 + "," + skippedEdge2 + ")";
        }
    }
}
