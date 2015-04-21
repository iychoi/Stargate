package edu.arizona.cs.stargate.common;


import edu.arizona.cs.stargate.gatekeeper.cluster.ClusterNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

/*
 * The MIT License
 *
 * Copyright 2015 iychoi.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/**
 *
 * @author iychoi
 */
public class NodeSelectionUtils {
    
    public static ClusterNode selectBestNode(ClusterNode myNode, Collection<ClusterNode> remoteNodes) {
        Random random = new Random();
        ArrayList<ClusterNode> selectPool = new ArrayList<ClusterNode>();
        
        for(ClusterNode node : remoteNodes) {
            if(!node.isUnreachable()) {
                selectPool.add(node);
            }
        }
        
        int rnd = random.nextInt(selectPool.size());
        ClusterNode[] nodeArr = selectPool.toArray(new ClusterNode[0]);
        return nodeArr[rnd];
    }
}
