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
package stargate.commons.utils;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.NodeStatus;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.transport.TransportServiceInfo;

/**
 *
 * @author iychoi
 */
public class NodeUtils {
    public static Node createNode(Class driverClass, URI connectionUri) throws URISyntaxException, IOException {
        if(driverClass == null) {
            throw new IllegalArgumentException("driverClass is null");
        }
        
        if(connectionUri == null) {
            throw new IllegalArgumentException("connectionUri is null");
        }
        
        String nodeName = null;
        try {
            nodeName = "Node_" + NameUtils.generateMachineID();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        
        TransportServiceInfo serviceInfo = new TransportServiceInfo(driverClass.getCanonicalName(), connectionUri);
        
        return new Node(nodeName, serviceInfo, IPUtils.getHostAddress());
    }
    
    public static boolean isLocalNode(Node node) throws IOException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        List<String> nodeAddr = new ArrayList<String>();
        nodeAddr.add(node.getName());
        nodeAddr.addAll(node.getHostName());
        
        Collection<String> hostAddress = IPUtils.getHostAddress();
        for(String addr : hostAddress) {
            if(nodeAddr.contains(addr)) {
                return true;
            }
        }
        return false;
    }
    
    public static Collection<Node> getLiveNode(RemoteCluster remoteCluster) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        List<String> liveNodeName = new ArrayList<String>();
        Collection<NodeStatus> nodeStatus = remoteCluster.getNodeStatus();
        for(NodeStatus ns : nodeStatus) {
            if(!ns.isBlacklisted() && !ns.isUnreachable()) {
                liveNodeName.add(ns.getNodeName());
            }
        }
        
        List<Node> liveNodeList = new ArrayList<Node>();
        for(String nodeName : liveNodeName) {
            Node node = remoteCluster.getNode(nodeName);
            liveNodeList.add(node);
        }
        
        return Collections.unmodifiableCollection(liveNodeList);
    }
    
    public static Collection<Node> getRandomContactNodeList(RemoteCluster remoteCluster) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        Collection<Node> liveNode = getLiveNode(remoteCluster);
        if(liveNode == null || liveNode.isEmpty()) {
            throw new IOException("no live node found at remote cluster " + remoteCluster.getName());
        }
        
        List<Node> contactOrder = new ArrayList<Node>();
        contactOrder.addAll(liveNode);
        
        Random r = new Random();
        Collections.rotate(contactOrder, r.nextInt(contactOrder.size()));
        return Collections.unmodifiableCollection(contactOrder);
    }
    
    public static Collection<Node> getLocalClusterAwareContactNodeList(Collection<Node> localClusterNode, Node localNode, RemoteCluster remoteCluster) throws IOException {
        if(localClusterNode == null || localClusterNode.isEmpty()) {
            throw new IllegalArgumentException("localClusterNode is null or empty");
        }
        
        if(localNode == null || localNode.isEmpty()) {
            throw new IllegalArgumentException("localNode is null or empty");
        }
        
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        Collection<Node> liveNode = getLiveNode(remoteCluster);
        if(liveNode == null || liveNode.isEmpty()) {
            throw new IOException("no live node found at remote cluster " + remoteCluster.getName());
        }
        
        int localNodePosition = 0;
        for(Node n : localClusterNode) {
            if(n.getName().equals(localNode.getName())) {
                break;
            }
            localNodePosition++;
        }
        
        double targetNodeLen = (double)liveNode.size() / (double)localClusterNode.size();
        double targetNodeStart = targetNodeLen * localNodePosition;
        double targetNodeEnd = targetNodeStart + targetNodeLen;
        
        List<Node> remoteNode = new ArrayList<Node>();
        remoteNode.addAll(liveNode);
        
        List<Node> contactOrder = new ArrayList<Node>();
        List<Node> backups = new ArrayList<Node>();
        
        int targetNodeStartInt = (int)targetNodeStart;
        int targetNodeEndInt = (int)Math.ceil(targetNodeEnd);
        
        for(int i=0;i<=liveNode.size();i++) {
            if(i >= targetNodeStartInt && i <= targetNodeEndInt) {
                contactOrder.add(remoteNode.get(i));
            } else {
                backups.add(remoteNode.get(i));
            }
        }
        
        Collections.shuffle(contactOrder);
        Collections.shuffle(backups);
        
        contactOrder.addAll(backups);
        return Collections.unmodifiableCollection(contactOrder);
    }
}
