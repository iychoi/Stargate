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

package stargate.server.cluster;

import stargate.commons.cluster.NodeAlreadyAddedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import stargate.commons.cluster.ACluster;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.NodeStatus;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.datastore.AReplicatedDataStore;
import stargate.commons.policy.ClusterPolicy;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.server.datastore.DataStoreManager;

/**
 *
 * @author iychoi
 */
public class LocalClusterManager extends ACluster {

    private static final Log LOG = LogFactory.getLog(LocalClusterManager.class);

    private static final String LOCALCLUSTERMANAGER_NODE_MAP_ID = LocalClusterManager.class.getName() + "_Node";
    private static final String LOCALCLUSTERMANAGER_NODE_STATUS_MAP_ID = LocalClusterManager.class.getName() + "_NodeStatus";
    
    private static LocalClusterManager instance;

    private DataStoreManager datastoreManager;
    
    private Node localNode;
    private AReplicatedDataStore node;
    private AReplicatedDataStore nodestatus;
    
    public static LocalClusterManager getInstance(DataStoreManager datastoreManager) {
        synchronized (LocalClusterManager.class) {
            if(instance == null) {
                instance = new LocalClusterManager(datastoreManager);
            }
            return instance;
        }
    }
    
    public static LocalClusterManager getInstance() throws ServiceNotStartedException {
        synchronized (LocalClusterManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("LocalClusterManager is not started");
            }
            return instance;
        }
    }
    
    public LocalClusterManager(DataStoreManager datastoreManager) {
        if(datastoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is empty or null");
        }
        
        this.datastoreManager = datastoreManager;
        
        this.node = this.datastoreManager.getReplicatedDataStore(LOCALCLUSTERMANAGER_NODE_MAP_ID, String.class, Node.class);
        this.nodestatus = this.datastoreManager.getReplicatedDataStore(LOCALCLUSTERMANAGER_NODE_STATUS_MAP_ID, String.class, NodeStatus.class);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @Override
    public synchronized String toString() {
        return "LocalCluster : " + this.name;
    }

    @JsonIgnore
    public synchronized Node getLocalNode() {
        return this.localNode;
    }
    
    @JsonIgnore
    public synchronized void setLocalNode(Node node) {
        this.localNode = node;
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @JsonIgnore
    public synchronized RemoteCluster toRemoteCluster() {
        try {
            RemoteCluster cluster = new RemoteCluster(this.name);
            
            Collection<Node> nodes = getNode();
            for(Node n : nodes) {
                try {
                    NodeStatus status = (NodeStatus) this.nodestatus.get(n.getName());
                    cluster.addNode(n, status);
                } catch (NodeAlreadyAddedException ex) {
                    LOG.error(ex);
                }
            }
            return cluster;
            
        } catch (Exception ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    @Override
    public synchronized int getNodeCount() {
        return this.node.size();
    }
    
    @Override
    public synchronized Collection<Node> getNode() throws IOException {
        List<Node> nodes = new ArrayList<Node>();
        Set<Object> keySet = this.node.keySet();
        for(Object key : keySet) {
            Node node = (Node) this.node.get(key);
            nodes.add(node);
        }

        return Collections.unmodifiableCollection(nodes);
    }
    
    @Override
    public synchronized Node getNode(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return (Node) this.node.get(name);
    }
    
    @Override
    public synchronized void addNode(Collection<Node> node) throws NodeAlreadyAddedException, IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        List<Node> failedNode = new ArrayList<Node>();
        
        for(Node n : node) {
            try {
                addNode(n);
            } catch(NodeAlreadyAddedException ex) {
                failedNode.add(n);
            }
        }
        
        if(!failedNode.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(Node n : failedNode) {
                if(sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(n.getName());
            }
            throw new NodeAlreadyAddedException("nodes (" + sb.toString() + ") are already added");
        }
    }
    
    @Override
    public synchronized void addNode(Node node) throws NodeAlreadyAddedException, IOException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }

        if(this.node.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + "is already added");
        }
        
        this.node.put(node.getName(), node);
        this.nodestatus.put(node.getName(), new NodeStatus(node));

        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @Override
    public synchronized Collection<NodeStatus> getNodeStatus() throws IOException {
        List<NodeStatus> nodestatuslist = new ArrayList<NodeStatus>();
        Set<Object> keySet = this.nodestatus.keySet();
        for(Object key : keySet) {
            NodeStatus status = (NodeStatus) this.nodestatus.get(key);
            nodestatuslist.add(status);
        }

        return Collections.unmodifiableCollection(nodestatuslist);
    }
    
    @Override
    public synchronized NodeStatus getNodeStatus(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return (NodeStatus) this.nodestatus.get(name);
    }
    
    @Override
    public synchronized void addNodeStatus(Collection<NodeStatus> status) throws IOException {
        if(status == null) {
            throw new IllegalArgumentException("status is null");
        }
        
        for(NodeStatus s : status) {
            addNodeStatus(s);
        }
    }
    
    @Override
    public synchronized void addNodeStatus(NodeStatus status) throws IOException {
        if(status == null || status.isEmpty()) {
            throw new IllegalArgumentException("status is empty or null");
        }
        
        if(this.nodestatus.containsKey(status.getNodeName())) {
            this.nodestatus.remove(status.getNodeName());
            this.nodestatus.put(status.getNodeName(), status);
            
            this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        }
    }

    @Override
    public synchronized void addNode(Node node, NodeStatus status) throws NodeAlreadyAddedException, IOException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.node.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + "is already added");
        }
        
        this.node.put(node.getName(), node);
        this.nodestatus.put(node.getName(), status);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @Override
    public synchronized void clearNode() {
        this.node.clear();
        this.nodestatus.clear();
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @Override
    public synchronized void removeNode(Node node) throws IOException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        removeNode(node.getName());
    }
    
    @Override
    public synchronized void removeNode(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.node.remove(name);
        this.nodestatus.remove(name);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @Override
    public synchronized boolean hasNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.node.containsKey(name);
    }
    
    @Override
    public synchronized boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        
        if(this.node == null || this.node.isEmpty()) {
            return true;
        }
        return false;
    }
    
    @Override
    public synchronized void reportNodeReachable(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(this.node.containsKey(name)) {
            try {
                long currentTime = DateTimeUtils.getCurrentTime();
                
                NodeStatus status = (NodeStatus) this.nodestatus.get(name);
                if(status.getLastUnreachableUpdateTime() < currentTime) {
                    status.setUnreachable(false);
                    status.setLastUnreachableUpdateTime(currentTime);
                    
                    // update
                    this.nodestatus.put(name, status);
                    
                    this.lastUpdateTime = currentTime;
                }
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
    }
    
    @Override
    public synchronized void reportNodeUnreachable(ClusterPolicy cp, String name) {
        if(cp == null) {
            throw new IllegalArgumentException("cp is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(this.node.containsKey(name)) {
            try {
                long currentTime = DateTimeUtils.getCurrentTime();

                NodeStatus status = (NodeStatus) this.nodestatus.get(name);
                long reportPeriod = cp.getUnreachableNodeReportMinInterval();
                
                if(DateTimeUtils.timeElapsedSecond(status.getLastUnreachableUpdateTime(), currentTime, reportPeriod)) {
                    status.setUnreachable(true);
                    status.setLastUnreachableUpdateTime(currentTime);
                        
                    if(!status.isBlacklisted()) {
                        int unreachableMaxCount = cp.getBlacklistNodeMaxUnreachableCount();
                        
                        status.increaseUnreachableCount();

                        if(status.getUnreachableCount() >= unreachableMaxCount) {
                            // exceed max => move to blacklist
                            status.setBlacklisted(true);
                            status.setBlacklistedTime(currentTime);
                        }
                    }
                    
                    // update
                    this.nodestatus.put(name, status);

                    this.lastUpdateTime = currentTime;
                }
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
    }
    
    @Override
    public synchronized void addNodeToBlacklist(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(this.node.containsKey(name)) {
            try {
                long currentTime = DateTimeUtils.getCurrentTime();

                NodeStatus status = (NodeStatus) this.nodestatus.get(name);
                if(!status.isBlacklisted()) {
                    status.setBlacklisted(true);
                    status.setBlacklistedTime(currentTime);

                    // update
                    this.nodestatus.put(name, status);

                    this.lastUpdateTime = currentTime;
                }
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
    }
    
    @Override
    public synchronized void removeNodeFromBlacklist(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(this.node.containsKey(name)) {
            try {
                long currentTime = DateTimeUtils.getCurrentTime();

                NodeStatus status = (NodeStatus) this.nodestatus.get(name);
                if(status.isBlacklisted()) {
                    status.setBlacklisted(false);
                    status.setBlacklistedTime(currentTime);

                    // update
                    this.nodestatus.put(name, status);

                    this.lastUpdateTime = currentTime;
                }
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
    }
}
