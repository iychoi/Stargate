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

package edu.arizona.cs.stargate.gatekeeper.cluster;

import edu.arizona.cs.stargate.gatekeeper.distributedcache.JsonReplicatedMap;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.distributedcache.DistributedCacheService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class LocalClusterManager {

    private static final Log LOG = LogFactory.getLog(LocalClusterManager.class);
    
    private static final String LOCALCLUSTERMANAGER_MAP_ID = "LocalClusterManager";
    
    private static LocalClusterManager instance;
    
    private String name;
    private ClusterNode localNode;
    private JsonReplicatedMap<String, ClusterNode> nodes;
    private ArrayList<ILocalClusterConfigurationChangeEventHandler> configChangeEventHandlers = new ArrayList<ILocalClusterConfigurationChangeEventHandler>();
    
    public static LocalClusterManager getInstance() {
        synchronized (LocalClusterManager.class) {
            if(instance == null) {
                instance = new LocalClusterManager();
            }
            return instance;
        }
    }
    
    LocalClusterManager() {
        try {
            DistributedCacheService distributedCacheService = DistributedCacheService.getInstance();
            this.nodes = new JsonReplicatedMap<String, ClusterNode>(distributedCacheService.getReplicatedMap(LOCALCLUSTERMANAGER_MAP_ID), ClusterNode.class);
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            throw new RuntimeException(ex);
        }
    }
    
    public synchronized String getName() {
        return this.name;
    }
    
    public synchronized void setName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.name = name;
    }
    
    public synchronized int getNodeCount() {
        return this.nodes.size();
    }
    
    public synchronized Collection<ClusterNode> getAllNodes() {
        return Collections.unmodifiableCollection(this.nodes.values());
    }
    
    public synchronized ClusterNode getNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodes.get(name);
    }
    
    public synchronized ClusterNode findNodeByAddress(String addr) {
        if(addr == null || addr.isEmpty()) {
            throw new IllegalArgumentException("addr is empty or null");
        }
        Collection<ClusterNode> values = this.nodes.values();
        for(ClusterNode node : values) {
            if(node.hasAddress(addr)) {
                return node;
            }
        }
        return null;
    }
    
    public synchronized boolean hasNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodes.containsKey(name);
    }
    
    public synchronized void removeAllNodes() {
        ArrayList<String> keys = new ArrayList<String>();
        
        Collection<ClusterNode> values = this.nodes.values();
        for(ClusterNode node : values) {
            keys.add(node.getName());
        }
        
        for(String name : keys) {
            removeNode(name);
        }
        
        keys.clear();
    }
    
    public synchronized void addNodes(Collection<ClusterNode> nodes) throws NodeAlreadyAddedException {
        for(ClusterNode node : nodes) {
            addNode(node);
        }
    }
    
    public synchronized void addNode(ClusterNode node) throws NodeAlreadyAddedException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.nodes.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + " is already added");
        }
        
        ClusterNode nodeAdded = this.nodes.put(node.getName(), node);
        if(nodeAdded != null) {
            raiseEventForAddNode(nodeAdded);
        }
    }
    
    public synchronized void addNode(ClusterNode node, boolean bLocal) throws NodeAlreadyAddedException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.nodes.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + " is already added");
        }
        
        if(bLocal) {
            this.localNode = node;
        }
        
        ClusterNode nodeAdded = this.nodes.put(node.getName(), node);
        if(nodeAdded != null) {
            raiseEventForAddNode(nodeAdded);
        }
    }
    
    public synchronized void removeNode(ClusterNode node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        removeNode(node.getName());
    }
    
    public synchronized void removeNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(this.localNode != null) {
            if(this.localNode.getName().equals(name)) {
                this.localNode = null;
            }
        }
        
        ClusterNode nodeRemoved = this.nodes.remove(name);
        if(nodeRemoved != null) {
            raiseEventForRemoveNode(nodeRemoved);
        }
    }
    
    @Override
    public String toString() {
        return "LocalClusterManager : " + this.name;
    }

    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        return false;
    }
    
    public synchronized void addConfigChangeEventHandler(ILocalClusterConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(ILocalClusterConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<ILocalClusterConfigurationChangeEventHandler> toberemoved = new ArrayList<ILocalClusterConfigurationChangeEventHandler>();
        
        for(ILocalClusterConfigurationChangeEventHandler handler : this.configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(ILocalClusterConfigurationChangeEventHandler handler : toberemoved) {
            this.configChangeEventHandlers.remove(handler);
        }
    }
    
    private synchronized void raiseEventForAddNode(ClusterNode node) {
        LOG.debug("Cluster node added : " + node.toString());
        
        for(ILocalClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addNode(this, node);
        }
    }

    private synchronized void raiseEventForRemoveNode(ClusterNode node) {
        LOG.debug("Cluster node removed : " + node.toString());
        
        for(ILocalClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeNode(this, node);
        }
    }
    
    public synchronized Cluster getCluster() {
        try {
            Cluster cluster = new Cluster(this.name);
            cluster.addNodes(this.nodes.values());
            return cluster;
        } catch (NodeAlreadyAddedException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    public synchronized ClusterNode getLocalNode() {
        return this.localNode;
    }
}
