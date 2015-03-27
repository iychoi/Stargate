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

package edu.arizona.cs.stargate.gatekeeper.service;

import edu.arizona.cs.stargate.cache.service.JsonMap;
import edu.arizona.cs.stargate.common.cluster.ClusterNodeInfo;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.common.cluster.NodeAlreadyAddedException;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.service.StargateService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
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
    private Map<String, ClusterNodeInfo> nodes;
    private ArrayList<IClusterConfigurationChangeEventHandler> configChangeEventHandlers = new ArrayList<IClusterConfigurationChangeEventHandler>();
    
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
            this.nodes = new JsonMap<String, ClusterNodeInfo>(StargateService.getInstance().getDistributedCacheService().getReplicatedMap(LOCALCLUSTERMANAGER_MAP_ID), ClusterNodeInfo.class);
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
    
    public synchronized Collection<ClusterNodeInfo> getAllNode() {
        return Collections.unmodifiableCollection(this.nodes.values());
    }
    
    public synchronized ClusterNodeInfo getNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodes.get(name);
    }
    
    public synchronized boolean hasNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodes.containsKey(name);
    }
    
    public synchronized void removeAllNode() {
        ArrayList<String> keys = new ArrayList<String>();
        
        Collection<ClusterNodeInfo> values = this.nodes.values();
        for(ClusterNodeInfo node : values) {
            keys.add(node.getName());
        }
        
        for(String name : keys) {
            removeNode(name);
        }
        
        keys.clear();
    }
    
    public synchronized void addNode(Collection<ClusterNodeInfo> nodes) throws NodeAlreadyAddedException {
        for(ClusterNodeInfo node : nodes) {
            addNode(node);
        }
    }
    
    public synchronized void addNode(ClusterNodeInfo node) throws NodeAlreadyAddedException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.nodes.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + "is already added");
        }
        
        ClusterNodeInfo nodeAdded = this.nodes.put(node.getName(), node);
        if(nodeAdded != null) {
            raiseEventForAddNode(nodeAdded);
        }
    }
    
    public synchronized void removeNode(ClusterNodeInfo node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        removeNode(node.getName());
    }
    
    public synchronized void removeNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        ClusterNodeInfo nodeRemoved = this.nodes.remove(name);
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
    
    public synchronized void addConfigChangeEventHandler(IClusterConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(IClusterConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<IClusterConfigurationChangeEventHandler> toberemoved = new ArrayList<IClusterConfigurationChangeEventHandler>();
        
        for(IClusterConfigurationChangeEventHandler handler : this.configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IClusterConfigurationChangeEventHandler handler : toberemoved) {
            this.configChangeEventHandlers.remove(handler);
        }
    }
    
    private synchronized void raiseEventForAddNode(ClusterNodeInfo node) {
        LOG.debug("Cluster node added : " + node.toString());
        
        for(IClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addNode(this, node);
        }
    }

    private synchronized void raiseEventForRemoveNode(ClusterNodeInfo node) {
        LOG.debug("Cluster node removed : " + node.toString());
        
        for(IClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeNode(this, node);
        }
    }
    
    public synchronized ClusterInfo getClusterInfo() {
        try {
            ClusterInfo clusterInfo = new ClusterInfo(this.name);
            clusterInfo.addNode(this.nodes.values());
            return clusterInfo;
        } catch (NodeAlreadyAddedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
