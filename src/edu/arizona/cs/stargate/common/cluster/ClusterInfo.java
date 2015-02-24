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

package edu.arizona.cs.stargate.common.cluster;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ClusterInfo {
    
    private static final Log LOG = LogFactory.getLog(ClusterInfo.class);
    
    private Map<String, ClusterNodeInfo> nodeTable = new HashMap<String, ClusterNodeInfo>();
    
    private ArrayList<IClusterConfigChangeEventHandler> configChangeEventHandlers = new ArrayList<IClusterConfigChangeEventHandler>();
    
    private String name;
    
    ClusterInfo() {
        this.name = null;
    }
    
    public static ClusterInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterInfo) serializer.fromJsonFile(file, ClusterInfo.class);
    }
    
    public static ClusterInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterInfo) serializer.fromJson(json, ClusterInfo.class);
    }
    
    public ClusterInfo(ClusterInfo that) {
        this.name = that.name;
        this.nodeTable.putAll(that.nodeTable);
        this.configChangeEventHandlers.addAll(that.configChangeEventHandlers);
    }
    
    public ClusterInfo(String name) {
        this.name = name;
    }
    
    public ClusterInfo(String name, ClusterNodeInfo[] node) throws NodeAlreadyAddedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.name = name;
        
        if(node != null) {
            for(ClusterNodeInfo nodeinfo : node) {
                addNode(nodeinfo);
            }
        }
    }
    
    public synchronized String getName() {
        return this.name;
    }
    
    synchronized void setName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.name = name;
    }
    
    @JsonIgnore
    public synchronized int getNodeNumber() {
        return this.nodeTable.keySet().size();
    }
    
    @JsonProperty("nodes")
    public synchronized Collection<ClusterNodeInfo> getAllNodeInfo() {
        return Collections.unmodifiableCollection(this.nodeTable.values());
    }
    
    @JsonIgnore
    public synchronized ClusterNodeInfo getNodeInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodeTable.get(name);
    }
    
    @JsonIgnore
    public synchronized boolean hasNodeInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodeTable.containsKey(name);
    }
    
    @JsonIgnore
    public synchronized ClusterNodeInfo getGatekeeperNodeInfo() {
        Collection<ClusterNodeInfo> values = this.nodeTable.values();
        for(ClusterNodeInfo node : values) {
            if(node.getGatekeeper()) {
                return node;
            }
        }
        return null;
    }
    
    public synchronized void removeAllNode() {
        ArrayList<String> keys = new ArrayList<String>();
        
        Collection<ClusterNodeInfo> values = this.nodeTable.values();
        for(ClusterNodeInfo node : values) {
            keys.add(node.getName());
        }
        
        for(String name : keys) {
            removeNode(name);
        }
        
        keys.clear();
    }
    
    @JsonProperty("nodes")
    public synchronized void addNodeInfo(Collection<ClusterNodeInfo> nodes) throws NodeAlreadyAddedException {
        for(ClusterNodeInfo node : nodes) {
            addNode(node);
        }
    }
    
    public synchronized void addNode(ClusterNodeInfo node) throws NodeAlreadyAddedException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.nodeTable.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + "is already added");
        }
        
        ClusterNodeInfo put = this.nodeTable.put(node.getName(), node);
        if(put != null) {
            raiseEventForAddNode(put);
        }
    }
    
    public synchronized void addConfigChangeEventHandler(IClusterConfigChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(IClusterConfigChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<IClusterConfigChangeEventHandler> toberemoved = new ArrayList<IClusterConfigChangeEventHandler>();
        
        for(IClusterConfigChangeEventHandler handler : this.configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IClusterConfigChangeEventHandler handler : toberemoved) {
            this.configChangeEventHandlers.remove(handler);
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
        
        ClusterNodeInfo removed = this.nodeTable.remove(name);
        if(removed != null) {
            raiseEventForRemoveNode(removed);
        }
    }

    private synchronized void raiseEventForAddNode(ClusterNodeInfo node) {
        LOG.debug("node added : " + node.toString());
        
        for(IClusterConfigChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addNode(this, node);
        }
    }
    
    private synchronized void raiseEventForRemoveNode(ClusterNodeInfo node) {
        LOG.debug("node removed : " + node.toString());
        
        for(IClusterConfigChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeNode(this, node);
        }
    }
    
    @Override
    public String toString() {
        return this.name;
    }

    @JsonIgnore
    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        return false;
    }
}
