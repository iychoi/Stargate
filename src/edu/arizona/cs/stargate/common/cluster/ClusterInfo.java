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

/**
 *
 * @author iychoi
 */
public class ClusterInfo {
    
    private static final Log LOG = LogFactory.getLog(ClusterInfo.class);
    
    private String name;
    private Map<String, ClusterNodeInfo> nodes = new HashMap<String, ClusterNodeInfo>();
    
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
        this.nodes.putAll(that.nodes);
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
        
        this.nodes.put(node.getName(), node);
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
        
        this.nodes.remove(name);
    }
    
    @Override
    public String toString() {
        return this.name;
    }

    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        return false;
    }
}
