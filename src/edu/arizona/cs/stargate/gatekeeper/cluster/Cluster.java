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
public class Cluster {
    
    private static final Log LOG = LogFactory.getLog(Cluster.class);
    
    private String name;
    private long lastContact;
    private Map<String, ClusterNode> nodes = new HashMap<String, ClusterNode>();
    
    Cluster() {
        this.name = null;
    }
    
    public static Cluster createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Cluster) serializer.fromJsonFile(file, Cluster.class);
    }
    
    public static Cluster createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Cluster) serializer.fromJson(json, Cluster.class);
    }
    
    public Cluster(Cluster that) {
        this.name = that.name;
        this.lastContact = that.lastContact;
        this.nodes.putAll(that.nodes);
    }
    
    public Cluster(String name) {
        this.name = name;
    }
    
    public Cluster(String name, ClusterNode[] node) throws NodeAlreadyAddedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.name = name;
        
        if(node != null) {
            for(ClusterNode nodeinfo : node) {
                Cluster.this.addNode(nodeinfo);
            }
        }
    }
    
    @JsonProperty("name")
    public synchronized String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public synchronized void setName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.name = name;
    }
    
    @JsonProperty("last_contact")
    public synchronized long getLastContact() {
        return this.lastContact;
    }
    
    @JsonProperty("last_contact")
    public synchronized void setLastContact(long lastContact) {
        this.lastContact = lastContact;
    }
    
    @JsonIgnore
    public synchronized int getNodeCount() {
        return this.nodes.size();
    }
    
    @JsonProperty("nodes")
    public synchronized Collection<ClusterNode> getAllNodes() {
        return Collections.unmodifiableCollection(this.nodes.values());
    }
    
    @JsonIgnore
    public synchronized ClusterNode getNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodes.get(name);
    }
    
    @JsonIgnore
    public synchronized boolean hasNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.nodes.containsKey(name);
    }
    
    @JsonIgnore
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
    
    @JsonProperty("nodes")
    public synchronized void addNode(Collection<ClusterNode> nodes) throws NodeAlreadyAddedException {
        for(ClusterNode node : nodes) {
            Cluster.this.addNode(node);
        }
    }
    
    @JsonIgnore
    public synchronized void addNode(ClusterNode node) throws NodeAlreadyAddedException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.nodes.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + "is already added");
        }
        
        this.nodes.put(node.getName(), node);
    }
    
    @JsonIgnore
    public synchronized void removeNode(ClusterNode node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        removeNode(node.getName());
    }
    
    @JsonIgnore
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

    @JsonIgnore
    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        return false;
    }
}
