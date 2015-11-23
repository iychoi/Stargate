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
package stargate.commons.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.policy.ClusterPolicy;
import stargate.commons.utils.DateTimeUtils;

/**
 *
 * @author iychoi
 */
public abstract class ACluster {
    
    private static final Log LOG = LogFactory.getLog(ACluster.class);
    
    protected String name;
    protected long lastUpdateTime;
    
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
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    @JsonIgnore
    public abstract int getNodeCount();
    
    @JsonProperty("node")
    public abstract Collection<Node> getNode() throws IOException;
    
    @JsonIgnore
    public abstract Node getNode(String name) throws IOException;
    
    @JsonProperty("node")
    public abstract void addNode(Collection<Node> node) throws NodeAlreadyAddedException, IOException;
    
    @JsonIgnore
    public abstract void addNode(Node node) throws NodeAlreadyAddedException, IOException;
    
    @JsonProperty("nodestatus")
    public abstract Collection<NodeStatus> getNodeStatus() throws IOException;
    
    @JsonIgnore
    public abstract NodeStatus getNodeStatus(String name) throws IOException;
    
    @JsonProperty("nodestatus")
    public abstract void addNodeStatus(Collection<NodeStatus> status) throws IOException;
    
    @JsonIgnore
    public abstract void addNodeStatus(NodeStatus status) throws IOException;
    
    @JsonIgnore
    public abstract void addNode(Node node, NodeStatus status) throws NodeAlreadyAddedException, IOException;
    
    @JsonIgnore
    public abstract void clearNode();
    
    @JsonIgnore
    public abstract void removeNode(Node node) throws IOException;
    
    @JsonIgnore
    public abstract void removeNode(String name) throws IOException;
    
    @JsonIgnore
    public abstract boolean hasNode(String name);
    
    @JsonIgnore
    public synchronized boolean isEmpty() {
        if(getNodeCount() == 0) {
            return true;
        }
        return false;
    }
    
    @JsonIgnore
    public abstract void reportNodeReachable(String name);
    
    @JsonIgnore
    public abstract void reportNodeUnreachable(ClusterPolicy cp, String name);
    
    @JsonIgnore
    public abstract void addNodeToBlacklist(String name);
    
    @JsonIgnore
    public abstract void removeNodeFromBlacklist(String name);
    
    @JsonProperty("last_update_time")
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    @JsonProperty("last_update_time")
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
    
    @JsonIgnore
    @Override
    public synchronized String toString() {
        return this.name;
    }
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
}
