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
package edu.arizona.cs.stargate.cluster;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class NodeStatus {
    private static final Log LOG = LogFactory.getLog(NodeStatus.class);
    
    private static final String HADOOP_CONFIG_KEY = NodeStatus.class.getCanonicalName();
    
    private String nodeName;
    private boolean unreachable;
    private long lastUnreachableUpdateTime;
    private int unreachableCount;
    private boolean blacklisted;
    private long blacklistedTime;
        
    public static NodeStatus createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (NodeStatus) serializer.fromJsonFile(file, NodeStatus.class);
    }
    
    public static NodeStatus createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (NodeStatus) serializer.fromJson(json, NodeStatus.class);
    }
    
    public static NodeStatus createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (NodeStatus) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, NodeStatus.class);
    }
    
    public static NodeStatus createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (NodeStatus) serializer.fromJsonFile(fs, file, NodeStatus.class);
    }
    
    public NodeStatus() {
        this.nodeName = null;
        this.unreachable = false;
        this.lastUnreachableUpdateTime = 0;
        this.unreachableCount = 0;
        this.blacklisted = false;
        this.blacklistedTime = 0;
    }
    
    public NodeStatus(Node node) {
        this.nodeName = node.getName();
        this.unreachable = false;
        this.lastUnreachableUpdateTime = 0;
        this.unreachableCount = 0;
        this.blacklisted = false;
        this.blacklistedTime = 0;
    }
    
    public NodeStatus(String nodeName) {
        this.nodeName = nodeName;
        this.unreachable = false;
        this.lastUnreachableUpdateTime = 0;
        this.unreachableCount = 0;
        this.blacklisted = false;
        this.blacklistedTime = 0;
    }
    
    public NodeStatus(NodeStatus that) {
        this.unreachable = that.unreachable;
        this.lastUnreachableUpdateTime = that.lastUnreachableUpdateTime;
        this.unreachableCount = that.unreachableCount;
        this.blacklisted = that.blacklisted;
        this.blacklistedTime = that.blacklistedTime;
    }
    
    @JsonProperty("node_name")
    public synchronized void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }
    
    @JsonProperty("node_name")
    public synchronized String getNodeName() {
        return this.nodeName;
    }
    
    @JsonProperty("unreachable")
    public synchronized boolean isUnreachable() {
        return this.unreachable;
    }
    
    @JsonProperty("unreachable")
    public synchronized void setUnreachable(boolean unreachable) {
        this.unreachable = unreachable;
    }
    
    @JsonProperty("last_unreachable_update_time")
    public synchronized long getLastUnreachableUpdateTime() {
        return this.lastUnreachableUpdateTime;
    }
    
    @JsonProperty("last_unreachable_update_time")
    public synchronized void setLastUnreachableUpdateTime(long time) {
        this.lastUnreachableUpdateTime = time;
    }
    
    @JsonProperty("unreachable_count")
    public synchronized int getUnreachableCount() {
        return this.unreachableCount;
    }
    
    @JsonProperty("unreachable_count")
    public synchronized void setUnreachableCount(int count) {
        this.unreachableCount = count;
    }
    
    @JsonIgnore
    public synchronized void increaseUnreachableCount() {
        this.unreachableCount++;
    }
    
    @JsonProperty("blacklisted")
    public synchronized boolean isBlacklisted() {
        return this.blacklisted;
    }
    
    @JsonProperty("blacklisted")
    public synchronized void setBlacklisted(boolean blacklisted) {
        this.blacklisted = blacklisted;
    }
    
    @JsonProperty("blacklisted_time")
    public synchronized long getBlacklistedTime() {
        return this.blacklistedTime;
    }
    
    @JsonProperty("blacklisted_time")
    public synchronized void setBlacklistedTime(long time) {
        this.blacklistedTime = time;
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.nodeName == null || this.nodeName.isEmpty()) {
            return true;
        }
        
        return false;
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
    
    @JsonIgnore
    public synchronized void saveTo(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
