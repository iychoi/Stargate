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
package edu.arizona.cs.stargate.policy;

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
public class ClusterPolicy {

    private static final Log LOG = LogFactory.getLog(ClusterPolicy.class);
    
    private static final String HADOOP_CONFIG_KEY = ClusterPolicy.class.getCanonicalName();
    
    // node unreachable
    private static final long DEFAULT_UNREACHABLE_NODE_REPORT_MIN_INTERVAL = 10;
    private static final String POLICY_KEY_UNREACHABLE_NODE_REPORT_MIN_INTERVAL = "cluster.node.unreachable.interval_min";
    private long unreachableNodeReportMinInterval = DEFAULT_UNREACHABLE_NODE_REPORT_MIN_INTERVAL;
    
    // max count to be moved to a blacklist
    private static final int DEFAULT_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT = 5;
    private static final String POLICY_KEY_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT = "cluster.node.unreachable.blacklist";
    private int blacklistNodeMaxUnreachableCount = DEFAULT_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT;
    
    // remote cluster node synchronization
    private static final int DEFAULT_CLUSTER_NODE_SYNC_INTERVAL = 60 * 60;
    private static final String POLICY_KEY_CLUSTER_NODE_SYNC_INTERVAL = "cluster.node.sync.interval";
    private long clusterNodeSyncInterval = DEFAULT_CLUSTER_NODE_SYNC_INTERVAL;
    
    public static ClusterPolicy createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterPolicy) serializer.fromJsonFile(file, ClusterPolicy.class);
    }
    
    public static ClusterPolicy createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterPolicy) serializer.fromJson(json, ClusterPolicy.class);
    }
    
    public static ClusterPolicy createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterPolicy) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, ClusterPolicy.class);
    }
    
    public static ClusterPolicy createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterPolicy) serializer.fromJsonFile(fs, file, ClusterPolicy.class);
    }
    
    public static ClusterPolicy createInstance(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        ClusterPolicy policy = new ClusterPolicy();
        policy.setNodeUnreachableReportMinInterval(getUnreachableNodeReportMinInterval(pm));
        policy.setBlacklistNodeMaxUnreachableCount(getBlacklistNodeMaxUnreachableCount(pm));
        policy.setClusterNodeSyncInterval(getClusterNodeSyncInterval(pm));
        return policy;
    }
    
    public ClusterPolicy() {
        this.unreachableNodeReportMinInterval = DEFAULT_UNREACHABLE_NODE_REPORT_MIN_INTERVAL;
        this.blacklistNodeMaxUnreachableCount = DEFAULT_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT;
        this.clusterNodeSyncInterval = DEFAULT_CLUSTER_NODE_SYNC_INTERVAL;
    }
    
    public ClusterPolicy(ClusterPolicy that) {
        this.unreachableNodeReportMinInterval = that.unreachableNodeReportMinInterval;
        this.blacklistNodeMaxUnreachableCount = that.blacklistNodeMaxUnreachableCount;
        this.clusterNodeSyncInterval = that.clusterNodeSyncInterval;
    }
    
    public static long getUnreachableNodeReportMinInterval(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        String p1 = pm.getPolicy(POLICY_KEY_UNREACHABLE_NODE_REPORT_MIN_INTERVAL);
        if(p1 != null) {
            return Long.parseLong(p1);
        }
        return DEFAULT_UNREACHABLE_NODE_REPORT_MIN_INTERVAL;
    }
    
    @JsonProperty("unreachable_node_report_min_interval")
    public long getUnreachableNodeReportMinInterval() {
        return this.unreachableNodeReportMinInterval;
    }
    
    public static void setNodeUnreachableReportMinInterval(PolicyManager pm, long sec) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        if(sec <= 0) {
            pm.addPolicy(POLICY_KEY_UNREACHABLE_NODE_REPORT_MIN_INTERVAL, Long.toString(1));
        } else {
            pm.addPolicy(POLICY_KEY_UNREACHABLE_NODE_REPORT_MIN_INTERVAL, Long.toString(sec));
        }
    }
    
    @JsonProperty("unreachable_node_report_min_interval")
    public void setNodeUnreachableReportMinInterval(long sec) {
        if(sec <= 0) {
            this.unreachableNodeReportMinInterval = 1;
        } else {
            this.unreachableNodeReportMinInterval = sec;
        }
    }
    
    public static int getBlacklistNodeMaxUnreachableCount(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        String p1 = pm.getPolicy(POLICY_KEY_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT);
        if(p1 != null) {
            return Integer.parseInt(p1);
        }
        return DEFAULT_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT;
    }
    
    @JsonProperty("blacklist_node_max_unreachable_count")
    public int getBlacklistNodeMaxUnreachableCount() {
        return this.blacklistNodeMaxUnreachableCount;
    }
    
    public static void setBlacklistNodeMaxUnreachableCount(PolicyManager pm, int max) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        if(max <= 0) {
            pm.addPolicy(POLICY_KEY_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT, Integer.toString(1));
        } else {
            pm.addPolicy(POLICY_KEY_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT, Integer.toString(max));
        }
    }
    
    @JsonProperty("blacklist_node_max_unreachable_count")
    public void setBlacklistNodeMaxUnreachableCount(int max) {
        if(max <= 0) {
            this.blacklistNodeMaxUnreachableCount = 1;
        } else {
            this.blacklistNodeMaxUnreachableCount = max;
        }
    }
    
    public static long getClusterNodeSyncInterval(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        String p1 = pm.getPolicy(POLICY_KEY_CLUSTER_NODE_SYNC_INTERVAL);
        if(p1 != null) {
            return Long.parseLong(p1);
        }
        return DEFAULT_CLUSTER_NODE_SYNC_INTERVAL;
    }
    
    @JsonProperty("cluster_node_sync_interval")
    public long getClusterNodeSyncInterval() {
        return this.clusterNodeSyncInterval;
    }
    
    public static void setClusterNodeSyncInterval(PolicyManager pm, long sec) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        if(sec <= 0) {
            pm.addPolicy(POLICY_KEY_CLUSTER_NODE_SYNC_INTERVAL, Long.toString(1));
        } else {
            pm.addPolicy(POLICY_KEY_CLUSTER_NODE_SYNC_INTERVAL, Long.toString(sec));
        }
    }
    
    @JsonProperty("cluster_node_sync_interval")
    public void setClusterNodeSyncInterval(long sec) {
        if(sec <= 0) {
            this.clusterNodeSyncInterval = 1;
        } else {
            this.clusterNodeSyncInterval = sec;
        }
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
    
    @JsonIgnore
    public synchronized void saveTo(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }

        setNodeUnreachableReportMinInterval(pm, this.unreachableNodeReportMinInterval);
        setBlacklistNodeMaxUnreachableCount(pm, this.blacklistNodeMaxUnreachableCount);
        setClusterNodeSyncInterval(pm, this.clusterNodeSyncInterval);
    }
}
