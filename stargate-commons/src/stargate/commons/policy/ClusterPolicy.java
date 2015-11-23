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
package stargate.commons.policy;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.datastore.ADataStore;

/**
 *
 * @author iychoi
 */
public class ClusterPolicy extends APolicy {

    private static final Log LOG = LogFactory.getLog(ClusterPolicy.class);
    
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
    
    public static ClusterPolicy createInstance(ADataStore datastore) throws IOException {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        ClusterPolicy policy = new ClusterPolicy();
        policy.readFrom(datastore);
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
    
    @JsonProperty("unreachable_node_report_min_interval")
    public long getUnreachableNodeReportMinInterval() {
        return this.unreachableNodeReportMinInterval;
    }
    
    @JsonProperty("unreachable_node_report_min_interval")
    public void setNodeUnreachableReportMinInterval(long sec) {
        if(sec <= 0) {
            this.unreachableNodeReportMinInterval = 1;
        } else {
            this.unreachableNodeReportMinInterval = sec;
        }
    }
    
    @JsonProperty("blacklist_node_max_unreachable_count")
    public int getBlacklistNodeMaxUnreachableCount() {
        return this.blacklistNodeMaxUnreachableCount;
    }
    
    @JsonProperty("blacklist_node_max_unreachable_count")
    public void setBlacklistNodeMaxUnreachableCount(int max) {
        if(max <= 0) {
            this.blacklistNodeMaxUnreachableCount = 1;
        } else {
            this.blacklistNodeMaxUnreachableCount = max;
        }
    }
    
    @JsonProperty("cluster_node_sync_interval")
    public long getClusterNodeSyncInterval() {
        return this.clusterNodeSyncInterval;
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
    @Override
    public void readFrom(ADataStore datastore) {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        this.unreachableNodeReportMinInterval = readLongFrom(datastore, POLICY_KEY_UNREACHABLE_NODE_REPORT_MIN_INTERVAL, DEFAULT_UNREACHABLE_NODE_REPORT_MIN_INTERVAL);
        this.blacklistNodeMaxUnreachableCount = readIntFrom(datastore, POLICY_KEY_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT, DEFAULT_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT);
        this.clusterNodeSyncInterval = readIntFrom(datastore, POLICY_KEY_CLUSTER_NODE_SYNC_INTERVAL, DEFAULT_CLUSTER_NODE_SYNC_INTERVAL);
    }

    @JsonIgnore
    @Override
    public void addTo(ADataStore datastore) throws IOException {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        datastore.put(POLICY_KEY_UNREACHABLE_NODE_REPORT_MIN_INTERVAL, this.unreachableNodeReportMinInterval);
        datastore.put(POLICY_KEY_BLACKLIST_NODE_MAX_UNREACHABLE_COUNT, this.blacklistNodeMaxUnreachableCount);
        datastore.put(POLICY_KEY_CLUSTER_NODE_SYNC_INTERVAL, this.clusterNodeSyncInterval);
    }
}
