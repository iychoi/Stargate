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
public class VolumePolicy {

    private static final Log LOG = LogFactory.getLog(VolumePolicy.class);
    
    private static final String HADOOP_CONFIG_KEY = VolumePolicy.class.getCanonicalName();
    
    // local recipe synchronization
    private static final int DEFAULT_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD = 60;
    private static final String POLICY_KEY_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD = "volume.local.recipe.sync.period";
    private long localClusterRecipeSyncPeriod = DEFAULT_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD;
    
    // remote cluster object metadata synchronization
    private static final int DEFAULT_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD = 60 * 60;
    private static final String POLICY_KEY_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD = "volume.remote.dataobject.metadata.sync.period";
    private long remoteClusterDataObjectMetadataSyncPeriod = DEFAULT_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD;
    
    public static VolumePolicy createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (VolumePolicy) serializer.fromJsonFile(file, VolumePolicy.class);
    }
    
    public static VolumePolicy createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (VolumePolicy) serializer.fromJson(json, VolumePolicy.class);
    }
    
    public static VolumePolicy createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (VolumePolicy) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, VolumePolicy.class);
    }
    
    public static VolumePolicy createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (VolumePolicy) serializer.fromJsonFile(fs, file, VolumePolicy.class);
    }
    
    public static VolumePolicy createInstance(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        VolumePolicy policy = new VolumePolicy();
        policy.setLocalClusterRecipeSyncPeriod(getLocalClusterRecipeSyncPeriod(pm));
        policy.setRemoteClusterDataObjectMetadataSyncPeriod(getRemoteClusterDataObjectMetadataSyncPeriod(pm));
        return policy;
    }
    
    public VolumePolicy() {
        this.localClusterRecipeSyncPeriod = DEFAULT_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD;
    }
    
    public VolumePolicy(VolumePolicy that) {
        this.localClusterRecipeSyncPeriod = that.localClusterRecipeSyncPeriod;
        this.remoteClusterDataObjectMetadataSyncPeriod = that.remoteClusterDataObjectMetadataSyncPeriod;
    }
    
    public static long getLocalClusterRecipeSyncPeriod(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        String p1 = pm.getPolicy(POLICY_KEY_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD);
        if(p1 != null) {
            return Long.parseLong(p1);
        }
        return DEFAULT_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD;
    }
    
    @JsonProperty("local_cluster_recipe_sync_period")
    public long getLocalClusterRecipeSyncPeriod() {
        return this.localClusterRecipeSyncPeriod;
    }
    
    public static void setLocalClusterRecipeSyncPeriod(PolicyManager pm, long sec) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        if(sec <= 0) {
            pm.addPolicy(POLICY_KEY_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD, Long.toString(1));
        } else {
            pm.addPolicy(POLICY_KEY_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD, Long.toString(sec));
        }
    }
    
    @JsonProperty("local_cluster_recipe_sync_period")
    public void setLocalClusterRecipeSyncPeriod(long sec) {
        if(sec <= 0) {
            this.localClusterRecipeSyncPeriod = 1;
        } else {
            this.localClusterRecipeSyncPeriod = sec;
        }
    }
    
    public static long getRemoteClusterDataObjectMetadataSyncPeriod(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        String p1 = pm.getPolicy(POLICY_KEY_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD);
        if(p1 != null) {
            return Long.parseLong(p1);
        }
        return DEFAULT_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD;
    }
    
    @JsonProperty("remote_cluster_data_object_metadata_sync_period")
    public long getRemoteClusterDataObjectMetadataSyncPeriod() {
        return this.remoteClusterDataObjectMetadataSyncPeriod;
    }
    
    public static void setRemoteClusterDataObjectMetadataSyncPeriod(PolicyManager pm, long sec) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        if(sec <= 0) {
            pm.addPolicy(POLICY_KEY_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD, Long.toString(1));
        } else {
            pm.addPolicy(POLICY_KEY_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD, Long.toString(sec));
        }
    }
    
    @JsonProperty("remote_cluster_data_object_metadata_sync_period")
    public void setRemoteClusterDataObjectMetadataSyncPeriod(long sec) {
        if(sec <= 0) {
            this.remoteClusterDataObjectMetadataSyncPeriod = 1;
        } else {
            this.remoteClusterDataObjectMetadataSyncPeriod = sec;
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

        setLocalClusterRecipeSyncPeriod(pm, this.localClusterRecipeSyncPeriod);
        setRemoteClusterDataObjectMetadataSyncPeriod(pm, this.remoteClusterDataObjectMetadataSyncPeriod);
    }
}
