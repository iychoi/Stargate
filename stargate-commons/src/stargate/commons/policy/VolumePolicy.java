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
public class VolumePolicy extends APolicy {

    private static final Log LOG = LogFactory.getLog(VolumePolicy.class);
    
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
    
    public VolumePolicy() {
        this.localClusterRecipeSyncPeriod = DEFAULT_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD;
        this.remoteClusterDataObjectMetadataSyncPeriod = DEFAULT_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD;
    }
    
    public VolumePolicy(VolumePolicy that) {
        this.localClusterRecipeSyncPeriod = that.localClusterRecipeSyncPeriod;
        this.remoteClusterDataObjectMetadataSyncPeriod = that.remoteClusterDataObjectMetadataSyncPeriod;
    }
    
    @JsonProperty("local_cluster_recipe_sync_period")
    public long getLocalClusterRecipeSyncPeriod() {
        return this.localClusterRecipeSyncPeriod;
    }
    
    @JsonProperty("local_cluster_recipe_sync_period")
    public void setLocalClusterRecipeSyncPeriod(long sec) {
        if(sec <= 0) {
            this.localClusterRecipeSyncPeriod = 1;
        } else {
            this.localClusterRecipeSyncPeriod = sec;
        }
    }
    
    @JsonProperty("remote_cluster_data_object_metadata_sync_period")
    public long getRemoteClusterDataObjectMetadataSyncPeriod() {
        return this.remoteClusterDataObjectMetadataSyncPeriod;
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
    @Override
    public void readFrom(ADataStore datastore) {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        this.localClusterRecipeSyncPeriod = readIntFrom(datastore, POLICY_KEY_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD, DEFAULT_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD);
        this.remoteClusterDataObjectMetadataSyncPeriod = readLongFrom(datastore, POLICY_KEY_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD, DEFAULT_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD);
    }

    @JsonIgnore
    @Override
    public void addTo(ADataStore datastore) throws IOException {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        datastore.put(POLICY_KEY_LOCAL_CLUSTER_RECIPE_SYNC_PERIOD, this.localClusterRecipeSyncPeriod);
        datastore.put(POLICY_KEY_REMOTE_CLUSTER_DATA_OBJECT_METADATA_SYNC_PERIOD, this.remoteClusterDataObjectMetadataSyncPeriod);
    }
}
