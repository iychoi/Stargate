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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Policy {
    private static final String HADOOP_CONFIG_KEY = Policy.class.getCanonicalName();
    
    private ClusterPolicy clusterPolicy;
    private VolumePolicy volumePolicy;
    
    public static PolicyManager createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (PolicyManager) serializer.fromJsonFile(file, PolicyManager.class);
    }
    
    public static PolicyManager createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (PolicyManager) serializer.fromJson(json, PolicyManager.class);
    }
    
    public static PolicyManager createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (PolicyManager) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, PolicyManager.class);
    }
    
    public static PolicyManager createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (PolicyManager) serializer.fromJsonFile(fs, file, PolicyManager.class);
    }
    
    public static Policy createInstance(PolicyManager pm) throws IOException {
        if(pm == null) {
            throw new IllegalArgumentException("pm is null");
        }
        
        Policy policy = new Policy();
        policy.setClusterPolicy(ClusterPolicy.createInstance(pm));
        policy.setVolumePolicy(VolumePolicy.createInstance(pm));
        
        return policy;
    }
    
    public Policy() {
        this.clusterPolicy = new ClusterPolicy();
        this.volumePolicy = new VolumePolicy();
    }
    
    public Policy(Policy that) {
        this.clusterPolicy = that.clusterPolicy;
        this.volumePolicy = that.volumePolicy;
    }
    
    @JsonProperty("cluster")
    public ClusterPolicy getClusterPolicy() {
        return this.clusterPolicy;
    }
    
    @JsonProperty("cluster")
    public void setClusterPolicy(ClusterPolicy policy) {
        if(policy == null) {
            throw new IllegalArgumentException("policy is null");
        }
        
        this.clusterPolicy = policy;
    }
    
    @JsonProperty("volume")
    public VolumePolicy getVolumePolicy() {
        return this.volumePolicy;
    }
    
    @JsonProperty("volume")
    public void setVolumePolicy(VolumePolicy policy) {
        if(policy == null) {
            throw new IllegalArgumentException("policy is null");
        }
        
        this.volumePolicy = policy;
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
        
        if(this.clusterPolicy != null) {
            this.clusterPolicy.saveTo(pm);
        }
        
        if(this.volumePolicy != null) {
            this.volumePolicy.saveTo(pm);
        }
    }
}
