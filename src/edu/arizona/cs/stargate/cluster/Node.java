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
import edu.arizona.cs.stargate.transport.TransportServiceInfo;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
public class Node {
    
    private static final Log LOG = LogFactory.getLog(Node.class);
    
    private static final String HADOOP_CONFIG_KEY = Node.class.getCanonicalName();
    
    private String name;
    private TransportServiceInfo transportServiceInfo;
    private List<String> hostname = new ArrayList<String>();
    
    public static Node createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (Node) serializer.fromJsonFile(file, Node.class);
    }
    
    public static Node createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (Node) serializer.fromJson(json, Node.class);
    }
    
    public static Node createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (Node) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, Node.class);
    }
    
    public static Node createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (Node) serializer.fromJsonFile(fs, file, Node.class);
    }
    
    public Node() {
        this.name = null;
        this.transportServiceInfo = null;
    }
    
    public Node(Node that) {
        this.name = that.name;
        this.transportServiceInfo = that.transportServiceInfo;
        this.hostname.addAll(that.hostname);
    }
    
    public Node(String name, TransportServiceInfo transportServiceInfo) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(transportServiceInfo == null) {
            throw new IllegalArgumentException("transportServiceInfo is null");
        }
        
        initialize(name, transportServiceInfo, null);
    }
    
    public Node(String name, TransportServiceInfo transportServiceInfo, Collection<String> hostAddr) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(transportServiceInfo == null) {
            throw new IllegalArgumentException("transportServiceInfo is null");
        }
        
        initialize(name, transportServiceInfo, hostAddr);
    }
    
    private void initialize(String name, TransportServiceInfo transportServiceInfo, Collection<String> hostAddr) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        if(transportServiceInfo == null) {
            throw new IllegalArgumentException("transportServiceInfo is null");
        }
        
        this.name = name;
        this.transportServiceInfo = transportServiceInfo;
        if(hostAddr != null) {
            addHostName(hostAddr);
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

    @JsonProperty("transport_service_info")
    public synchronized TransportServiceInfo getTransportServiceInfo() {
        return transportServiceInfo;
    }

    @JsonProperty("transport_service_info")
    public synchronized void setTransportServiceInfo(TransportServiceInfo transportServiceInfo) {
        if(transportServiceInfo == null) {
            throw new IllegalArgumentException("transportServiceInfo is null");
        }
        
        this.transportServiceInfo = transportServiceInfo;
    }
    
    @JsonProperty("hostname")
    public synchronized Collection<String> getHostName() {
        return Collections.unmodifiableList(this.hostname);
    }
    
    @JsonProperty("hostname")
    public synchronized void addHostName(Collection<String> hostname) {
        if(hostname == null) {
            throw new IllegalArgumentException("hostname is null");
        }
        
        for(String name : hostname) {
            addHostName(name);
        }
    }
    
    @JsonIgnore
    public synchronized void addHostName(String hostname) {
        if(hostname == null || hostname.isEmpty()) {
            throw new IllegalArgumentException("hostname is empty or null");
        }
        
        if(!this.hostname.contains(hostname)) {
            this.hostname.add(hostname);
        }
    }
    
    @JsonIgnore
    public synchronized boolean hasHostName(String hostname) {
        if(this.hostname != null) {
            for(String name : this.hostname) {
                if(name.equalsIgnoreCase(hostname)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    @JsonIgnore
    public synchronized boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        
        if(this.transportServiceInfo == null) {
            return true;
        }
        return false;
    }
    
    @Override
    @JsonIgnore
    public synchronized String toString() {
        return this.name + "(" + this.transportServiceInfo.toString() + ")";
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