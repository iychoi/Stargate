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

package stargate.drivers.hazelcast;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.drivers.ADriverConfiguration;

/**
 *
 * @author iychoi
 */
public class HazelcastCoreDriverConfiguration extends ADriverConfiguration {
    
    private static final Log LOG = LogFactory.getLog(HazelcastCoreDriverConfiguration.class);
    
    private static final int DEFAULT_SERVICE_PORT = 21010;
    
    private String serviceName;
    private List<String> knownHostAddr = new ArrayList<String>();
    private String myHostAddr;
    private int port = DEFAULT_SERVICE_PORT;
    
    public static HazelcastCoreDriverConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (HazelcastCoreDriverConfiguration) serializer.fromJsonFile(file, HazelcastCoreDriverConfiguration.class);
    }
    
    public static HazelcastCoreDriverConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (HazelcastCoreDriverConfiguration) serializer.fromJson(json, HazelcastCoreDriverConfiguration.class);
    }
    
    public HazelcastCoreDriverConfiguration() {
    }
    
    @JsonProperty("service_name")
    public void setServiceName(String serviceName) {
        if(serviceName == null || serviceName.isEmpty()) {
            throw new IllegalArgumentException("serviceName is null or empty");
        }
        
        super.verifyMutable();
        
        this.serviceName = serviceName;
    }
    
    @JsonProperty("service_name")
    public String getServiceName() {
        return this.serviceName;
    }
    
    @JsonProperty("known_host")
    public void addKnownHostAddr(Collection<String> addr) {
        //knownhost can be empty
        if(addr == null) {
            throw new IllegalArgumentException("addr is null");
        }
        
        super.verifyMutable();
        
        this.knownHostAddr.addAll(addr);
    }
    
    @JsonIgnore
    public void addKnownHostAddr(String addr) {
        if(addr == null || addr.isEmpty()) {
            throw new IllegalArgumentException("addr is null or empty");
        }
        
        super.verifyMutable();
        
        this.knownHostAddr.add(addr);
    }
    
    @JsonProperty("known_host")
    public Collection<String> getKnownHostAddr() {
        return Collections.unmodifiableCollection(this.knownHostAddr);
    }
    
    @JsonProperty("my_host")
    public void setMyHostAddr(String addr) {
        if(addr == null || addr.isEmpty()) {
            throw new IllegalArgumentException("addr is null or empty");
        }
        
        super.verifyMutable();
        
        this.myHostAddr = addr;
    }
    
    @JsonProperty("my_host")
    public String getMyHostAddr() {
        return this.myHostAddr;
    }
    
    @JsonProperty("port")
    public void setPort(int port) {
        if(port <= 0) {
            throw new IllegalArgumentException("port is invalid number");
        }
        
        super.verifyMutable();
        
        this.port = port;
    }
    
    @JsonProperty("port")
    public int getPort() {
        return this.port;
    }
    
    @JsonIgnore
    public boolean isLeaderHost() {
        if(this.knownHostAddr == null || this.knownHostAddr.isEmpty()) {
            return true;
        }
        return false;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
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
