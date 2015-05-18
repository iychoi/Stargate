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

package edu.arizona.cs.stargate.gatekeeper.distributed;

import edu.arizona.cs.stargate.common.ImmutableConfiguration;
import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class DistributedServiceConfiguration extends ImmutableConfiguration {
    
    public static final int DEFAULT_SERVICE_PORT = 21010;
    
    private String knownHostAddr;
    private String myHostAddr;
    private int port = DEFAULT_SERVICE_PORT;
    
    public static DistributedServiceConfiguration createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DistributedServiceConfiguration) serializer.fromJsonFile(file, DistributedServiceConfiguration.class);
    }
    
    public static DistributedServiceConfiguration createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DistributedServiceConfiguration) serializer.fromJson(json, DistributedServiceConfiguration.class);
    }
    
    public DistributedServiceConfiguration() {
    }
    
    @JsonProperty("knownHostAddr")
    public void setKnownHostAddr(String addr) {
        super.verifyMutable();
        
        this.knownHostAddr = addr;
    }
    
    @JsonProperty("knownHostAddr")
    public String getKnownHostAddr() {
        return this.knownHostAddr;
    }
    
    @JsonProperty("myHostAddr")
    public void setMyHostAddr(String addr) {
        super.verifyMutable();
        
        this.myHostAddr = addr;
    }
    
    @JsonProperty("myHostAddr")
    public String getMyHostAddr() {
        return this.myHostAddr;
    }
    
    @JsonProperty("port")
    public void setPort(int port) {
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
}
