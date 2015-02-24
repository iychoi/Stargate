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

package edu.arizona.cs.stargate.common.cluster;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ClusterNodeInfo {
    
    private String name;
    private URI addr;
    private boolean gatekeeper;
    
    ClusterNodeInfo() {
        this.name = null;
        this.addr = null;
        this.gatekeeper = false;
    }
    
    public static ClusterNodeInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterNodeInfo) serializer.fromJsonFile(file, ClusterNodeInfo.class);
    }
    
    public static ClusterNodeInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterNodeInfo) serializer.fromJson(json, ClusterNodeInfo.class);
    }
    
    public ClusterNodeInfo(ClusterNodeInfo that) {
        this.name = that.name;
        this.addr = that.addr;
        this.gatekeeper = that.gatekeeper;
    }
    
    public ClusterNodeInfo(String name, URI addr, boolean gatekeeper) {
        initializeClusterNodeInfo(name, addr, gatekeeper);
    }
    
    public ClusterNodeInfo(String name, URI addr) {
        initializeClusterNodeInfo(name, addr, false);
    }
    
    public ClusterNodeInfo(String name, String addr) throws URISyntaxException {
        initializeClusterNodeInfo(name, new URI(addr), false);
    }
    
    public ClusterNodeInfo(String name, String addr, boolean gatekeeper) throws URISyntaxException {
        initializeClusterNodeInfo(name, new URI(addr), gatekeeper);
    }
    
    private void initializeClusterNodeInfo(String name, URI addr, boolean gatekeeper) {
        setName(name);
        setAddr(addr);
        setGatekeeper(gatekeeper);
    }

    public String getName() {
        return this.name;
    }
    
    void setName(String name) {
        this.name = name;
    }

    @JsonProperty("addr")
    public URI getAddr() {
        return addr;
    }

    @JsonProperty("addr")
    void setAddr(URI addr) {
        this.addr = addr;
    }
    
    void setAddr(String addr) throws URISyntaxException {
        this.addr = new URI(addr);
    }
    
    public boolean getGatekeeper() {
        return this.gatekeeper;
    }
    
    void setGatekeeper(boolean gatekeeper) {
        this.gatekeeper = gatekeeper;
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        
        if(this.addr == null || this.addr.getHost().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        String gkwk = this.gatekeeper ? "GK" : "WK";
        return this.name + "(" + gkwk + ", " + this.addr.toString() + ")";
    }
}