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

package edu.arizona.cs.stargate.gatekeeper.cluster;

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
public class ClusterNode {
    
    private String name;
    private URI serviceURL;
    private String[] hostAddrs;
    private boolean unreachable;
    
    ClusterNode() {
        this.name = null;
        this.serviceURL = null;
        this.hostAddrs = null;
        this.unreachable = false;
    }
    
    public static ClusterNode createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterNode) serializer.fromJsonFile(file, ClusterNode.class);
    }
    
    public static ClusterNode createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterNode) serializer.fromJson(json, ClusterNode.class);
    }
    
    public ClusterNode(ClusterNode that) {
        this.name = that.name;
        this.serviceURL = that.serviceURL;
        this.hostAddrs = that.hostAddrs;
        this.unreachable = that.unreachable;
    }
    
    public ClusterNode(String name, URI serviceURL) {
        initialize(name, serviceURL, null, false);
    }
    
    public ClusterNode(String name, URI publicAddr, String[] hostAddrs) {
        initialize(name, publicAddr, hostAddrs, false);
    }
    
    public ClusterNode(String name, String publicAddr) throws URISyntaxException {
        initialize(name, new URI(publicAddr), null, false);
    }
    
    public ClusterNode(String name, String publicAddr, String[] hostAddrs) throws URISyntaxException {
        initialize(name, new URI(publicAddr), hostAddrs, false);
    }
    
    public ClusterNode(String name, String publicAddr, String[] hostAddrs, boolean unreachable) throws URISyntaxException {
        initialize(name, new URI(publicAddr), hostAddrs, unreachable);
    }
    
    private void initialize(String name, URI serviceURL, String[] hostAddrs, boolean unreachable) {
        this.name = name;
        this.serviceURL = serviceURL;
        this.hostAddrs = hostAddrs;
        this.unreachable = unreachable;
    }

    @JsonProperty("name")
    public String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("service_url")
    public URI getServiceURL() {
        return serviceURL;
    }

    @JsonProperty("service_url")
    public void setServiceURL(URI serviceURL) {
        this.serviceURL = serviceURL;
    }
    
    @JsonIgnore
    public void setServiceURL(String serviceURL) throws URISyntaxException {
        this.serviceURL = new URI(serviceURL);
    }
    
    @JsonProperty("host_addrs")
    public String[] getHostAddrs() {
        return this.hostAddrs;
    }
    
    @JsonProperty("host_addrs")
    public void setHostAddrs(String[] hostAddrs) {
        this.hostAddrs = hostAddrs;
    }
    
    @JsonIgnore
    public boolean hasAddress(String addr) {
        if(this.hostAddrs != null) {
            for(String hostAddr : this.hostAddrs) {
                if(hostAddr.equalsIgnoreCase(addr)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    @JsonProperty("unreachable")
    public boolean isUnreachable() {
        return this.unreachable;
    }
    
    @JsonProperty("unreachable")
    public void setUnrechable(boolean unreachable) {
        this.unreachable = unreachable;
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        
        if(this.serviceURL == null || this.serviceURL.getHost().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        return this.name + "(" + this.serviceURL.toString() + ")";
    }
}