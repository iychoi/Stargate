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

package edu.arizona.cs.stargate.gatekeeper.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class GateKeeperClient {
    
    /*
    TODO: Add thread pool and future things to use async function
    
    ref : http://www.vogella.com/tutorials/JavaConcurrency/article.html
    https://jersey.java.net/nonav/apidocs/1.9/jersey/com/sun/jersey/api/client/AsyncWebResource.html#get(java.lang.Class)
    */
    
    private static final Log LOG = LogFactory.getLog(GateKeeperClient.class);
    
    private static GateKeeperClient instance;
    
    private GateKeeperClientConfiguration config;
    private ClientConfig clientConfig;
    private Client client;
    
    public static GateKeeperClient getInstance(GateKeeperClientConfiguration conf) {
        synchronized (GateKeeperClient.class) {
            if(instance == null) {
                instance = new GateKeeperClient(conf);
            }
            return instance;
        }
    }
    
    GateKeeperClient(GateKeeperClientConfiguration conf) {
        this.config = conf;
        
        this.clientConfig = new DefaultClientConfig();
        this.clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        
        this.client = Client.create(this.clientConfig);
    }
    
    public Object post(String path, Object request, Class reply) throws IOException {
        URI absURI = this.config.getServiceURI().resolve(path);
        
        WebResource webResource = this.client.resource(absURI);
        ClientResponse response = webResource.accept("application/json").type("application/json").post(ClientResponse.class, request);
        
        if(response.getStatus() != 200) {
            throw new IOException("HTTP error code : " + response.getStatus());
        }
        
        return response.getEntity(reply);
    }
    
    public Object get(String path, Class reply) throws IOException {
        URI absURI = this.config.getServiceURI().resolve(path);
        
        WebResource webResource = this.client.resource(absURI);
        ClientResponse response = webResource.accept("application/json").type("application/json").get(ClientResponse.class);
        
        if(response.getStatus() != 200) {
            throw new IOException("HTTP error code : " + response.getStatus());
        }
        
        return response.getEntity(reply);
    }
    
    public ClusterManagerClient getClusterManagerClient() {
        return new ClusterManagerClient(this);
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperClient";
    }
}
