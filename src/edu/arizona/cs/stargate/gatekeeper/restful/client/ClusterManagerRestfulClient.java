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

package edu.arizona.cs.stargate.gatekeeper.restful.client;

import edu.arizona.cs.stargate.common.WebParamBuilder;
import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AClusterManagerRestfulAPI;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ClusterManagerRestfulClient extends AClusterManagerRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(ClusterManagerRestfulClient.class);
    
    private GateKeeperClient gatekeeperClient;
    private GateKeeperRestfulClient gatekeeperRPCClient;

    public ClusterManagerRestfulClient(GateKeeperClient gatekeeperClient) {
        this.gatekeeperClient = gatekeeperClient;
        this.gatekeeperRPCClient = gatekeeperClient.getRPCClient();
    }
    
    public String getResourcePath(String path) {
        return AClusterManagerRestfulAPI.BASE_PATH + path;
    }
    
    @Override
    public Cluster getLocalCluster() throws Exception {
        RestfulResponse<Cluster> response;
        try {
            String url = getResourcePath(AClusterManagerRestfulAPI.LOCAL_CLUSTER_PATH);
            response = (RestfulResponse<Cluster>) this.gatekeeperRPCClient.get(url, new GenericType<RestfulResponse<Cluster>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            return response.getResponse();
        }
    }

    @Override
    public Collection<Cluster> getAllRemoteClusters() throws Exception {
        RestfulResponse<Collection<Cluster>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AClusterManagerRestfulAPI.REMOTE_CLUSTER_PATH));
            builder.addParam("name", "*");
            String url = builder.build();
            response = (RestfulResponse<Collection<Cluster>>) this.gatekeeperRPCClient.get(url, new GenericType<RestfulResponse<Collection<Cluster>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            return response.getResponse();
        }
    }
    
    @Override
    public Cluster getRemoteClusters(String name) throws Exception {
        RestfulResponse<Collection<Cluster>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AClusterManagerRestfulAPI.REMOTE_CLUSTER_PATH));
            builder.addParam("name", name);
            String url = builder.build();
            response = (RestfulResponse<Collection<Cluster>>) this.gatekeeperRPCClient.get(url, new GenericType<RestfulResponse<Collection<Cluster>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            Collection<Cluster> clusters = response.getResponse();
            Iterator<Cluster> iterator = clusters.iterator();
            if(iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }
    }

    @Override
    public void addRemoteCluster(Cluster cluster) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            String url = getResourcePath(AClusterManagerRestfulAPI.REMOTE_CLUSTER_PATH);
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.post(url, cluster, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }

    @Override
    public void removeRemoteCluster(String name) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AClusterManagerRestfulAPI.REMOTE_CLUSTER_PATH));
            builder.addParam("name", name);
            String url = builder.build();
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
    
    @Override
    public void removeAllRemoteClusters() throws Exception {
        RestfulResponse<Boolean> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AClusterManagerRestfulAPI.REMOTE_CLUSTER_PATH));
            builder.addParam("name", "*");
            String url = builder.build();
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
}
