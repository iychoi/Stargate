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

import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.gatekeeper.AClusterManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ClusterManagerClient extends AClusterManagerAPI {
    
    private static final Log LOG = LogFactory.getLog(ClusterManagerClient.class);
    
    private GateKeeperClient gatekeeperClient;
    private GateKeeperRPCClient gatekeeperRPCClient;

    public ClusterManagerClient(GateKeeperClient gatekeeperClient) {
        this.gatekeeperClient = gatekeeperClient;
        this.gatekeeperRPCClient = gatekeeperClient.getRPCClient();
    }
    
    public String getResourcePath(String path) {
        return AClusterManagerAPI.PATH + path;
    }
    
    public String getResourcePath(String path, Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, String>> entrySet = params.entrySet();
        for(Map.Entry<String, String> entry : entrySet) {
            sb.append(entry.getKey() + "=" + entry.getValue() + "&");
        }
        return AClusterManagerAPI.PATH + path + "?" + sb.toString();
    }
    
    @Override
    public ClusterInfo getLocalClusterInfo() throws Exception {
        RestfulResponse<ClusterInfo> response;
        try {
            response = (RestfulResponse<ClusterInfo>) this.gatekeeperRPCClient.get(getResourcePath(AClusterManagerAPI.GET_LOCAL_CLUSTER_INFO_PATH), new GenericType<RestfulResponse<ClusterInfo>>(){});
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
    public Collection<ClusterInfo> getRemoteClusterInfo() throws Exception {
        RestfulResponse<Collection<ClusterInfo>> response;
        try {
            response = (RestfulResponse<Collection<ClusterInfo>>) this.gatekeeperRPCClient.get(getResourcePath(AClusterManagerAPI.GET_REMOTE_CLUSTER_INFO_PATH), new GenericType<RestfulResponse<Collection<ClusterInfo>>>(){});
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
    public void addRemoteCluster(ClusterInfo cluster) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.post(getResourcePath(AClusterManagerAPI.ADD_REMOTE_CLUSTER_PATH), cluster, new GenericType<RestfulResponse<Boolean>>(){});
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
            Map<String, String> params = new HashMap<String, String>();
            params.put("name", name);
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(getResourcePath(AClusterManagerAPI.DELETE_REMOTE_CLUSTER_PATH, params), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
    
    @Override
    public void removeAllRemoteCluster() throws Exception {
        RestfulResponse<Boolean> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("name", "*");
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(getResourcePath(AClusterManagerAPI.DELETE_REMOTE_CLUSTER_PATH, params), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
}
