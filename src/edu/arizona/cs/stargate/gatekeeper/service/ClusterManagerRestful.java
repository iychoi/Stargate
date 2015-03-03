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

package edu.arizona.cs.stargate.gatekeeper.service;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.gatekeeper.AClusterManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
@Path(AClusterManagerAPI.PATH)
@Singleton
public class ClusterManagerRestful extends AClusterManagerAPI {
    
    private static final Log LOG = LogFactory.getLog(ClusterManagerRestful.class);
    
    @GET
    @Path(AClusterManagerAPI.GET_LOCAL_CLUSTER_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetLocalClusterInfoText() {
        try {
            return DataFormatter.toJSONFormat(responseGetLocalClusterInfoJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AClusterManagerAPI.GET_LOCAL_CLUSTER_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<ClusterInfo> responseGetLocalClusterInfoJSON() {
        try {
            return new RestfulResponse<ClusterInfo>(getLocalClusterInfo());
        } catch(Exception ex) {
            return new RestfulResponse<ClusterInfo>(ex);
        }
    }

    @Override
    public ClusterInfo getLocalClusterInfo() throws Exception {
        ClusterManager cm = getClusterManager();
        return cm.getLocalClusterInfo();
    }

    @GET
    @Path(AClusterManagerAPI.GET_REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRemoteClusterInfoText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatter.toJSONFormat(responseGetRemoteClusterInfoJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AClusterManagerAPI.GET_REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<ClusterInfo>> responseGetRemoteClusterInfoJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            if(name != null) {
                if(name.equals("*")) {
                    return new RestfulResponse<Collection<ClusterInfo>>(getAllRemoteClusterInfo());
                } else {
                    ClusterInfo info = getRemoteClusterInfo(name);
                    List<ClusterInfo> clusterInfo = new ArrayList<ClusterInfo>();
                    clusterInfo.add(info);
                    
                    return new RestfulResponse<Collection<ClusterInfo>>(Collections.unmodifiableCollection(clusterInfo));
                }
            } else {
                return new RestfulResponse<Collection<ClusterInfo>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<ClusterInfo>>(ex);
        }
    }
    
    @Override
    public ClusterInfo getRemoteClusterInfo(String name) throws Exception {
        ClusterManager cm = getClusterManager();
        return cm.getRemoteClusterInfo(name);
    }
    
    @Override
    public Collection<ClusterInfo> getAllRemoteClusterInfo() throws Exception {
        ClusterManager cm = getClusterManager();
        return cm.getAllRemoteClusterInfo();
    }
    
    @POST
    @Path(AClusterManagerAPI.ADD_REMOTE_CLUSTER_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseAddRemoteClusterText(ClusterInfo clusterInfo) {
        try {
            return DataFormatter.toJSONFormat(responseAddRemoteClusterJSON(clusterInfo));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @POST
    @Path(AClusterManagerAPI.ADD_REMOTE_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseAddRemoteClusterJSON(ClusterInfo clusterInfo) {
        try {
            if(clusterInfo != null) {
                addRemoteCluster(clusterInfo);
                return new RestfulResponse<Boolean>(true);
            } else {
                return new RestfulResponse<Boolean>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Boolean>(ex);
        }
    }
    
    @Override
    public void addRemoteCluster(ClusterInfo cluster) throws ClusterAlreadyAddedException {
        ClusterManager cm = getClusterManager();
        cm.addRemoteCluster(cluster);
    }
    
    @DELETE
    @Path(AClusterManagerAPI.DELETE_REMOTE_CLUSTER_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteRemoteClusterText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatter.toJSONFormat(responseDeleteRemoteClusterJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(AClusterManagerAPI.DELETE_REMOTE_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseDeleteRemoteClusterJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            if(name != null) {
                if(name.equals("*")) {
                    removeAllRemoteCluster();
                } else {
                    removeRemoteCluster(name);
                }
                return new RestfulResponse<Boolean>(true);
            } else {
                return new RestfulResponse<Boolean>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Boolean>(ex);
        }
    }
    
    @Override
    public void removeAllRemoteCluster() {
        ClusterManager cm = getClusterManager();
        cm.removeAllRemoteCluster();
    }

    @Override
    public void removeRemoteCluster(String name) {
        ClusterManager cm = getClusterManager();
        cm.removeRemoteCluster(name);
    }
    
    private ClusterManager getClusterManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getClusterManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
