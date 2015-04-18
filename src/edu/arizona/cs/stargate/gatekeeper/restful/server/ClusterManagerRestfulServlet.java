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

package edu.arizona.cs.stargate.gatekeeper.restful.server;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.common.ClusterAlreadyAddedException;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperService;
import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.gatekeeper.cluster.RemoteClusterManager;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AClusterManagerRestfulAPI;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
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
@Path(AClusterManagerRestfulAPI.BASE_PATH)
@Singleton
public class ClusterManagerRestfulServlet extends AClusterManagerRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(ClusterManagerRestfulServlet.class);
    
    @GET
    @Path(AClusterManagerRestfulAPI.LOCAL_CLUSTER_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetLocalClusterInfoText() {
        LOG.info("request local cluster info");
        try {
            return DataFormatUtils.toJSONFormat(responseGetLocalClusterInfoJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AClusterManagerRestfulAPI.LOCAL_CLUSTER_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Cluster> responseGetLocalClusterInfoJSON() {
        LOG.info("request local cluster info");
        try {
            return new RestfulResponse<Cluster>(getLocalClusterInfo());
        } catch(Exception ex) {
            return new RestfulResponse<Cluster>(ex);
        }
    }

    @Override
    public Cluster getLocalClusterInfo() throws Exception {
        LocalClusterManager lcm = getLocalClusterManager();
        return lcm.getCluster();
    }

    @GET
    @Path(AClusterManagerRestfulAPI.REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRemoteClusterInfoText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        LOG.info("request remote cluster info : name = " + name);
        try {
            return DataFormatUtils.toJSONFormat(responseGetRemoteClusterInfoJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AClusterManagerRestfulAPI.REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<Cluster>> responseGetRemoteClusterInfoJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        LOG.info("request remote cluster info : name = " + name);
        try {
            if(name != null) {
                if(name.equals("*")) {
                    return new RestfulResponse<Collection<Cluster>>(getAllRemoteClusterInfo());
                } else {
                    Cluster info = getRemoteClusterInfo(name);
                    List<Cluster> clusterInfo = new ArrayList<Cluster>();
                    clusterInfo.add(info);
                    
                    return new RestfulResponse<Collection<Cluster>>(Collections.unmodifiableCollection(clusterInfo));
                }
            } else {
                return new RestfulResponse<Collection<Cluster>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<Cluster>>(ex);
        }
    }
    
    @Override
    public Cluster getRemoteClusterInfo(String name) throws Exception {
        RemoteClusterManager rcm = getRemoteClusterManager();
        return rcm.getCluster(name);
    }
    
    @Override
    public Collection<Cluster> getAllRemoteClusterInfo() throws Exception {
        RemoteClusterManager rcm = getRemoteClusterManager();
        return rcm.getAllClusters();
    }
    
    @POST
    @Path(AClusterManagerRestfulAPI.REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseAddRemoteClusterText(Cluster clusterInfo) {
        try {
            return DataFormatUtils.toJSONFormat(responseAddRemoteClusterJSON(clusterInfo));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @POST
    @Path(AClusterManagerRestfulAPI.REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseAddRemoteClusterJSON(Cluster clusterInfo) {
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
    public void addRemoteCluster(Cluster cluster) throws ClusterAlreadyAddedException {
        RemoteClusterManager rcm = getRemoteClusterManager();
        rcm.addCluster(cluster);
    }
    
    @DELETE
    @Path(AClusterManagerRestfulAPI.REMOTE_CLUSTER_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteRemoteClusterText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseDeleteRemoteClusterJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(AClusterManagerRestfulAPI.REMOTE_CLUSTER_INFO_PATH)
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
        RemoteClusterManager rcm = getRemoteClusterManager();
        rcm.removeAllClusters();
    }

    @Override
    public void removeRemoteCluster(String name) {
        RemoteClusterManager rcm = getRemoteClusterManager();
        rcm.removeCluster(name);
    }
    
    private RemoteClusterManager getRemoteClusterManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getRemoteClusterManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }

    private LocalClusterManager getLocalClusterManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getLocalClusterManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
