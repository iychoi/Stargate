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
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperService;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExportManager;
import edu.arizona.cs.stargate.gatekeeper.recipe.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeChunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipeManager;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AFileSystemRestfulAPI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
@Path(AFileSystemRestfulAPI.BASE_PATH)
@Singleton
public class FileSystemRestfulServlet extends AFileSystemRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(FileSystemRestfulServlet.class);
    
    @GET
    @Path(AFileSystemRestfulAPI.LOCAL_CLUSTER_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetLocalClusterText() {
        try {
            return DataFormatUtils.toJSONFormat(responseGetLocalClusterJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.LOCAL_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Cluster> responseGetLocalClusterJSON() {
        try {
            return new RestfulResponse<Cluster>(getLocalCluster());
        } catch(Exception ex) {
            return new RestfulResponse<Cluster>(ex);
        }
    }

    @Override
    public Cluster getLocalCluster() throws Exception {
        LocalClusterManager lcm = getLocalClusterManager();
        return lcm.getCluster();
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.VIRTUAL_FILE_STATUS_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetVirtualFileStatusText() {
        try {
            return DataFormatUtils.toJSONFormat(responseGetVirtualFileStatusJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.VIRTUAL_FILE_STATUS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<VirtualFileStatus>> responseGetVirtualFileStatusJSON() {
        try {
            return new RestfulResponse<Collection<VirtualFileStatus>>(getAllVirtualFileStatus());
        } catch(Exception ex) {
            return new RestfulResponse<Collection<VirtualFileStatus>>(ex);
        }
    }
    
    @Override
    public Collection<VirtualFileStatus> getAllVirtualFileStatus() throws Exception {
        LocalClusterManager lcm = getLocalClusterManager();
        DataExportManager dem = getDataExportManager();
        LocalRecipeManager lrm = getLocalRecipeManager();
        
        ArrayList<VirtualFileStatus> status = new ArrayList<VirtualFileStatus>();
        
        Collection<DataExport> local_exports = dem.getAllDataExports();
        for(DataExport export : local_exports) {
            LocalRecipe recipe = lrm.getRecipe(export.getResourcePath());
            if(recipe != null) {
                VirtualFileStatus t_status = new VirtualFileStatus(lcm.getName(), export.getVirtualPath(), false, recipe.getSize(), recipe.getChunkSize(), recipe.getModificationTime());
            }
        }
        return status;
    }

    @GET
    @Path(AFileSystemRestfulAPI.FS_BLOCK_SIZE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetBlockSizeText() {
        try {
            return DataFormatUtils.toJSONFormat(responseGetBlockSizeJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.FS_BLOCK_SIZE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Long> responseGetBlockSizeJSON() {
        try {
            return new RestfulResponse<Long>(getBlockSize());
        } catch(Exception ex) {
            return new RestfulResponse<Long>(ex);
        }
    }
    
    @Override
    public long getBlockSize() throws Exception {
        LocalRecipeManager lrm = getLocalRecipeManager();
        return lrm.getConfiguration().getChunkSize();
    }

    @Override
    public byte[] readChunkData(String clusterName, String virtualPath, long offset, long size) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    
    private LocalRecipeManager getLocalRecipeManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getLocalRecipeManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }

    private DataExportManager getDataExportManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getDataExportManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
