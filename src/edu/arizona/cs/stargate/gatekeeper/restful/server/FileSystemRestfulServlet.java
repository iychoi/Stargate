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
import edu.arizona.cs.stargate.gatekeeper.filesystem.FileSystemManager;
import edu.arizona.cs.stargate.gatekeeper.filesystem.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AFileSystemRestfulAPI;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
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
    @Path(AFileSystemRestfulAPI.LIST_FILE_STATUS_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseListStatusText(
            @DefaultValue("null") @QueryParam("mpath") String mpath
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseListStatusJSON(mpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.LIST_FILE_STATUS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<VirtualFileStatus>> responseListStatusJSON(
            @DefaultValue("null") @QueryParam("mpath") String mpath
    ) {
        try {
            if(mpath != null) {
                Collection<VirtualFileStatus> status = listStatus(mpath);
                return new RestfulResponse<Collection<VirtualFileStatus>>(Collections.unmodifiableCollection(status));
            } else {
                return new RestfulResponse<Collection<VirtualFileStatus>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<VirtualFileStatus>>(ex);
        }
    }
    
    @Override
    public Collection<VirtualFileStatus> listStatus(String mappedPath) throws Exception {
        FileSystemManager fsm = getFileSystemManager();
        return fsm.listStatus(mappedPath);
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.FILE_STATUS_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetFileStatusText(
            @DefaultValue("null") @QueryParam("mpath") String mpath
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseGetFileStatusJSON(mpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.FILE_STATUS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<VirtualFileStatus> responseGetFileStatusJSON(
            @DefaultValue("null") @QueryParam("mpath") String mpath
    ) {
        try {
            if(mpath != null) {
                VirtualFileStatus status = getFileStatus(mpath);
                return new RestfulResponse<VirtualFileStatus>(status);
            } else {
                return new RestfulResponse<VirtualFileStatus>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<VirtualFileStatus>(ex);
        }
    }

    @Override
    public VirtualFileStatus getFileStatus(String mappedPath) throws Exception {
        FileSystemManager fsm = getFileSystemManager();
        return fsm.getFileStatus(mappedPath);
    }
    
    @GET
    @Path(AFileSystemRestfulAPI.DATA_CHUNK_PATH)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response responseGetDataChunk(
            @DefaultValue("null") @QueryParam("mpath") String mpath,
            @DefaultValue("0") @QueryParam("offset") long offset,
            @DefaultValue("0") @QueryParam("len") int len
    ) throws Exception {
        final InputStream is = getDataChunk(mpath, offset, len);
        if(is == null) {
            LOG.error("data chunk not found : mpath - " + mpath);
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        StreamingOutput stream = new StreamingOutput() {

            @Override
            public void write(OutputStream out) throws IOException, WebApplicationException {
                try {
                    int buffersize = 100 * 1024;
                    byte[] buffer = new byte[buffersize];

                    int read = 0;
                    while ((read = is.read(buffer)) > 0) {
                        out.write(buffer, 0, read);
                    }
                    is.close();

                } catch (Exception ex) {
                    throw new WebApplicationException(ex);
                }
            }
        };

        return Response.ok(stream).header("content-disposition", "attachment; filename = " + mpath).build();
    }

    @Override
    public InputStream getDataChunk(String mappedPath, long offset, int size) throws Exception {
        FileSystemManager fsm = getFileSystemManager();
        return fsm.getDataChunk(mappedPath, offset, size);
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
    
    private FileSystemManager getFileSystemManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getFileSystemManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
