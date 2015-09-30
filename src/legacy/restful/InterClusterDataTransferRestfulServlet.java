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

package legacy.restful;
/*
import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.utils.HexaUtils;
import edu.arizona.cs.stargate.gatekeeper.recipe.Chunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.ChunkReaderFactory;
import edu.arizona.cs.stargate.gatekeeper.recipe.RemoteRecipe;
import edu.arizona.cs.stargate.transport.http.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.InterClusterDataTransferRestfulAPI;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.service.GateKeeperService;
import edu.arizona.cs.stargate.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipeManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
*/
/**
 *
 * @author iychoi
 */
//@Path(InterClusterDataTransferRestfulAPI.BASE_PATH)
//@Singleton
public class InterClusterDataTransferRestfulServlet {
    /*
    private static final Log LOG = LogFactory.getLog(InterClusterDataTransferRestfulServlet.class);
    
    @GET
    @Path(InterClusterDataTransferRestfulAPI.RECIPE_PATH + "/{vpath:.*}")
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRecipeText(
            @DefaultValue("null") @PathParam("vpath") String vpath
    ) {
        LOG.info("vpath = " + vpath);
        try {
            return HexaUtils.toJSONFormat(responseGetRecipeJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(InterClusterDataTransferRestfulAPI.RECIPE_PATH + "/{vpath:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<RemoteRecipe>> responseGetRecipeJSON(
            @DefaultValue("null") @PathParam("vpath") String vpath
    ) {
        try {
            if(vpath != null) {
                if(vpath.equals("*")) {
                    return new RestfulResponse<Collection<RemoteRecipe>>(getAllRecipes());
                } else {
                    RemoteRecipe recipe = getRecipe(vpath);
                    List<RemoteRecipe> recipes = new ArrayList<RemoteRecipe>();
                    recipes.add(recipe);
                    
                    return new RestfulResponse<Collection<RemoteRecipe>>(Collections.unmodifiableCollection(recipes));
                }
            } else {
                return new RestfulResponse<Collection<RemoteRecipe>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<RemoteRecipe>>(ex);
        }
    }
    
    @Override
    public RemoteRecipe getRecipe(String vpath) throws Exception {
        LocalClusterManager lcm = getLocalClusterManager();
        DataExportManager dem = getDataExportManager();
        LocalRecipeManager lrm = getLocalRecipeManager();
        
        DataExportEntry export = dem.getDataExport(vpath);
        if(export != null) {
            LocalRecipe recipe = lrm.getRecipe(export.getResourcePath());
            return new RemoteRecipe(lcm.getName(), export.getVirtualPath(), recipe);
        } else {
            return null;
        }
    }
    
    @Override
    public Collection<RemoteRecipe> getAllRecipes() throws Exception {
        LocalClusterManager lcm = getLocalClusterManager();
        DataExportManager dem = getDataExportManager();
        LocalRecipeManager lrm = getLocalRecipeManager();
        
        Collection<DataExportEntry> exports = dem.getDataExport();
        ArrayList<RemoteRecipe> remoteRecipes = new ArrayList<RemoteRecipe>();
        if(exports != null) {
            for(DataExportEntry export : exports) {
                LocalRecipe recipe = lrm.getRecipe(export.getResourcePath());
                remoteRecipes.add(new RemoteRecipe(lcm.getName(), export.getVirtualPath(), recipe));
            }
        }
        return remoteRecipes;
    }
    
    @GET
    @Path(InterClusterDataTransferRestfulAPI.DATA_CHUNK_PATH + "/{param:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response responseGetDataChunkURL(
            @DefaultValue("null") @PathParam("param") String param,
            @DefaultValue("0") @QueryParam("offset") long offset,
            @DefaultValue("0") @QueryParam("len") int len
    ) throws Exception {
        String hash = null;
        String vpath = null;
        if(param != null) {
            if(!param.contains(".") && !param.contains("/")) {
                hash = param;
            } else {
                vpath = param;
            }
        }
        if(hash != null) {
            final InputStream is = getDataChunk(hash);
            if(is == null) {
                LOG.error("data chunk not found : hash - " + hash);
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
            
            return Response.ok(stream).header("content-disposition", "attachment; filename = " + vpath).build();
        } else if(vpath != null && len > 0) {
            final InputStream is = getDataChunk(vpath, offset, len);
            if(is == null) {
                LOG.error("data chunk not found : vpath - " + vpath);
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
            
            return Response.ok(stream).header("content-disposition", "attachment; filename = " + vpath).build();
        } else {
            throw new Exception("invalid parameter");
        }
    }

    @Override
    public InputStream getDataChunk(String vpath, long offset, int len) throws Exception {
        DataExportManager dem = getDataExportManager();
        DataExportEntry export = dem.getDataExport(vpath);
        if(export != null) {
            return ChunkReaderFactory.getChunkReader(export.getResourcePath(), offset, len);
        } else {
            throw new Exception("chunk for vpath (" + vpath + ") was not found");
        }
    }
    
    @Override
    public InputStream getDataChunk(String hash) throws Exception {
        LocalRecipeManager lrm = getLocalRecipeManager();
        Chunk chunk = lrm.getChunk(hash);
        if(chunk != null) {
            return ChunkReaderFactory.getChunkReader(chunk.getResourcePath(), chunk.getOffset(), chunk.getLength());
        } else {
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
    */
}
