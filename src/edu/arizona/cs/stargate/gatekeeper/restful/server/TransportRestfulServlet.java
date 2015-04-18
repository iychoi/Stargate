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
import edu.arizona.cs.stargate.gatekeeper.recipe.Chunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.ChunkReaderFactory;
import edu.arizona.cs.stargate.gatekeeper.recipe.RemoteRecipe;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.ATransportRestfulAPI;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExportManager;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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

/**
 *
 * @author iychoi
 */
@Path(ATransportRestfulAPI.BASE_PATH)
@Singleton
public class TransportRestfulServlet extends ATransportRestfulAPI {

    private static final Log LOG = LogFactory.getLog(TransportRestfulServlet.class);
    
    @GET
    @Path(ATransportRestfulAPI.RECIPE_PATH + "/{vpath:.*}")
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRecipeText(
            @DefaultValue("null") @PathParam("vpath") String vpath
    ) {
        LOG.info("vpath = " + vpath);
        try {
            return DataFormatUtils.toJSONFormat(responseGetRecipeJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ATransportRestfulAPI.RECIPE_PATH + "/{vpath:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<RemoteRecipe> responseGetRecipeJSON(
            @DefaultValue("null") @PathParam("vpath") String vpath
    ) {
        try {
            return new RestfulResponse<RemoteRecipe>(getRecipe(vpath));
        } catch(Exception ex) {
            return new RestfulResponse<RemoteRecipe>(ex);
        }
    }
    
    @Override
    public RemoteRecipe getRecipe(String vpath) throws Exception {
        DataExportManager dem = DataExportManager.getInstance();
        DataExport export = dem.getDataExport(vpath);
        if(export != null) {
            try {
                RecipeManager rm = RecipeManager.getInstance();
                LocalRecipe recipe = rm.getRecipe(export.getResourcePath());
                
                return new RemoteRecipe(export.getVirtualPath(), recipe);
            } catch (ServiceNotStartedException ex) {
                LOG.error(ex);
                return null;
            }
        } else {
            return null;
        }
    }
    
    @GET
    @Path(ATransportRestfulAPI.DATA_CHUNK_PATH + "/{param:.*}")
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
        DataExportManager dem = DataExportManager.getInstance();
        DataExport export = dem.getDataExport(vpath);
        if(export != null) {
            return ChunkReaderFactory.getChunkReader(export.getResourcePath(), offset, len);
        } else {
            throw new Exception("chunk for vpath (" + vpath + ") was not found");
        }
    }
    
    @Override
    public InputStream getDataChunk(String hash) throws Exception {
        try {
            RecipeManager rm = RecipeManager.getInstance();
            Chunk chunk = rm.getChunk(hash);
            if(chunk != null) {
                return ChunkReaderFactory.getChunkReader(chunk.getResourcePath(), chunk.getOffset(), chunk.getLength());
            } else {
                return null;
            }
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    private String[] splitPath(URI path) {
        String[] paths = new String[2];
        
        String uriPath = path.getPath();
        int startIdx = 0;
        if(uriPath.startsWith("/")) {
            startIdx++;
        }
        
        int endIdx = uriPath.indexOf("/", startIdx);
        if(endIdx > 0) {
            // cluster
            paths[0] = uriPath.substring(startIdx, endIdx);
            // vpath
            paths[1] = uriPath.substring(endIdx);
        } else {
            // cluster
            paths[0] = "local";
            // vpath
            paths[1] = "/";
        }
        
        return paths;
    }
    
    private String extractClusterName(URI path) {
        String[] splitPath = splitPath(path);
        return splitPath[0];
    }
    
    private String extractVPath(URI path) {
        String[] splitPath = splitPath(path);
        return splitPath[1];
    }
}
