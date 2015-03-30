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
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.ChunkReaderFactory;
import edu.arizona.cs.stargate.common.recipe.Recipe;
import edu.arizona.cs.stargate.gatekeeper.ATransportManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
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
@Path(ATransportManagerAPI.PATH)
@Singleton
public class TransportManagerRestful extends ATransportManagerAPI {

    private static final Log LOG = LogFactory.getLog(TransportManagerRestful.class);
    
    @GET
    @Path(ATransportManagerAPI.GET_RECIPE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRecipeText(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        LOG.info("vpath = " + vpath);
        try {
            return DataFormatter.toJSONFormat(responseGetRecipeJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Recipe> responseGetRecipeJSON(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        LOG.info("vpath = " + vpath);
        try {
            return new RestfulResponse<Recipe>(getRecipe(vpath));
        } catch(Exception ex) {
            return new RestfulResponse<Recipe>(ex);
        }
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_RECIPE_URL_PATH + "/{vpath:.*}")
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRecipeTextURL(
            @DefaultValue("null") @PathParam("vpath") String vpath
    ) {
        LOG.info("vpath = " + vpath);
        try {
            return DataFormatter.toJSONFormat(responseGetRecipeJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_RECIPE_URL_PATH + "/{vpath:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Recipe> responseGetRecipeJSONURL(
            @DefaultValue("null") @PathParam("vpath") String vpath
    ) {
        LOG.info("vpath = " + vpath);
        try {
            return new RestfulResponse<Recipe>(getRecipe(vpath));
        } catch(Exception ex) {
            return new RestfulResponse<Recipe>(ex);
        }
    }
    
    @Override
    public Recipe getRecipe(String vpath) throws Exception {
        TransportManager tm = getTransportManager();
        return tm.getRecipe(vpath);
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_CHUNK_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetChunkInfoText(
            @DefaultValue("null") @QueryParam("hash") String hash
    ) {
        LOG.info("hash = " + hash);
        try {
            return DataFormatter.toJSONFormat(responseGetChunkInfoJSON(hash));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_CHUNK_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<ChunkInfo> responseGetChunkInfoJSON(
            @DefaultValue("null") @QueryParam("hash") String hash
    ) {
        LOG.info("hash = " + hash);
        try {
            return new RestfulResponse<ChunkInfo>(getChunkInfo(hash));
        } catch(Exception ex) {
            return new RestfulResponse<ChunkInfo>(ex);
        }
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_CHUNK_INFO_URL_PATH + "/{hash:.*}")
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetChunkInfoTextURL(
            @DefaultValue("null") @PathParam("hash") String hash
    ) {
        LOG.info("hash = " + hash);
        try {
            return DataFormatter.toJSONFormat(responseGetChunkInfoJSON(hash));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_CHUNK_INFO_URL_PATH + "/{hash:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<ChunkInfo> responseGetChunkInfoJSONURL(
            @DefaultValue("null") @PathParam("hash") String hash
    ) {
        LOG.info("hash = " + hash);
        try {
            return new RestfulResponse<ChunkInfo>(getChunkInfo(hash));
        } catch(Exception ex) {
            return new RestfulResponse<ChunkInfo>(ex);
        }
    }

    @Override
    public ChunkInfo getChunkInfo(String hash) throws Exception {
        TransportManager tm = getTransportManager();
        return tm.getChunkInfo(hash);
    }
    
    @GET
    @Path(ATransportManagerAPI.GET_DATA_CHUNK_PATH)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response responseGetDataChunk(
            @DefaultValue("null") @QueryParam("vpath") String vpath,
            @DefaultValue("0") @QueryParam("offset") long offset,
            @DefaultValue("0") @QueryParam("len") int len,
            @DefaultValue("null") @QueryParam("hash") String hash
    ) throws Exception {
        LOG.info("vpath = " + vpath);
        LOG.info("offset = " + offset);
        LOG.info("len = " + len);
        LOG.info("hash = " + hash);
        if(hash != null) {
            final InputStream is = getDataChunk(hash);
            
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
            
            return Response.ok(stream).header("content-disposition", "attachment; filename = " + hash).build();
        } else if(vpath != null && len > 0) {
            final InputStream is = getDataChunk(vpath, offset, len);
            
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
    
    @GET
    @Path(ATransportManagerAPI.GET_DATA_CHUNK_URL_PATH + "/{param:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response responseGetDataChunkURL(
            @DefaultValue("null") @PathParam("param") String param,
            @DefaultValue("0") @QueryParam("offset") long offset,
            @DefaultValue("0") @QueryParam("len") int len
    ) throws Exception {
        String hash = null;
        String vpath = null;
        LOG.info("param = " + param);
        LOG.info("offset = " + offset);
        LOG.info("len = " + len);
        if(param != null) {
            if(!param.contains(".") && !param.contains("/")) {
                hash = param;
            } else {
                vpath = param;
            }
        }
        if(hash != null) {
            final InputStream is = getDataChunk(hash);
            
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
        TransportManager tm = getTransportManager();
        URI resourcePath = tm.getResourcePath(vpath);
        if(resourcePath != null) {
            return ChunkReaderFactory.getChunkReader(resourcePath, offset, len);
        } else {
            return null;
        }
    }
    
    @Override
    public InputStream getDataChunk(String hash) throws Exception {
        TransportManager tm = getTransportManager();
        ChunkInfo chunk = tm.getChunkInfo(hash);

        if(chunk != null) {
            return ChunkReaderFactory.getChunkReader(chunk.getResourcePath(), chunk.getChunkStart(), chunk.getChunkLen());
        } else {
            return null;
        }
    }

    private TransportManager getTransportManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getTransportManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
