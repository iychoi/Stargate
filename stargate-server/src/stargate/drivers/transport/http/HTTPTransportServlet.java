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
package stargate.drivers.transport.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
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
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulResponse;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.transport.ATransportServer;
import stargate.commons.volume.Directory;
import stargate.server.service.StargateService;

/**
 *
 * @author iychoi
 */
@Path(HTTPTransportRestfulConstants.BASE_PATH)
public class HTTPTransportServlet extends ATransportServer {

    private static final Log LOG = LogFactory.getLog(HTTPTransportServlet.class);
    
    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_LIVE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response isLiveRestful() {
        try {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(isLive());
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public boolean isLive() {
        return true;
    }

    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterRestful() {
        try {
            RestfulResponse<RemoteCluster> rres = new RestfulResponse<RemoteCluster>(getCluster());
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<RemoteCluster> rres = new RestfulResponse<RemoteCluster>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public RemoteCluster getCluster() throws IOException {
        try {
            StargateService service = StargateService.getInstance();
            return service.getClusterManager().getLocalClusterManager().toRemoteCluster();
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }

    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_DIRECTORY_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDirectoryRestful(
        @DefaultValue("") @QueryParam("path") String path) {
        try {
            RestfulResponse<Directory> rres = new RestfulResponse<Directory>(getDirectory(path));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Directory> rres = new RestfulResponse<Directory>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    private Directory getDirectory(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        DataObjectPath objectPath = new DataObjectPath(path);
        return getDirectory(objectPath);
    }
    
    @Override
    public Directory getDirectory(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        try {
            StargateService service = StargateService.getInstance();
            return service.getVolumeManager().getDirectory(path);
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }

    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_METADATA_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataObjectMetadataRestful(
        @DefaultValue("") @QueryParam("path") String path) {
        try {
            RestfulResponse<DataObjectMetadata> rres = new RestfulResponse<DataObjectMetadata>(getDataObjectMetadata(path));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(FileNotFoundException ex) {
            return Response.status(Response.Status.NOT_FOUND).entity(ex.toString()).build();
        } catch(Exception ex) {
            RestfulResponse<DataObjectMetadata> rres = new RestfulResponse<DataObjectMetadata>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    private DataObjectMetadata getDataObjectMetadata(String path) throws IOException, FileNotFoundException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        DataObjectPath objectPath = new DataObjectPath(path);
        return getDataObjectMetadata(objectPath);
    }
    
    @Override
    public DataObjectMetadata getDataObjectMetadata(DataObjectPath path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        try {
            StargateService service = StargateService.getInstance();
            return service.getVolumeManager().getDataObjectMetadata(path);
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecipeRestful(
        @DefaultValue("") @QueryParam("path") String path) {
        try {
            RestfulResponse<Recipe> rres = new RestfulResponse<Recipe>(getRecipe(path));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Recipe> rres = new RestfulResponse<Recipe>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    private Recipe getRecipe(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        DataObjectPath objectPath = new DataObjectPath(path);
        return getRecipe(objectPath);
    }

    @Override
    public Recipe getRecipe(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        try {
            StargateService service = StargateService.getInstance();
            return service.getVolumeManager().getRecipe(path);
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_LIST_METADATA_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataObjectMetadataRestful(
            @DefaultValue("") @QueryParam("path") String path) {
        try {
            RestfulResponse<Collection<DataObjectMetadata>> rres = new RestfulResponse<Collection<DataObjectMetadata>>(listDataObjectMetadata(path));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<DataObjectMetadata>> rres = new RestfulResponse<Collection<DataObjectMetadata>>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    private Collection<DataObjectMetadata> listDataObjectMetadata(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        DataObjectPath objectPath = new DataObjectPath(path);
        return listDataObjectMetadata(objectPath);
    }

    @Override
    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        try {
            StargateService service = StargateService.getInstance();
            return service.getVolumeManager().listDataObjectMetadata(path);
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.RESTFUL_DATACHUNK_PATH + "/{clusterName:.*}/{hash:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getDataChunkRestful(
            @DefaultValue("") @PathParam("clusterName") String clusterName,
            @DefaultValue("") @PathParam("hash") String hash) throws Exception {
        
        try {
            final InputStream is = getDataChunk(clusterName, hash);
            if(is == null) {
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

            return Response.ok(stream).header("content-disposition", "attachment; filename = " + hash).build();
        } catch (Exception ex) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }

    @Override
    public InputStream getDataChunk(String clusterName, String hash) throws IOException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            StargateService service = StargateService.getInstance();
            return service.getVolumeManager().getDataChunk(clusterName, hash);
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }
}
