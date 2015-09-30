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
package edu.arizona.cs.stargate.drivers.userinterface.http;

import edu.arizona.cs.stargate.common.restful.RestfulResponse;
import edu.arizona.cs.stargate.cluster.RemoteCluster;
import edu.arizona.cs.stargate.recipe.DataObjectMetadata;
import edu.arizona.cs.stargate.recipe.DataObjectPath;
import edu.arizona.cs.stargate.recipe.Recipe;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.service.StargateService;
import edu.arizona.cs.stargate.userinterface.AUserInterfaceServer;
import edu.arizona.cs.stargate.volume.Directory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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

/**
 *
 * @author iychoi
 */
@Path(HTTPUserInterfaceRestfulConstants.BASE_PATH)
public class HTTPUserInterfaceServlet extends AUserInterfaceServer {

    private static final Log LOG = LogFactory.getLog(HTTPUserInterfaceServlet.class);
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_LIVE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> isLiveRestful() {
        return new RestfulResponse<Boolean>(isLive());
    }
    
    @Override
    public boolean isLive() {
        return true;
    }

    @GET
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<RemoteCluster> getClusterRestful() {
        try {
            return new RestfulResponse<RemoteCluster>(getCluster());
        } catch(Exception ex) {
            return new RestfulResponse<RemoteCluster>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_DIRECTORY_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Directory> getDirectoryRestful(
        @DefaultValue("null") @QueryParam("path") String path) {
        try {
            return new RestfulResponse<Directory>(getDirectory(path));
        } catch(Exception ex) {
            return new RestfulResponse<Directory>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_METADATA_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<DataObjectMetadata> getDataObjectMetadataRestful(
        @DefaultValue("null") @QueryParam("path") String path) {
        try {
            return new RestfulResponse<DataObjectMetadata>(getDataObjectMetadata(path));
        } catch(Exception ex) {
            return new RestfulResponse<DataObjectMetadata>(ex);
        }
    }
    
    private DataObjectMetadata getDataObjectMetadata(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        DataObjectPath objectPath = new DataObjectPath(path);
        return getDataObjectMetadata(objectPath);
    }
    
    @Override
    public DataObjectMetadata getDataObjectMetadata(DataObjectPath path) throws IOException {
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
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Recipe> getRecipeRestful(
        @DefaultValue("null") @QueryParam("path") String path) {
        try {
            return new RestfulResponse<Recipe>(getRecipe(path));
        } catch(Exception ex) {
            return new RestfulResponse<Recipe>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_LIST_METADATA_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<DataObjectMetadata>> listDataObjectMetadataRestful(
            @DefaultValue("null") @QueryParam("path") String path) {
        try {
            return new RestfulResponse<Collection<DataObjectMetadata>>(listDataObjectMetadata(path));
        } catch(Exception ex) {
            return new RestfulResponse<Collection<DataObjectMetadata>>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_DATACHUNK_PATH + "/{clusterName:.*}/{hash:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getDataChunkRestful(
            @DefaultValue("null") @PathParam("clusterName") String clusterName,
            @DefaultValue("null") @PathParam("hash") String hash) throws Exception {
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

    @GET
    @Path(HTTPUserInterfaceRestfulConstants.RESTFUL_LOCAL_CLUSTER_RESOURCE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<URI> getLocalResourcePathRestful(
            @DefaultValue("null") @QueryParam("path") String path) {
        try {
            return new RestfulResponse<URI>(getLocalResourcePath(path));
        } catch(Exception ex) {
            return new RestfulResponse<URI>(ex);
        }
    }
    
    private URI getLocalResourcePath(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        DataObjectPath objectPath = new DataObjectPath(path);
        return getLocalResourcePath(objectPath);
    }
    
    @Override
    public URI getLocalResourcePath(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        try {
            //TODO
            StargateService service = StargateService.getInstance();
            return null;
        } catch (ServiceNotStartedException ex) {
            throw new IOException(ex);
        }
    }
}
