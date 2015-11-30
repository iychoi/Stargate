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
package stargate.drivers.userinterface.http;

import com.sun.jersey.api.client.GenericType;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.client.restful.RestfulClient;
import stargate.client.restful.RestfulResponse;
import stargate.client.restful.WebParamBuilder;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.commons.userinterface.AUserInterfaceClient;
import stargate.commons.utils.PathUtils;
import stargate.commons.volume.Directory;

/**
 *
 * @author iychoi
 */
public class HTTPUserInterfaceClient extends AUserInterfaceClient {

    private static final Log LOG = LogFactory.getLog(HTTPUserInterfaceClient.class);
    
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    
    private RestfulClient restfulClient;
    
    public HTTPUserInterfaceClient(URI serviceURL) {
        if(serviceURL == null) {
            throw new IllegalArgumentException("serviceURL is null");
        }
        
        this.restfulClient = new RestfulClient(serviceURL, DEFAULT_THREAD_POOL_SIZE);
    }
    
    public HTTPUserInterfaceClient(URI serviceURL, int threadPoolSize) {
        if(serviceURL == null) {
            throw new IllegalArgumentException("serviceURL is null");
        }
        
        if(threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize is invalid");
        }
        
        this.restfulClient = new RestfulClient(serviceURL, threadPoolSize);
    }
    
    public void close() {
        this.restfulClient.close();
    }
    
    public String getResourcePath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        return PathUtils.concatPath(HTTPUserInterfaceRestfulConstants.BASE_PATH, path);
    }
    
    @Override
    public boolean isLive() {
        RestfulResponse<Boolean> response;
        try {
            String url = getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_LIVE_PATH);
            LOG.debug(url);
            response = (RestfulResponse<Boolean>) this.restfulClient.get(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            return false;
        }
        
        if(response.getException() != null) {
            return false;
        } else {
            return response.getResponse().booleanValue();
        }
    }

    @Override
    public RemoteCluster getCluster() throws IOException {
        RestfulResponse<RemoteCluster> response;
        try {
            String url = getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_CLUSTER_PATH);
            LOG.debug(url);
            response = (RestfulResponse<RemoteCluster>) this.restfulClient.get(url, new GenericType<RestfulResponse<RemoteCluster>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw new IOException(response.getException());
        } else {
            return response.getResponse();
        }
    }

    @Override
    public Directory getDirectory(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        RestfulResponse<Directory> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_DIRECTORY_PATH));
            builder.addParam("path", path.toString());
            String url = builder.build();
            LOG.debug(url);
            response = (RestfulResponse<Directory>) this.restfulClient.get(url, new GenericType<RestfulResponse<Directory>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw new IOException(response.getException());
        } else {
            return response.getResponse();
        }
    }

    @Override
    public DataObjectMetadata getDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        LOG.info("getDataObjectMetadata : " + path.toString());
        
        RestfulResponse<DataObjectMetadata> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_METADATA_PATH));
            builder.addParam("path", path.toString());
            String url = builder.build();
            LOG.debug(url);
            response = (RestfulResponse<DataObjectMetadata>) this.restfulClient.get(url, new GenericType<RestfulResponse<DataObjectMetadata>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw new IOException(response.getException());
        } else {
            return response.getResponse();
        }
    }

    @Override
    public Recipe getRecipe(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        RestfulResponse<Recipe> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_RECIPE_PATH));
            builder.addParam("path", path.toString());
            String url = builder.build();
            LOG.debug(url);
            response = (RestfulResponse<Recipe>) this.restfulClient.get(url, new GenericType<RestfulResponse<Recipe>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw new IOException(response.getException());
        } else {
            return response.getResponse();
        }
    }

    @Override
    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        LOG.info("listDataObjectMetadata : " + path.toString());
        
        RestfulResponse<Collection<DataObjectMetadata>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_LIST_METADATA_PATH));
            builder.addParam("path", path.toString());
            String url = builder.build();
            LOG.debug(url);
            response = (RestfulResponse<Collection<DataObjectMetadata>>) this.restfulClient.get(url, new GenericType<RestfulResponse<Collection<DataObjectMetadata>>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }

        if(response.getException() != null) {
            throw new IOException(response.getException());
        } else {
            return response.getResponse();
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
            String datachunkUrl = PathUtils.concatPath(HTTPUserInterfaceRestfulConstants.RESTFUL_DATACHUNK_PATH, clusterName + "/" + hash);
            String url = getResourcePath(datachunkUrl);
            LOG.debug(url);
            return this.restfulClient.download(url);
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }
    }

    @Override
    public URI getLocalResourcePath(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        RestfulResponse<URI> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPUserInterfaceRestfulConstants.RESTFUL_LOCAL_CLUSTER_RESOURCE_PATH));
            builder.addParam("path", path.toString());
            String url = builder.build();
            LOG.debug(url);
            response = (RestfulResponse<URI>) this.restfulClient.get(url, new GenericType<RestfulResponse<URI>>(){});
        } catch (IOException ex) {
            LOG.error("Exception occurred while calling Restful operation", ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw new IOException(response.getException());
        } else {
            return response.getResponse();
        }
    }
}
