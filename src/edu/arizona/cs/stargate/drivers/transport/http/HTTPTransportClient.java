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
package edu.arizona.cs.stargate.drivers.transport.http;

import edu.arizona.cs.stargate.common.restful.RestfulResponse;
import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.cluster.RemoteCluster;
import edu.arizona.cs.stargate.common.restful.RestfulClient;
import edu.arizona.cs.stargate.common.restful.WebParamBuilder;
import edu.arizona.cs.stargate.common.utils.PathUtils;
import edu.arizona.cs.stargate.recipe.DataObjectMetadata;
import edu.arizona.cs.stargate.recipe.DataObjectPath;
import edu.arizona.cs.stargate.recipe.Recipe;
import edu.arizona.cs.stargate.transport.ATransportClient;
import edu.arizona.cs.stargate.volume.Directory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class HTTPTransportClient extends ATransportClient {

    private static final Log LOG = LogFactory.getLog(HTTPTransportClient.class);
    
    private RestfulClient restfulClient;
    
    public HTTPTransportClient(URI serviceURL, int threadPoolSize) {
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
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.BASE_PATH, path);
    }
    
    @Override
    public boolean isLive() {
        RestfulResponse<Boolean> response;
        try {
            String url = getResourcePath(HTTPTransportRestfulConstants.RESTFUL_LIVE_PATH);
            response = (RestfulResponse<Boolean>) this.restfulClient.get(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
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
            String url = getResourcePath(HTTPTransportRestfulConstants.RESTFUL_CLUSTER_PATH);
            response = (RestfulResponse<RemoteCluster>) this.restfulClient.get(url, new GenericType<RestfulResponse<RemoteCluster>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
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
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPTransportRestfulConstants.RESTFUL_DIRECTORY_PATH));
            builder.addParam("name", path.toString());
            String url = builder.build();
            response = (RestfulResponse<Directory>) this.restfulClient.get(url, new GenericType<RestfulResponse<Directory>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
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
        
        RestfulResponse<DataObjectMetadata> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPTransportRestfulConstants.RESTFUL_METADATA_PATH));
            builder.addParam("name", path.toString());
            String url = builder.build();
            response = (RestfulResponse<DataObjectMetadata>) this.restfulClient.get(url, new GenericType<RestfulResponse<DataObjectMetadata>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
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
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPTransportRestfulConstants.RESTFUL_RECIPE_PATH));
            builder.addParam("name", path.toString());
            String url = builder.build();
            response = (RestfulResponse<Recipe>) this.restfulClient.get(url, new GenericType<RestfulResponse<Recipe>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
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
        
        RestfulResponse<Collection<DataObjectMetadata>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(HTTPTransportRestfulConstants.RESTFUL_LIST_METADATA_PATH));
            builder.addParam("name", path.toString());
            String url = builder.build();
            response = (RestfulResponse<Collection<DataObjectMetadata>>) this.restfulClient.get(url, new GenericType<RestfulResponse<Collection<DataObjectMetadata>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
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
            String datachunkUrl = PathUtils.concatPath(HTTPTransportRestfulConstants.RESTFUL_DATACHUNK_PATH, clusterName + "/" + hash);
            String url = getResourcePath(datachunkUrl);
            return this.restfulClient.download(url);
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
    }
}
