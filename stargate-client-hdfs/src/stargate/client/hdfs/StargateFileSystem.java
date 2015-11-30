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

package stargate.client.hdfs;

import stargate.drivers.userinterface.http.HTTPChunkInputStream;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;

/**
 *
 * @author iychoi
 */
public class StargateFileSystem {
    
    private static final Log LOG = LogFactory.getLog(StargateFileSystem.class);
    
    private static final int DEFAULT_BLOCK_SIZE = 1024*1024;
    
    private HTTPUserInterfaceClient userInterfaceClient;
    private RemoteCluster localCluster;
    
    public StargateFileSystem(String stargateUIServiceURL) throws IOException {
        if(stargateUIServiceURL == null) {
            throw new IllegalArgumentException("stargateUIServiceURL is null");
        }
        
        if(!stargateUIServiceURL.startsWith("http://")) {
            stargateUIServiceURL = "http://" + stargateUIServiceURL;
        }
        
        try {
            initialize(new URI(stargateUIServiceURL));
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    public void initialize(URI stargateUIServiceURL) throws IOException {
        if(stargateUIServiceURL == null) {
            throw new IllegalArgumentException("stargateUIServiceURL is null");
        }
        
        LOG.info("connecting to Stargate : " + stargateUIServiceURL.toASCIIString());
        
        this.userInterfaceClient = new HTTPUserInterfaceClient(stargateUIServiceURL);

        if(!this.userInterfaceClient.isLive()) {
            throw new IOException("cannot connect to Stargate : " + stargateUIServiceURL.toASCIIString());
        }
        
        this.localCluster = this.userInterfaceClient.getCluster();
        
        LOG.info("connected : " + stargateUIServiceURL.toASCIIString());
    }
    
    private String getClusterName(URI resourceURI) {
        String path = resourceURI.getPath();
        
        int startIdx = 0;
        if(path.startsWith("/")) {
            startIdx++;
        }
        
        int endIdx = path.indexOf("/", startIdx);
        if(endIdx > 0) {
            return path.substring(startIdx, endIdx);
        } else {
            if(path.length() - startIdx > 0) {
                return path.substring(startIdx, path.length());
            }
        }
        return "";
    }
    
    private String getPathPart(URI resourceURI) {
        String path = resourceURI.getPath();
        
        int startIdx = 0;
        if(path.startsWith("/")) {
            startIdx++;
        }
        
        int endIdx = path.indexOf("/", startIdx);
        if(endIdx > 0) {
            return path.substring(endIdx, path.length());
        } else {
            return "";
        }
    }
    
    private boolean isLocalClusterPath(DataObjectPath path) {
        String clusterName = path.getClusterName();
        if(clusterName == null || clusterName.isEmpty()) {
            // root
            return false;
        }
        
        if(this.localCluster.getName().equalsIgnoreCase(clusterName) ||
            clusterName.equals("localhost")) {
            // if path is for a file
            String p = path.getPath();
            if(p == null || p.isEmpty()) {
                return false;
            }
            
            if(p.endsWith("/")) {
                return false;
            }
            return true;
        }
        return false;
    }
    
    private DataObjectPath makeDataObjectPath(URI path) {
        return new DataObjectPath(getClusterName(path), getPathPart(path));
    }
    
    private URI urify(DataObjectPath path) throws URISyntaxException {
        String clusterName = path.getClusterName();
        String p = path.getPath();
        
        if(clusterName == null || clusterName.isEmpty()) {
            return new URI("/");
        }
        
        if(p == null || p.isEmpty() || p.equals("/")) {
            return new URI("/" + clusterName);
        }
        
        return new URI("/" + clusterName + p);
    }
    
    private StargateFileStatus makeStargateFileStatus(DataObjectMetadata metadata, URI resourceURI) throws IOException {
        if(!metadata.isDirectory() && isLocalClusterPath(metadata.getPath())) {
            try {
                URI metaURI = urify(metadata.getPath());
                URI absURI = resourceURI.resolve(metaURI);
                return new StargateFileStatus(metadata, DEFAULT_BLOCK_SIZE, absURI, this.userInterfaceClient.getLocalResourcePath(metadata.getPath()));
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        } else {
            try {
                URI metaURI = urify(metadata.getPath());
                URI absURI = resourceURI.resolve(metaURI);
                return new StargateFileStatus(metadata, DEFAULT_BLOCK_SIZE, absURI);
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized Collection<StargateFileStatus> listStatus(URI resourceURI) throws IOException {
        if(resourceURI == null) {
            throw new IllegalArgumentException("resourceURI is null");
        }
        
        try {
            DataObjectPath path = makeDataObjectPath(resourceURI);
            List<StargateFileStatus> status = new ArrayList<StargateFileStatus>();
            Collection<DataObjectMetadata> metadata = this.userInterfaceClient.listDataObjectMetadata(path);
            if(metadata != null) {
                for(DataObjectMetadata m : metadata) {
                    status.add(makeStargateFileStatus(m, resourceURI));
                }
            }
            return status;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized FSDataInputStream open(URI resourceURI, int bufferSize) throws IOException {
        if(resourceURI == null) {
            throw new IllegalArgumentException("resourceURI is null");
        }
        
        DataObjectPath path = makeDataObjectPath(resourceURI);
        Recipe recipe = this.userInterfaceClient.getRecipe(path);
        if(recipe != null) {
            return new FSDataInputStream(new HTTPChunkInputStream(this.userInterfaceClient, recipe));
        }
        return null;
    }

    public synchronized StargateFileStatus getFileStatus(URI resourceURI) throws IOException {
        if(resourceURI == null) {
            throw new IllegalArgumentException("resourceURI is null");
        }
        
        try {
            DataObjectPath path = makeDataObjectPath(resourceURI);
            DataObjectMetadata metadata = this.userInterfaceClient.getDataObjectMetadata(path);
            if(metadata == null) {
                return null;
            }
            return makeStargateFileStatus(metadata, resourceURI);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized long getBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }
    
    public synchronized void close() {
        this.userInterfaceClient.close();
    }
}
