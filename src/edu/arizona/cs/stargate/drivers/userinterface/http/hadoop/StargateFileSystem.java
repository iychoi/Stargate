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

package edu.arizona.cs.stargate.drivers.userinterface.http.hadoop;

import edu.arizona.cs.stargate.cluster.RemoteCluster;
import edu.arizona.cs.stargate.recipe.DataObjectMetadata;
import edu.arizona.cs.stargate.recipe.DataObjectPath;
import edu.arizona.cs.stargate.recipe.Recipe;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPChunkInputStream;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

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
        
        if(this.localCluster.getName().equalsIgnoreCase(clusterName) ||
            clusterName.equals("localhost")) {
            return true;
        }
        return false;
    }
    
    private DataObjectPath makeDataObjectPath(URI path) {
        return new DataObjectPath(getClusterName(path), getPathPart(path));
    }
    
    private StargateFileStatus makeStargateFileStatus(DataObjectMetadata metadata, URI resourceURI) throws IOException {
        if(isLocalClusterPath(metadata.getPath())) {
            return new StargateFileStatus(metadata, DEFAULT_BLOCK_SIZE, resourceURI, this.userInterfaceClient.getLocalResourcePath(metadata.getPath()));
        } else {
            return new StargateFileStatus(metadata, DEFAULT_BLOCK_SIZE, resourceURI);
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
                    status.add(makeStargateFileStatus(m, resourceURI.resolve(m.getPath().getName())));
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
