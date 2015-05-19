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

package edu.arizona.cs.stargate.client.fs;

import edu.arizona.cs.stargate.common.PathUtils;
import edu.arizona.cs.stargate.gatekeeper.filesystem.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClientConfiguration;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.restful.client.FileSystemRestfulClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * @author iychoi
 */
public class StargateFileSystem {
    
    private static final Log LOG = LogFactory.getLog(StargateFileSystem.class);
    
    private GateKeeperClient gatekeeperClient;
    private FileSystemRestfulClient filesystemClient;
    private Cluster localCluster;
    
    public StargateFileSystem(String gatekeeperHost) throws IOException {
        String gatekeeperServiceURL = gatekeeperHost;
        if(!gatekeeperHost.startsWith("http://")) {
            gatekeeperServiceURL = "http://" + gatekeeperHost;
        }
        
        try {
            initialize(new URI(gatekeeperServiceURL));
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    public StargateFileSystem(URI gatekeeperServiceURL) throws IOException {
        initialize(gatekeeperServiceURL);
    }
    
    public void initialize(URI gatekeeperServiceURL) throws IOException {
        GateKeeperClientConfiguration config = new GateKeeperClientConfiguration(gatekeeperServiceURL);
        LOG.info("connecting to GateKeeper : " + gatekeeperServiceURL.toASCIIString());
        
        this.gatekeeperClient = new GateKeeperClient(config);

        this.gatekeeperClient.start();
        
        this.filesystemClient = this.gatekeeperClient.getRestfulClient().getFileSystemClient();
        
        try {
            this.localCluster = this.filesystemClient.getLocalCluster();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
    
    private synchronized String getClusterName(URI resourceURI) {
        return PathUtils.extractClusterNameFromPath(resourceURI);
    }
    
    private synchronized String getVirtualPath(URI resourceURI) {
        return PathUtils.extractVirtualPath(resourceURI);
    }
    
    private synchronized String makeMappedPath(URI resourceURI) {
        String clusterName = getClusterName(resourceURI);
        String virtualPath = getVirtualPath(resourceURI);
        
        return PathUtils.concatPath(clusterName, virtualPath);
    }
    
    public synchronized VirtualFileStatus[] listStatus(URI resourceURI) throws IOException {
        try {
            String mappedPath = makeMappedPath(resourceURI);
            Collection<VirtualFileStatus> status = this.filesystemClient.listStatus(mappedPath);
            if(status != null) {
                return status.toArray(new VirtualFileStatus[0]);
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized InputStream open(URI resourceURI, int bufferSize) throws IOException {
        VirtualFileStatus status = getFileStatus(resourceURI);
        if(status != null) {
            return new ChunkInputStream(this.filesystemClient, status);
        }
        return null;
    }
    
    public synchronized FSDataInputStream open2(URI resourceURI, int bufferSize) throws IOException {
        VirtualFileStatus status = getFileStatus(resourceURI);
        if(status != null) {
            return new FSDataInputStream(new FSChunkInputStream(this.filesystemClient, status));
        }
        return null;
    }

    public synchronized VirtualFileStatus getFileStatus(URI resourceURI) throws IOException {
        try {
            String mappedPath = makeMappedPath(resourceURI);
            return this.filesystemClient.getFileStatus(mappedPath);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized long getBlockSize() {
        try {
            VirtualFileStatus status = this.filesystemClient.getFileStatus("/");
            return status.getBlockSize();
        } catch (Exception ex) {
            return 1024*1024; // default
        }
    }

    public synchronized URI getLocalClusterResourcePath(URI resourceURI) throws IOException {
        VirtualFileStatus status = getFileStatus(resourceURI);
        if(status != null) {
            return status.getLocalResourcePath();
        }
        return null;
    }

    public synchronized boolean isLocalClusterPath(URI resourceURI) {
        String clusterName = getClusterName(resourceURI);
        
        if(this.localCluster.getName().equalsIgnoreCase(clusterName) ||
            clusterName.equalsIgnoreCase("localhost")) {
            return true;
        }
        return false;
    }
    
    public synchronized void close() {
        this.gatekeeperClient.stop();
    }
}
