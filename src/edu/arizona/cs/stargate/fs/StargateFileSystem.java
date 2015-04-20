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

package edu.arizona.cs.stargate.fs;

import edu.arizona.cs.stargate.common.DateTimeUtils;
import edu.arizona.cs.stargate.common.PathUtils;
import edu.arizona.cs.stargate.gatekeeper.dataexport.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClientConfiguration;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.restful.client.FileSystemRestfulClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class StargateFileSystem {
    
    private static final Log LOG = LogFactory.getLog(StargateFileSystem.class);
    
    private GateKeeperClient gatekeeperClient;
    private FileSystemRestfulClient filesystemClient;
    private Cluster localCluster;
    private Map<String, VirtualFileStatus> mappedEntries = new HashMap<String, VirtualFileStatus>();
    private long lastUpdatedTime;
    
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
        
        syncLocalCluster();
        syncMappedEntries();
    }

    private synchronized void syncLocalCluster() throws IOException {
        try {
            this.localCluster = this.gatekeeperClient.getRestfulClient().getClusterManagerClient().getLocalCluster();
        } catch (Exception ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    private VirtualFileStatus makeRootDirectoryStatus() {
        long blockSize = this.filesystemClient.getBlockSize();
        return new VirtualFileStatus("", "/", null, true, 4096, 1, blockSize, this.lastUpdatedTime);
    }
    
    private VirtualFileStatus makeParentDirectoryStatus(VirtualFileStatus status) {
        if(status.getClusterName() == null || status.getClusterName().isEmpty()) {
            return null;
        }
        
        String parentPath = PathUtils.getParent(status.getVirtualPath());
        if(parentPath != null) {
            return new VirtualFileStatus(status.getClusterName(), parentPath, null, true, 4096, 1, 4096, this.lastUpdatedTime);
        }
        return null;
    }
    
    private synchronized void putMapping(VirtualFileStatus status) {
        String path = PathUtils.getPath(status);
        if(path == null || path.isEmpty()) {
            return;
        }
        
        if(!this.mappedEntries.containsKey(path)) {
            VirtualFileStatus parentStatus = makeParentDirectoryStatus(status);
            if(parentStatus != null) {
                putMapping(parentStatus);
            }
            
            this.mappedEntries.put(path, status);
        }
    }
    
    private synchronized void syncMappedEntries() throws IOException {
        this.lastUpdatedTime = DateTimeUtils.getCurrentTime();
        this.mappedEntries.clear();
        
        VirtualFileStatus rootFileStatus = makeRootDirectoryStatus();
        putMapping(rootFileStatus);
        
        Collection<VirtualFileStatus> list_status = this.filesystemClient.getAllVirtualFileStatus();
        for(VirtualFileStatus status : list_status) {
            putMapping(status);
        }
    }
    
    private synchronized String getClusterName(URI resourceURI) {
        String clusterName = resourceURI.getHost();
        if(clusterName.equalsIgnoreCase("localhost")) {
            return this.localCluster.getName();
        }
        return clusterName;
    }
    
    private synchronized String getVirtualPath(URI resourceURI) {
        return resourceURI.getPath();
    }
    
    private synchronized String makeMappedPath(URI resourceURI) {
        String clusterName = getClusterName(resourceURI);
        String virtualPath = getVirtualPath(resourceURI);
        
        return PathUtils.getPath(clusterName, virtualPath);
    }
    
    public synchronized VirtualFileStatus[] listStatus(URI resourceURI) {
        String mappedPath = makeMappedPath(resourceURI);
        
        ArrayList<VirtualFileStatus> list_status = new ArrayList<VirtualFileStatus>();
        Set<String> keySet = this.mappedEntries.keySet();
        for(String path : keySet) {
            String parent = PathUtils.getParent(path);
            if(parent != null && parent.equals(mappedPath)) {
                list_status.add(this.mappedEntries.get(path));
            }
        }
        return list_status.toArray(new VirtualFileStatus[0]);
    }

    public synchronized InputStream open(URI resourceURI, int bufferSize) {
        String mappedPath = makeMappedPath(resourceURI);
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public synchronized VirtualFileStatus getFileStatus(URI resourceURI) {
        String mappedPath = makeMappedPath(resourceURI);
        return this.mappedEntries.get(mappedPath);
    }

    public synchronized long getBlockSize() {
        return this.mappedEntries.get("/").getBlockSize();
    }

    public synchronized URI getLocalClusterPath(URI resourceURI) {
        String mappedPath = makeMappedPath(resourceURI);
        VirtualFileStatus status = this.mappedEntries.get(mappedPath);
        
        if(status != null) {
            return status.getResourcePath();
        }
        return null;
    }

    public synchronized boolean isLocalClusterPath(URI resourceURI) {
        String clusterName = getClusterName(resourceURI);
        
        if(clusterName.equals(this.localCluster.getName())) {
            return true;
        }
        return false;
    }
    
    public synchronized void close() {
        this.gatekeeperClient.stop();
    }
}
