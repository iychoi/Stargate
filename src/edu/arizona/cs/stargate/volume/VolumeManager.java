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

package edu.arizona.cs.stargate.volume;

import edu.arizona.cs.stargate.recipe.DataObjectPath;
import edu.arizona.cs.stargate.recipe.DataObjectMetadata;
import edu.arizona.cs.stargate.cluster.ClusterManager;
import edu.arizona.cs.stargate.cluster.RemoteCluster;
import edu.arizona.cs.stargate.common.utils.DateTimeUtils;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.datastore.ADistributedDataStore;
import edu.arizona.cs.stargate.datastore.DataStoreManager;
import edu.arizona.cs.stargate.policy.PolicyManager;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.recipe.Recipe;
import edu.arizona.cs.stargate.recipe.RecipeChunk;
import edu.arizona.cs.stargate.recipe.RecipeManager;
import edu.arizona.cs.stargate.sourcefs.ASourceFileSystem;
import edu.arizona.cs.stargate.sourcefs.SourceFileSystemManager;
import edu.arizona.cs.stargate.transport.ATransportClient;
import edu.arizona.cs.stargate.transport.TransportManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class VolumeManager {
    
    private static final Log LOG = LogFactory.getLog(VolumeManager.class);
    
    private static final String VOLUMEMANAGER_DIRECTORY_HIERARCHY_MAP_ID = "VolumeManager_Directory_Hierarchy";
    
    private static final long DIRECTORY_METADATA_SIZE = 4*1024;
    
    private static VolumeManager instance;
    
    private PolicyManager policyManager;
    private DataStoreManager dataStoreManager;
    private SourceFileSystemManager sourceFileSystemManager;
    private ClusterManager clusterManager;
    private DataExportManager dataExportManager;
    private RecipeManager recipeManager;
    private TransportManager transportManager;
    
    private ADistributedDataStore directoryHierarchy;
    
    protected long lastUpdateTime;
    
    public static VolumeManager getInstance(PolicyManager policyManager, DataStoreManager dataStoreManager, SourceFileSystemManager sourceFileSystemManager, ClusterManager clusterManager, DataExportManager dataExportManager, RecipeManager recipeManager, TransportManager transportManager) throws IOException {
        synchronized (VolumeManager.class) {
            if(instance == null) {
                instance = new VolumeManager(policyManager, dataStoreManager, sourceFileSystemManager, clusterManager, dataExportManager, recipeManager, transportManager);
            }
            return instance;
        }
    }
    
    public static VolumeManager getInstance() throws ServiceNotStartedException {
        synchronized (VolumeManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("VolumeManager is not started");
            }
            return instance;
        }
    }
    
    VolumeManager(PolicyManager policyManager, DataStoreManager dataStoreManager, SourceFileSystemManager sourceFileSystemManager, ClusterManager clusterManager, DataExportManager dataExportManager, RecipeManager recipeManager, TransportManager transportManager) throws IOException {
        if(policyManager == null) {
            throw new IllegalArgumentException("policyManager is null");
        }
        
        if(dataStoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is null");
        }
        
        if(sourceFileSystemManager == null) {
            throw new IllegalArgumentException("sourceFileSystemManager is null");
        }
        
        if(clusterManager == null) {
            throw new IllegalArgumentException("clusterManager is null");
        }
        
        if(dataExportManager == null) {
            throw new IllegalArgumentException("dataExportManager is null");
        }
        
        if(recipeManager == null) {
            throw new IllegalArgumentException("recipeManager is null");
        }
        
        if(transportManager == null) {
            throw new IllegalArgumentException("transportManager is null");
        }
        
        this.policyManager = policyManager;
        this.dataStoreManager = dataStoreManager;
        this.sourceFileSystemManager = sourceFileSystemManager;
        this.clusterManager = clusterManager;
        this.dataExportManager = dataExportManager;
        this.recipeManager = recipeManager;
        this.transportManager = transportManager;

        this.directoryHierarchy = this.dataStoreManager.getDistributedDataStore(VOLUMEMANAGER_DIRECTORY_HIERARCHY_MAP_ID, String.class, Directory.class);
        
        // make local cluster root
        Directory localClusterRootDir = makeLocalClusterRootDirectory();
        this.directoryHierarchy.put(localClusterRootDir.getPath().toString(), localClusterRootDir);
    }
    
    private Directory makeLocalClusterRootDirectory() {
        DataObjectPath clusterDataObjectPath = new DataObjectPath(this.clusterManager.getLocalClusterManager().getName(), "/");
        return new Directory(clusterDataObjectPath);
    }
    
    private DataObjectMetadata makeRootDataObjectMetadata() {
        long currentTime = DateTimeUtils.getCurrentTime();
        DataObjectPath rootDataObjectPath = new DataObjectPath("", "/");
        DataObjectMetadata rootMetdata = new DataObjectMetadata(rootDataObjectPath, DIRECTORY_METADATA_SIZE, true, currentTime);
        return rootMetdata;
    }
    
    private DataObjectMetadata makeClusterDataObjectMetadata(String clusterName) {
        long currentTime = DateTimeUtils.getCurrentTime();
        DataObjectPath clusterDataObjectPath = new DataObjectPath(clusterName, "/");
        DataObjectMetadata localClusterMetdata = new DataObjectMetadata(clusterDataObjectPath, DIRECTORY_METADATA_SIZE, true, currentTime);
        return localClusterMetdata;
    }
    
    private DataObjectMetadata makeDirectoryDataObjectMetadata(DataObjectPath path) {
        long currentTime = DateTimeUtils.getCurrentTime();
        DataObjectMetadata directoryMetdata = new DataObjectMetadata(path, DIRECTORY_METADATA_SIZE, true, currentTime);
        return directoryMetdata;
    }
    
    private synchronized Directory getRootDirectory() throws IOException {
        Directory directory = new Directory(new DataObjectPath("", "/"));
        // local cluster
        directory.addEntry(this.clusterManager.getLocalClusterManager().getName());
        
        // remote clusters
        Collection<RemoteCluster> remoteCluster = this.clusterManager.getRemoteCluster();
        for(RemoteCluster cluster : remoteCluster) {
            if(cluster.isReachable()) {
                directory.addEntry(cluster.getName());
            }
        }
        
        return directory;
    }
    
    public synchronized Directory getDirectory(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            return getRootDirectory();
        } else if(path.getClusterName().equals(this.clusterManager.getLocalClusterManager().getName())) {
            // local
            Directory directory = (Directory)this.directoryHierarchy.get(path.toString());
            if(path.isClusterRoot() && directory == null) {
                directory = new Directory(path);
            } else if(directory == null) {
                throw new IOException("unable to find a directory for " + path.toString());
            }
            return directory;
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(path.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(this.clusterManager, remoteCluster, this.policyManager);
                return transportClient.getDirectory(path);
            } else {
                throw new IOException("unable to find a directory at a remote cluster for " + path.toString());
            }
        }
    }
    
    private synchronized boolean isLocalDataObject(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            return true;
        } else if(path.getClusterName().equals(this.clusterManager.getLocalClusterManager().getName())) {
            return true;
        }
        return false;
    }
    
    private synchronized void addLocalDirectory(Directory directory) throws IOException {
        if(directory == null) {
            throw new IllegalArgumentException("directory is null");
        }
        
        if(directory.getPath().isRoot()) {
            throw new IllegalArgumentException("root directory is not allowed");
        } else if(isLocalDataObject(directory.getPath())) {
            // local
            // put directory
            this.directoryHierarchy.put(directory.getPath().toString(), directory);
            
            // update parent if necessary
            DataObjectPath parentPath = directory.getPath().getParent();
            if(parentPath != null) {
                Directory parentDir = (Directory)this.directoryHierarchy.get(parentPath.toString());
                if(parentDir == null) {
                    parentDir = new Directory(parentPath);

                    // recurse
                    addLocalDirectory(parentDir);
                }
                
                parentDir.addEntry(directory.getPath());
                this.directoryHierarchy.put(parentPath.toString(), parentDir);
            }
        } else {
            throw new IllegalArgumentException("directory of a remote cluster is not allowed");
        }
    }
    
    public synchronized void addLocalDirectoryEntry(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            throw new IllegalArgumentException("root entry is not allowed");
        } else if(isLocalDataObject(path)) {
            // local
            // put entry to parent directory
            DataObjectPath parentPath = path.getParent();
            if(parentPath != null) {
                Directory parentDir = (Directory)this.directoryHierarchy.get(parentPath.toString());
                if(parentDir == null) {
                    parentDir = new Directory(parentPath);

                    addLocalDirectory(parentDir);
                }
                parentDir.addEntry(path);
                
                this.directoryHierarchy.put(parentPath.toString(), parentDir);
                this.lastUpdateTime = DateTimeUtils.getCurrentTime();
            } else {
                throw new IOException("path " + path.toString() + " has no parent");
            }
        } else {
            throw new IllegalArgumentException("remote cluster is not allowed");
        }
    }
    
    public synchronized void removeLocalDirectoryEntry(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            throw new IllegalArgumentException("root entry is not allowed");
        } else if(path.isClusterRoot()) {
            throw new IllegalArgumentException("cluster root entry is not allowed");
        } else if(isLocalDataObject(path)) {
            // local
            DataObjectPath parentPath = path.getParent();
            if(parentPath != null) {
                Directory parentDir = (Directory)this.directoryHierarchy.get(parentPath.toString());
                if(parentDir == null) {
                    throw new IOException("parent directory is not found");
                }
                
                parentDir.removeEntry(path);
                
                if(parentDir.isEmpty() && !parentPath.isClusterRoot()) {
                    this.directoryHierarchy.remove(parentPath.toString());

                    // recurse
                    removeLocalDirectoryEntry(parentPath);
                } else {
                    this.directoryHierarchy.put(parentPath.toString(), parentDir);
                }
                
                this.lastUpdateTime = DateTimeUtils.getCurrentTime();
            }
        } else {
            throw new IllegalArgumentException("remote cluster is not allowed");
        }
    }
    
    public synchronized Collection<DataObjectPath> listDirectory(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        List<DataObjectPath> entry = new ArrayList<DataObjectPath>();
        
        Directory dir = getDirectory(path);
        if(dir != null) {
            for(String entryName : dir.getEntry()) {
                DataObjectPath entryPath = new DataObjectPath(path, entryName);
                entry.add(entryPath);
            }
        }
        
        return Collections.unmodifiableCollection(entry);
    }
    
    public synchronized DataObjectMetadata getDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            return makeRootDataObjectMetadata();
        } else if(path.isClusterRoot()) {
            return makeClusterDataObjectMetadata(path.getClusterName());
        } else if(isLocalDataObject(path)) {
            // local
            Directory directory = (Directory)this.directoryHierarchy.get(path.toString());
            if(directory != null) {
                // directory
                return makeDirectoryDataObjectMetadata(path);
            } else if(path.isClusterRoot()) {
                // root directory
                return makeDirectoryDataObjectMetadata(path);
            } else {
                // file
                Recipe recipe = this.recipeManager.getRecipe(path);
                return recipe.getMetadata();
            }
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(path.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(this.clusterManager, remoteCluster, this.policyManager);
                return transportClient.getDataObjectMetadata(path);
            } else {
                throw new IOException("unable to find a metadata at a remote cluster for " + path.toString());
            }
        }
    }
    
    public synchronized Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            List<DataObjectMetadata> entry = new ArrayList<DataObjectMetadata>();
            Directory dir = getRootDirectory();
            if(dir != null) {
                for(String entryName : dir.getEntry()) {
                    DataObjectMetadata metadata = makeClusterDataObjectMetadata(entryName);
                    entry.add(metadata);
                }
            }
            return Collections.unmodifiableCollection(entry);
        } else if(path.getClusterName().equals(this.clusterManager.getLocalClusterManager().getName())) {
            // local
            List<DataObjectMetadata> entry = new ArrayList<DataObjectMetadata>();
            Directory dir = getDirectory(path);
            if(dir != null) {
                for(String entryName : dir.getEntry()) {
                    DataObjectPath entryPath = new DataObjectPath(path, entryName);
                    DataObjectMetadata metadata = getDataObjectMetadata(entryPath);
                    entry.add(metadata);
                }
            }
            return Collections.unmodifiableCollection(entry);
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(path.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(this.clusterManager, remoteCluster, this.policyManager);
                return transportClient.listDataObjectMetadata(path);
            } else {
                throw new IOException("unable to find a directory at a remote cluster for " + path.toString());
            }
        }
    }
    
    public synchronized Recipe getRecipe(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(isLocalDataObject(path)) {
            // local
            return this.recipeManager.getRecipe(path);
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(path.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(this.clusterManager, remoteCluster, this.policyManager);
                return transportClient.getRecipe(path);
            } else {
                throw new IOException("unable to find a recipe at a remote cluster for " + path.toString());
            }
        }
    }
    
    public synchronized InputStream getDataChunk(DataObjectPath path, String hash) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        return getDataChunk(path.getClusterName(), hash);
    }
    
    public synchronized InputStream getDataChunk(String clusterName, String hash) throws IOException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(hash == null) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(clusterName.equals(this.clusterManager.getLocalClusterManager().getName())) {
            // local
            Recipe recipe = this.recipeManager.getRecipe(hash);
            if(recipe == null) {
                throw new IOException("unable to find recipe for " + hash);
            }
            
            DataExportEntry dataExport = this.dataExportManager.getDataExport(recipe.getMetadata().getPath().getPath());
            if(dataExport == null) {
                throw new IOException("unable to find dataexport for " + recipe.getMetadata().getPath().getPath());
            }
            
            ASourceFileSystem fileSystem = this.sourceFileSystemManager.getFileSystem();
            
            for(RecipeChunk chunk : recipe.getChunk()) {
                if(chunk.hasHash(hash)) {
                    URI resourcePath = dataExport.getResourcePath();
                    return fileSystem.getInputStream(resourcePath, chunk.getOffset(), chunk.getLength());
                }
            }
            
            throw new IOException("unable to find chunk for " + hash);
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(clusterName);
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(this.clusterManager, remoteCluster, this.policyManager);
                return transportClient.getDataChunk(clusterName, hash);
            } else {
                throw new IOException("unable to find a data chunk at a remote cluster for " + hash);
            }
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
    
    @Override
    public synchronized String toString() {
        return "VolumeManager";
    }
}
