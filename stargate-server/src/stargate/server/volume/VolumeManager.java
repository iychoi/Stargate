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

package stargate.server.volume;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.datastore.ADistributedDataStore;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.sourcefs.ASourceFileSystem;
import stargate.commons.transport.ATransportClient;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.volume.Directory;
import stargate.server.cluster.ClusterManager;
import stargate.server.dataexport.DataExportManager;
import stargate.server.datastore.DataStoreManager;
import stargate.server.policy.PolicyManager;
import stargate.server.recipe.RecipeManager;
import stargate.server.sourcefs.SourceFileSystemManager;
import stargate.server.transport.TransportManager;

/**
 *
 * @author iychoi
 */
public class VolumeManager {
    
    private static final Log LOG = LogFactory.getLog(VolumeManager.class);
    
    public static final String VOLUMEMANAGER_DIRECTORY_HIERARCHY_MAP_ID = "VolumeManager_Directory_Hierarchy";
    
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
    
    private DataExportChangedEventHandler dataExportChangedHandler;
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
    
    public VolumeManager(PolicyManager policyManager, DataStoreManager dataStoreManager, SourceFileSystemManager sourceFileSystemManager, ClusterManager clusterManager, DataExportManager dataExportManager, RecipeManager recipeManager, TransportManager transportManager) throws IOException {
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

        this.directoryHierarchy = this.dataStoreManager.getDistributedDataStore(VOLUMEMANAGER_DIRECTORY_HIERARCHY_MAP_ID, Directory.class);
        
        // make local cluster root
        Directory localClusterRootDir = makeLocalClusterRootDirectory();
        this.directoryHierarchy.putIfAbsent(localClusterRootDir.getPath().toString(), localClusterRootDir);
        
        this.dataExportChangedHandler = new DataExportChangedEventHandler(this.clusterManager, this);
        this.dataExportManager.addEventHandler(this.dataExportChangedHandler);
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
        DataObjectMetadata clusterMetdata = new DataObjectMetadata(clusterDataObjectPath, DIRECTORY_METADATA_SIZE, true, currentTime);
        return clusterMetdata;
    }
    
    private DataObjectMetadata makeDirectoryDataObjectMetadata(DataObjectPath path) {
        long currentTime = DateTimeUtils.getCurrentTime();
        DataObjectMetadata directoryMetdata = new DataObjectMetadata(path, DIRECTORY_METADATA_SIZE, true, currentTime);
        return directoryMetdata;
    }
    
    private DataObjectPath makeAbsolutePath(DataObjectPath path) {
        if(path.isRoot()) {
            return path;
        }
        
        if(path.getClusterName().equalsIgnoreCase("localhost")) {
            return new DataObjectPath(this.clusterManager.getLocalClusterManager().getName(), path.getPath());
        }
        return path;
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
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("Get a directory - " + absPath.toString());
        
        if(absPath.isRoot()) {
            return getRootDirectory();
        } else if(isLocalDataObject(path)) {
            // local
            Directory directory = (Directory)this.directoryHierarchy.get(absPath.toString());
            if(absPath.isClusterRoot() && directory == null) {
                directory = new Directory(absPath);
            } else if(directory == null) {
                throw new IOException("unable to find a directory for " + absPath.toString());
            }
            return directory;
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(absPath.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(remoteCluster);
                return transportClient.getDirectory(absPath);
            } else {
                throw new IOException("unable to find a directory at a remote cluster for " + absPath.toString());
            }
        }
    }
    
    private boolean isLocalCluster(String clusterName) {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(clusterName.equalsIgnoreCase("localhost")) {
            return true;
        } else {
            return clusterName.equals(this.clusterManager.getLocalClusterManager().getName());
        }
    }
    
    private synchronized boolean isLocalDataObject(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(path.isRoot()) {
            return true;
        } else if(isLocalCluster(path.getClusterName())) {
            return true;
        }
        return false;
    }
    
    private synchronized void addLocalDirectory(Directory directory) throws IOException {
        if(directory == null) {
            throw new IllegalArgumentException("directory is null");
        }
        
        LOG.info("Adding a local directory - " + directory.getPath().toString());
        
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
                
                LOG.info("Adding a directory - " + directory.getPath().toString() + " to " + parentDir.getPath().toString());
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
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("Adding a local directory entry - " + absPath.toString());
        
        if(absPath.isRoot()) {
            throw new IllegalArgumentException("root entry is not allowed");
        } else if(isLocalDataObject(absPath)) {
            // local
            // put entry to parent directory
            DataObjectPath parentPath = absPath.getParent();
            if(parentPath != null) {
                Directory parentDir = (Directory)this.directoryHierarchy.get(parentPath.toString());
                if(parentDir == null) {
                    parentDir = new Directory(parentPath);

                    addLocalDirectory(parentDir);
                }
                parentDir.addEntry(absPath);
                
                this.directoryHierarchy.put(parentPath.toString(), parentDir);
                this.lastUpdateTime = DateTimeUtils.getCurrentTime();
            } else {
                throw new IOException("path " + absPath.toString() + " has no parent");
            }
        } else {
            throw new IllegalArgumentException("remote cluster is not allowed");
        }
    }
    
    public synchronized void removeLocalDirectoryEntry(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        if(absPath.isRoot()) {
            throw new IllegalArgumentException("root entry is not allowed");
        } else if(absPath.isClusterRoot()) {
            throw new IllegalArgumentException("cluster root entry is not allowed");
        } else if(isLocalDataObject(absPath)) {
            // local
            DataObjectPath parentPath = absPath.getParent();
            if(parentPath != null) {
                Directory parentDir = (Directory)this.directoryHierarchy.get(parentPath.toString());
                if(parentDir == null) {
                    throw new IOException("parent directory is not found");
                }
                
                parentDir.removeEntry(absPath);
                
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
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("List a local directory entry - " + absPath.toString());
        
        List<DataObjectPath> entry = new ArrayList<DataObjectPath>();
        
        Directory dir = getDirectory(absPath);
        if(dir != null) {
            for(String entryName : dir.getEntry()) {
                DataObjectPath entryPath = new DataObjectPath(absPath, entryName);
                entry.add(entryPath);
            }
        }
        
        return Collections.unmodifiableCollection(entry);
    }
    
    public synchronized URI getLocalResourcePath(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("Get a local resource path - " + absPath.toString());
        
        if(absPath.isRoot()) {
            throw new IOException("root directory is virtual");
        } else if(absPath.isClusterRoot()) {
            throw new IOException("cluster root directory is virtual");
        } else if(isLocalDataObject(absPath)) {
            // local
            Directory directory = (Directory)this.directoryHierarchy.get(absPath.toString());
            if(directory == null) {
                // file
                DataExportEntry dataExport = this.dataExportManager.getDataExport(absPath.getPath());
                if(dataExport == null) {
                    throw new IOException("unable to find dataexport for " + absPath.getPath());
                }

                return dataExport.getResourcePath();
            }
            throw new IOException("given path is a directory - " + absPath.toString());
        } else {
            // remote
            throw new IOException("given path is a remote resource - " + absPath.toString());
        }
    }
    
    public synchronized DataObjectMetadata getDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("Get a data object metadata - " + absPath.toString());
        
        if(absPath.isRoot()) {
            return makeRootDataObjectMetadata();
        } else if(absPath.isClusterRoot()) {
            return makeClusterDataObjectMetadata(absPath.getClusterName());
        } else if(isLocalDataObject(absPath)) {
            // local
            Directory directory = (Directory)this.directoryHierarchy.get(absPath.toString());
            if(directory != null) {
                // directory
                return makeDirectoryDataObjectMetadata(absPath);
            } else {
                // file
                Recipe recipe = this.recipeManager.getRecipe(absPath);
                if(recipe == null) {
                    // not exist
                    throw new FileNotFoundException("file not found - " + absPath.toString());
                }
                return recipe.getMetadata();
            }
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(absPath.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(remoteCluster);
                return transportClient.getDataObjectMetadata(absPath);
            } else {
                throw new IOException("unable to find a metadata at a remote cluster for " + absPath.toString());
            }
        }
    }
    
    public synchronized Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("List data object metadata - " + absPath.toString());
        
        if(absPath.isRoot()) {
            List<DataObjectMetadata> entry = new ArrayList<DataObjectMetadata>();
            Directory dir = getRootDirectory();
            if(dir != null) {
                for(String entryName : dir.getEntry()) {
                    DataObjectMetadata metadata = makeClusterDataObjectMetadata(entryName);
                    entry.add(metadata);
                }
            }
            return Collections.unmodifiableCollection(entry);
        } else if(this.isLocalDataObject(absPath)) {
            // local
            List<DataObjectMetadata> entry = new ArrayList<DataObjectMetadata>();
            Directory dir = getDirectory(absPath);
            if(dir != null) {
                for(String entryName : dir.getEntry()) {
                    DataObjectPath entryPath = new DataObjectPath(absPath, entryName);
                    DataObjectMetadata metadata = getDataObjectMetadata(entryPath);
                    entry.add(metadata);
                }
            }
            return Collections.unmodifiableCollection(entry);
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(absPath.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(remoteCluster);
                return transportClient.listDataObjectMetadata(absPath);
            } else {
                throw new IOException("unable to find a directory at a remote cluster for " + absPath.toString());
            }
        }
    }
    
    public synchronized Recipe getRecipe(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        LOG.info("Get a recipe - " + absPath.toString());
        
        if(isLocalDataObject(absPath)) {
            // local
            return this.recipeManager.getRecipe(absPath);
        } else {
            // remote
            RemoteCluster remoteCluster = this.clusterManager.getRemoteCluster(absPath.getClusterName());
            if(remoteCluster != null) {
                ATransportClient transportClient = this.transportManager.getTransportClient(remoteCluster);
                return transportClient.getRecipe(absPath);
            } else {
                throw new IOException("unable to find a recipe at a remote cluster for " + absPath.toString());
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
        
        DataObjectPath absPath = makeAbsolutePath(path);
        
        return getDataChunk(absPath.getClusterName(), hash);
    }
    
    public synchronized InputStream getDataChunk(String clusterName, String hash) throws IOException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(hash == null) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(isLocalCluster(clusterName)) {
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
                ATransportClient transportClient = this.transportManager.getTransportClient(remoteCluster);
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
