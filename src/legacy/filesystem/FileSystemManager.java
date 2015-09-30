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
package legacy.filesystem;
/*
import edu.arizona.cs.stargate.common.utils.DateTimeUtils;
import edu.arizona.cs.stargate.common.utils.PathUtils;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClient;
import edu.arizona.cs.stargate.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.cluster.ClusterManager;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.schedule.DistributedService;
import legacy.hazelcast.JsonIMap;
import edu.arizona.cs.stargate.transport.http.RemoteGateKeeperClientManager;
import edu.arizona.cs.stargate.gatekeeper.recipe.ChunkReaderFactory;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipeManager;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeChunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeChunkOffsetComparator;
import edu.arizona.cs.stargate.gatekeeper.recipe.RemoteRecipe;
import edu.arizona.cs.stargate.gatekeeper.recipe.RemoteRecipeManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
*/
/**
 *
 * @author iychoi
 */
public class FileSystemManager {
    /*
    private static final Log LOG = LogFactory.getLog(FileSystemManager.class);
    
    private static final String FILESYSTEMMANAGER_HIERARCHY_MAP_ID = "FileSystemManager_Hierarchy";
    
    private static FileSystemManager instance;
    
    private DistributedService distributedService;
    private LocalClusterManager localClusterManager;
    private ClusterManager remoteClusterManager;
    private DataExportManager dataExportManager;
    private LocalRecipeManager localRecipeManager;
    private RemoteRecipeManager remoteRecipeManager;
    private RemoteGateKeeperClientManager gatekeeperClientManager;
    
    private JsonIMap<String, VirtualFileStatus> hierarchy;
    
    private boolean updated;
    
    private long lastUpdatedTime;
    
    public static FileSystemManager getInstance(DistributedService distributedService, LocalClusterManager localClusterManager, ClusterManager remoteClusterManager, DataExportManager dataExportManager, LocalRecipeManager localRecipeManager, RemoteRecipeManager remoteRecipeManager, RemoteGateKeeperClientManager gatekeeperClientManager) {
        synchronized (FileSystemManager.class) {
            if(instance == null) {
                instance = new FileSystemManager(distributedService, localClusterManager, remoteClusterManager, dataExportManager, localRecipeManager, remoteRecipeManager, gatekeeperClientManager);
            }
            return instance;
        }
    }
    
    public static FileSystemManager getInstance() throws ServiceNotStartedException {
        synchronized (FileSystemManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("FileSystemManager is not started");
            }
            return instance;
        }
    }
    
    FileSystemManager(DistributedService distributedService, LocalClusterManager localClusterManager, ClusterManager remoteClusterManager, DataExportManager dataExportManager, LocalRecipeManager localRecipeManager, RemoteRecipeManager remoteRecipeManager, RemoteGateKeeperClientManager gatekeeperClientManager) {
        this.distributedService = distributedService;
        this.localClusterManager = localClusterManager;
        this.remoteClusterManager = remoteClusterManager;
        this.dataExportManager = dataExportManager;
        this.localRecipeManager = localRecipeManager;
        this.remoteRecipeManager = remoteRecipeManager;
        this.gatekeeperClientManager = gatekeeperClientManager;
        
        this.hierarchy = new JsonIMap<String, VirtualFileStatus>(this.distributedService.getDistributedMap(FILESYSTEMMANAGER_HIERARCHY_MAP_ID), VirtualFileStatus.class);
        this.lastUpdatedTime = DateTimeUtils.getCurrentTime();
    }
    
    private VirtualFileStatus makeRootDirectoryStatus() {
        long blockSize = this.localRecipeManager.getConfiguration().getChunkSize();
        return new VirtualFileStatus("", "/", true, 4096, blockSize, this.lastUpdatedTime);
    }
    
    private VirtualFileStatus makeParentDirectoryStatus(VirtualFileStatus status) {
        if(status.getClusterName() == null || status.getClusterName().isEmpty()) {
            return null;
        }
        
        String parentPath = PathUtils.getParent(status.getVirtualPath());
        if(parentPath != null) {
            return new VirtualFileStatus(status.getClusterName(), parentPath, true, 4096, 4096, this.lastUpdatedTime);
        }
        return null;
    }
    
    private synchronized void putMapping(VirtualFileStatus status) {
        String path = PathUtils.getPath(status);
        if(path == null || path.isEmpty()) {
            return;
        }
        
        if(!this.hierarchy.containsKey(path)) {
            VirtualFileStatus parentStatus = makeParentDirectoryStatus(status);
            if(parentStatus != null) {
                putMapping(parentStatus);
            }
            
            this.hierarchy.put(path, status);
        }
        
        this.updated = true;
    }
    
    public synchronized void syncHierarchy() throws IOException {
        this.lastUpdatedTime = DateTimeUtils.getCurrentTime();
        this.hierarchy.clear();
        
        VirtualFileStatus rootFileStatus = makeRootDirectoryStatus();
        putMapping(rootFileStatus);
        
        Collection<RemoteRecipe> remoteRecipes = this.remoteRecipeManager.getAllRecipes();
        for(RemoteRecipe recipe : remoteRecipes) {
            putMapping(new VirtualFileStatus(recipe));
        }
        
        Collection<DataExportEntry> dataExports = this.dataExportManager.getDataExport();
        for(DataExportEntry dataExport : dataExports) {
            LocalRecipe recipe = this.localRecipeManager.getRecipe(dataExport.getResourcePath());
            putMapping(new VirtualFileStatus(this.localClusterManager.getName(), dataExport.getVirtualPath(), recipe));
        }
        
        this.updated = true;
    }
    
    private synchronized String getClusterName(String mappedPath) {
        String clusterName = PathUtils.extractClusterNameFromPath(mappedPath);
        if(clusterName.equalsIgnoreCase("localhost") || clusterName.isEmpty()) {
            return this.localClusterManager.getName();
        }
        return clusterName;
    }
    
    private synchronized String getClusterName(URI resourceURI) {
        String clusterName = PathUtils.extractClusterNameFromPath(resourceURI);
        if(clusterName.equalsIgnoreCase("localhost") || clusterName.isEmpty()) {
            return this.localClusterManager.getName();
        }
        return clusterName;
    }
    
    private synchronized String getVirtualPath(String mappedPath) {
        return PathUtils.extractVirtualPath(mappedPath);
    }
    
    private synchronized String getVirtualPath(URI resourceURI) {
        return PathUtils.extractVirtualPath(resourceURI);
    }
    
    private synchronized String makeMappedPath(URI resourceURI) {
        String clusterName = getClusterName(resourceURI);
        String virtualPath = getVirtualPath(resourceURI);
        
        return PathUtils.concatPath(clusterName, virtualPath);
    }
    
    public synchronized VirtualFileStatus getFileStatus(URI resourceURI) {
        String mappedPath = makeMappedPath(resourceURI);
        return getFileStatus(mappedPath);
    }
    
    public synchronized VirtualFileStatus getFileStatus(String mappedPath) {
        return this.hierarchy.get(mappedPath);
    }
    
    public synchronized Collection<VirtualFileStatus> listStatus(URI resourceURI) {
        String mappedPath = makeMappedPath(resourceURI);
        return listStatus(mappedPath);
    }
    
    public synchronized Collection<VirtualFileStatus> listStatus(String mappedPath) {
        ArrayList<VirtualFileStatus> list_status = new ArrayList<VirtualFileStatus>();
        Set<String> keySet = this.hierarchy.keySet();
        for(String path : keySet) {
            String parent = PathUtils.getParent(path);
            if(parent != null && parent.equals(mappedPath)) {
                list_status.add(this.hierarchy.get(path));
            }
        }
        return list_status;
    }
    
    public InputStream getDataChunk(String mappedPath, long offset, int len) throws Exception {
        if(isLocalClusterPath(mappedPath)) {
            DataExportEntry export = this.dataExportManager.getDataExport(getVirtualPath(mappedPath));
            if(export != null) {
                return ChunkReaderFactory.getChunkReader(export.getResourcePath(), offset, len);
            } else {
                throw new Exception("chunk for mpath (" + mappedPath + ") was not found");
            }
        } else {
            String clusterName = getClusterName(mappedPath);
            String vpath = getVirtualPath(mappedPath);

            GateKeeperClient client = this.gatekeeperClientManager.getGateKeeperClient(clusterName);
                
            RemoteRecipe recipe = this.remoteRecipeManager.getRecipe(clusterName, vpath);
            Collection<RecipeChunk> chunks = recipe.getAllChunks();

            ArrayList<RecipeChunk> targetChunks = new ArrayList<RecipeChunk>();
            for(RecipeChunk chunk : chunks) {
                if(chunk.getOffset() + chunk.getLength() <= offset) {
                    // skip
                } else if(chunk.getOffset() >= offset + len) {
                    // skip
                } else {
                    targetChunks.add(chunk);
                }
            }
            
            Collections.sort(targetChunks, new RecipeChunkOffsetComparator());
            
            byte[] buffer = new byte[len];
            int cur_offset = 0;
            int cur_left = len;
            
            for(RecipeChunk chunk : targetChunks) {
                InputStream is = null;
                if(chunk.getHashString() == null || chunk.getHashString().isEmpty()) {
                    // retrieve by vpath
                    is = client.getRestfulClient().getInterClusterDataTransferClient().getDataChunk(vpath, chunk.getOffset(), chunk.getLength());
                } else {
                    is = client.getRestfulClient().getInterClusterDataTransferClient().getDataChunk(chunk.getHashString());
                }
                
                int diff = 0;
                if(chunk.getOffset() < offset) {
                    diff = (int) (offset - chunk.getOffset());
                    is.skip(diff);
                }
                
                IOUtils.readFully(is, buffer, cur_offset, cur_left);
                is.close();
                
                int read = chunk.getLength() - diff;
                
                cur_offset += read;
                cur_left -= read;
            }
            
            return new ByteArrayInputStream(buffer, 0, len);
        }
    }
    
    public synchronized boolean isLocalClusterPath(String mappedPath) {
        String clusterName = getClusterName(mappedPath);
        
        if(clusterName.equals(this.localClusterManager.getName())) {
            return true;
        }
        return false;
    }
    
    public synchronized boolean isLocalClusterPath(URI resourceURI) {
        String clusterName = getClusterName(resourceURI);
        
        if(clusterName.equals(this.localClusterManager.getName())) {
            return true;
        }
        return false;
    }
    
    public synchronized void setUpdated(boolean updated) {
        this.updated = updated;
    }
    
    public synchronized boolean getUpdated() {
        return this.updated;
    }
    
    @Override
    public synchronized String toString() {
        return "FileSystemManager";
    }
    */
}
