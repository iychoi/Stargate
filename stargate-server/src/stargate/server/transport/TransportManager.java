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
package stargate.server.transport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.transport.ATransportClient;
import stargate.commons.transport.ATransportDriver;
import stargate.commons.volume.Directory;
import stargate.server.blockcache.BlockCacheEntry;
import stargate.server.blockcache.BlockCacheManager;
import stargate.server.dataexport.DataExportManager;
import stargate.server.datastore.DataStoreManager;
import stargate.server.recipe.RecipeGeneratorManager;
import stargate.server.recipe.RecipeManager;
import stargate.server.sourcefs.SourceFileSystemManager;
import stargate.server.volume.CachedInputStreamHandler;
import stargate.server.volume.IInterceptableInputStreamHandler;
import stargate.server.volume.InterceptableInputStream;

/**
 *
 * @author iychoi
 */
public class TransportManager {

    private static final Log LOG = LogFactory.getLog(TransportManager.class);
    
    private static TransportManager instance;

    private DataStoreManager dataStoreManager;
    private RecipeManager recipeManager;
    private RecipeGeneratorManager recipeGeneratorManager;
    private SourceFileSystemManager sourceFileSystemManager;
    private DataExportManager dataExportManager;
    private BlockCacheManager blockCacheManager;
    
    private ATransportDriver driver;
    private Thread preloadTaskThread;
    
    public static TransportManager getInstance(ATransportDriver driver, DataStoreManager dataStoreManager, RecipeManager recipeManager, RecipeGeneratorManager recipeGeneratorManager, SourceFileSystemManager sourceFileSystemManager, DataExportManager dataExportManager, BlockCacheManager blockCacheManager) {
        synchronized (TransportManager.class) {
            if(instance == null) {
                instance = new TransportManager(driver, dataStoreManager, recipeManager, recipeGeneratorManager, sourceFileSystemManager, dataExportManager, blockCacheManager);
            }
            return instance;
        }
    }
    
    public static TransportManager getInstance() throws ServiceNotStartedException {
        synchronized (TransportManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("TransportManager is not started");
            }
            return instance;
        }
    }
    
    TransportManager(ATransportDriver driver, DataStoreManager dataStoreManager, RecipeManager recipeManager, RecipeGeneratorManager recipeGeneratorManager, SourceFileSystemManager sourceFileSystemManager, DataExportManager dataExportManager, BlockCacheManager blockCacheManager) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(dataStoreManager == null) {
            throw new IllegalArgumentException("dataStoreManager is null");
        }
        
        if(recipeManager == null) {
            throw new IllegalArgumentException("recipeManager is null");
        }
        
        if(recipeGeneratorManager == null) {
            throw new IllegalArgumentException("recipeGeneratorManager is null");
        }
        
        if(sourceFileSystemManager == null) {
            throw new IllegalArgumentException("sourceFileSystemManager is null");
        }
        
        if(dataExportManager == null) {
            throw new IllegalArgumentException("dataExportManager is null");
        }
    
        if(blockCacheManager == null) {
            throw new IllegalArgumentException("blockCacheManager is null");
        }
        
        this.driver = driver;
        this.dataStoreManager = dataStoreManager;
        this.recipeManager = recipeManager;
        this.recipeGeneratorManager = recipeGeneratorManager;
        this.sourceFileSystemManager = sourceFileSystemManager;
        this.dataExportManager = dataExportManager;
        this.blockCacheManager = blockCacheManager;
    }
    
    public ATransportDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        this.driver.startDriver();
    }

    public synchronized void stop() throws IOException {
        this.driver.stopDriver();
    }
    
    protected synchronized ATransportClient getTransportClient(RemoteCluster remoteCluster) throws IOException {
        return this.driver.getTransportClient(remoteCluster);
    }
    
    public synchronized RemoteCluster getClusterInfo(RemoteCluster remoteCluster) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            return transportClient.getCluster();
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }
    
    public synchronized Directory getDirectory(RemoteCluster remoteCluster, DataObjectPath path) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            return transportClient.getDirectory(path);
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }
    
    public synchronized DataObjectMetadata getDataObjectMetadata(RemoteCluster remoteCluster, DataObjectPath path) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            return transportClient.getDataObjectMetadata(path);
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }
    
    public synchronized Collection<DataObjectMetadata> listDataObjectMetadata(RemoteCluster remoteCluster, DataObjectPath path) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            return transportClient.listDataObjectMetadata(path);
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }

    public synchronized Recipe getRecipe(RemoteCluster remoteCluster, DataObjectPath path) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            return transportClient.getRecipe(path);
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }
    
    public synchronized InputStream getDataChunk(RemoteCluster remoteCluster, String hash) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        if(hash == null) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        // step 1. check out local recipe to check if a block exists locally
        try {
            Recipe recipe = this.recipeManager.getRecipe(hash);
            if(recipe != null) {
                DataExportEntry dataExport = this.dataExportManager.getDataExport(recipe.getMetadata().getPath().getPath());
                if(dataExport != null) {
                    for(RecipeChunk chunk : recipe.getChunk()) {
                        if(chunk.hasHash(hash)) {
                            URI resourcePath = dataExport.getResourcePath();
                            return this.sourceFileSystemManager.getInputStream(resourcePath, chunk.getOffset(), chunk.getLength());
                        }
                    }
                }
            }
        } catch (IOException ex) {
        }

        // step 2. check block-cache
        try {
            BlockCacheEntry blockCache = this.blockCacheManager.getBlockCacheEntry(hash);
            if(blockCache != null) {
                byte[] blockData = blockCache.getBlockData();
                if(blockData != null) {
                    ByteArrayInputStream bais = new ByteArrayInputStream(blockData);
                    return bais;
                }
            }
        } catch (IOException ex) {
        }

        // step 3. go remote
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            InputStream dataChunkInputStream = transportClient.getDataChunk(remoteCluster.getName(), hash);
            IInterceptableInputStreamHandler handler = new CachedInputStreamHandler(this.blockCacheManager, this.recipeGeneratorManager, hash);

            InterceptableInputStream iterceptableInputStream = new InterceptableInputStream(dataChunkInputStream, handler);

            return iterceptableInputStream;
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }
    
    public synchronized void scheduleTransferAndFillCache(RemoteCluster remoteCluster, String hash) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        if(hash == null) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        // step 1. check out local recipe to check if a block exists locally
        try {
            Recipe recipe = this.recipeManager.getRecipe(hash);
            if(recipe != null) {
                return;
            }
        } catch (IOException ex) {
        }

        // step 2. check block-cache
        try {
            BlockCacheEntry blockCache = this.blockCacheManager.getBlockCacheEntry(hash);
            if(blockCache != null) {
                return;
            }
        } catch (IOException ex) {
        }

        // step 3. go remote
        ATransportClient transportClient = getTransportClient(remoteCluster);
        if(transportClient != null) {
            InputStream dataChunkInputStream = transportClient.getDataChunk(remoteCluster.getName(), hash);
            IInterceptableInputStreamHandler handler = new CachedInputStreamHandler(this.blockCacheManager, this.recipeGeneratorManager, hash);

            InterceptableInputStream iterceptableInputStream = new InterceptableInputStream(dataChunkInputStream, handler);

            return iterceptableInputStream;
        } else {
            throw new IOException("unable to contact a remote cluster - " + remoteCluster.getName());
        }
    }
    
    public synchronized void wakeupPreloadTask() {
        if(this.preloadTaskThread == null) {
            this.preloadTaskThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    // check preload queue and cache
                }
            });
        }
    }
    
    @Override
    public synchronized String toString() {
        return "TransportManager";
    }
}
