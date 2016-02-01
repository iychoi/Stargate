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
package stargate.server.blockcache;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datastore.ADistributedDataStore;
import stargate.commons.service.ServiceNotStartedException;
import stargate.server.datastore.DataStoreManager;
import stargate.server.temporalstorage.TemporalStorageManager;

/**
 *
 * @author iychoi
 */
public class BlockCacheManager {
    
    private static final Log LOG = LogFactory.getLog(BlockCacheManager.class);
    
    private static final String BLOCKCACHEMANAGER_MAP_ID = "BlockCacheManager_Block_Cache_Metadata";
    private static final String BUCKET_ROOT = "blockcache";
    
    private static BlockCacheManager instance;
    
    private TemporalStorageManager temporalStorageManager;
    private DataStoreManager datastoreManager;
    
    private ADistributedDataStore blockCache;

    public static BlockCacheManager getInstance(TemporalStorageManager temporalStorageManager, DataStoreManager datastoreManager) {
        synchronized (BlockCacheManager.class) {
            if(instance == null) {
                instance = new BlockCacheManager(temporalStorageManager, datastoreManager);
            }
            return instance;
        }
    }
    
    public static BlockCacheManager getInstance() throws ServiceNotStartedException {
        synchronized (BlockCacheManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("BlockCacheManager is not started");
            }
            return instance;
        }
    }
    
    BlockCacheManager(TemporalStorageManager temporalStorageManager, DataStoreManager datastoreManager) {
        if(temporalStorageManager == null) {
            throw new IllegalArgumentException("temporalStorageManager is null");
        }
        
        if(datastoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is null");
        }
        
        this.temporalStorageManager = temporalStorageManager;
        this.datastoreManager = datastoreManager;
        
        this.blockCache = this.datastoreManager.getPersistentDistributedDataStore(BLOCKCACHEMANAGER_MAP_ID, BlockCacheEntry.class);
        
        prepareBucket();
    }
    
    private void prepareBucket() {
        LOG.info("creating a bucket directory for blockcache");
        
        try {
            URI bucket = getBucketPath();
            if(!this.temporalStorageManager.exists(bucket)) {
                this.temporalStorageManager.makeDirs(bucket);
            }
        } catch (URISyntaxException ex) {
            LOG.error("Failed to get a bucket path", ex);
        } catch (IOException ex) {
            LOG.error("Failed to create a bucket directory", ex);
        }
    }
    
    private URI getBucketPath() throws URISyntaxException {
        return new URI(BUCKET_ROOT);
    }
    
    private URI getEntryFilePath(String key) throws URISyntaxException {
        return new URI(BUCKET_ROOT + "/" + key);
    }
    
    public synchronized int getBlockCacheCount() {
        return this.blockCache.size();
    }
    
    public synchronized Collection<BlockCacheMetadata> getBlockCache() throws IOException {
        List<BlockCacheMetadata> entries = new ArrayList<BlockCacheMetadata>();
        Set<String> keySet = this.blockCache.keySet();
        for(String key : keySet) {
            BlockCacheMetadata bcm = (BlockCacheMetadata) this.blockCache.get(key);
            entries.add(bcm);
        }

        return Collections.unmodifiableCollection(entries);
    }
    
    public synchronized BlockCacheMetadata getBlockCacheMetadata(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty or null");
        }
        
        return (BlockCacheMetadata) this.blockCache.get(hash);
    }
    
    public synchronized BlockCacheEntry getBlockCacheEntry(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty or null");
        }
        
        BlockCacheMetadata metadata = (BlockCacheMetadata) this.blockCache.get(hash);
        if(metadata == null) {
            return null;
        }
        
        byte[] blockdata = readBlockCacheData(metadata);
        if(blockdata == null) {
            return null;
        }
        
        return new BlockCacheEntry(metadata, blockdata);
    }
    
    protected synchronized byte[] readBlockCacheData(BlockCacheMetadata metadata) throws IOException {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is empty or null");
        }
        
        try {
            URI entryFilePath = getEntryFilePath(metadata.getHash());
            if(!this.temporalStorageManager.exists(entryFilePath)) {
                return null;
            }
            
            InputStream inputStream = this.temporalStorageManager.getInputStream(entryFilePath);
            byte[] barray = IOUtils.toByteArray(inputStream);
            IOUtils.closeQuietly(inputStream);
            return barray;
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    protected synchronized void writeBlockCacheData(BlockCacheMetadata metadata, byte[] blockdata) throws IOException {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is empty or null");
        }
        
        if(blockdata == null) {
            throw new IllegalArgumentException("blockdata is null");
        }
        
        try {
            URI bucket = getBucketPath();
            if(!this.temporalStorageManager.exists(bucket)) {
                this.temporalStorageManager.makeDirs(bucket);
            }
            
            URI entryFilePath = getEntryFilePath(metadata.getHash());
            OutputStream outputStream = this.temporalStorageManager.getOutputStream(entryFilePath);
            IOUtils.write(blockdata, outputStream);
            IOUtils.closeQuietly(outputStream);
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    protected synchronized void deleteBlockCacheData(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty or null");
        }
        
        try {
            URI entryFilePath = getEntryFilePath(hash);
            if(this.temporalStorageManager.exists(entryFilePath)) {
                this.temporalStorageManager.remove(entryFilePath);
            }
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    public synchronized void addBlockCache(Collection<BlockCacheEntry> entry) throws IOException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        for(BlockCacheEntry e : entry) {
            addBlockCache(e);
        }
    }
    
    public synchronized void addBlockCache(BlockCacheEntry entry) throws IOException {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is empty or null");
        }

        BlockCacheMetadata metadata = entry.getMetadata();
        this.blockCache.put(metadata.getHash(), metadata);
        writeBlockCacheData(metadata, entry.getBlockData());
    }
    
    public synchronized void clearBlockCache() throws IOException {
        Set<String> keys = this.blockCache.keySet();
        for(String key : keys) {
            removeBlockCache((String) key);
        }
    }
    
    public synchronized void removeBlockCache(BlockCacheMetadata metadata) throws IOException {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is empty or null");
        }
        
        removeBlockCache(metadata.getHash());
    }
    
    public synchronized void removeBlockCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty or null");
        }
        
        this.blockCache.remove(hash);
        deleteBlockCacheData(hash);
    }
    
    public synchronized boolean hasBlockCache(String hash) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty or null");
        }
        
        try {
            if(this.blockCache.containsKey(hash)) {
                URI entryFilePath = getEntryFilePath(hash);
                if(this.temporalStorageManager.exists(entryFilePath)) {
                    return true;
                }
            }
            
            return false;
        } catch (URISyntaxException ex) {
            return false;
        } catch (IOException ex) {
            return false;
        }
    }
    
    public synchronized boolean isEmpty() {
        if(this.blockCache == null || this.blockCache.isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public synchronized String toString() {
        return "BlockCacheManager";
    }
}
