/*
 * The MIT License
 *
 * Copyright 2016 iychoi.
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
package stargate.drivers.hazelcast;

import com.hazelcast.core.MapStore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.utils.PathUtils;
import stargate.server.temporalstorage.TemporalStorageManager;

/**
 *
 * @author iychoi
 */
public class PersistentMapStore implements MapStore<String, String> {
    
    private static final Log LOG = LogFactory.getLog(PersistentMapStore.class);
    
    private static final String BUCKET_ROOT = "bucket";
    
    private TemporalStorageManager temporalStorageManager;
    private String mapName;
    private String refinedMapName;
    private Properties properties;
    
    public PersistentMapStore(TemporalStorageManager temporalStorageManager, String name, Properties props) {
        if(temporalStorageManager == null) {
            throw new IllegalArgumentException("temporalStorageManager is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.temporalStorageManager = temporalStorageManager;
        this.mapName = name;
        if(name.startsWith(HazelcastCoreDriver.HAZELCAST_PERSISTENT_MAP_PREFIX)) {
            this.refinedMapName = name.substring(HazelcastCoreDriver.HAZELCAST_PERSISTENT_MAP_PREFIX.length());
        } else {
            this.refinedMapName = name;
        }
        this.properties = props;
        
        prepareBucket(name);
    }
    
    private void prepareBucket(String name) {
        LOG.info("creating a bucket directory for Map " + name);
        
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
        return new URI(BUCKET_ROOT + "/" + this.refinedMapName);
    }
    
    private URI getEntryFilePath(String key) throws URISyntaxException {
        return new URI(BUCKET_ROOT + "/" + this.refinedMapName + "/" + PathUtils.encodeHadoopFilename(key));
    }
    
    @Override
    public synchronized void store(String k, String v) {
        try {
            URI bucketPath = getBucketPath();
            URI entryPath = getEntryFilePath(k);
            OutputStream outputStream = null;
            try {
                if(!this.temporalStorageManager.exists(bucketPath)) {
                    this.temporalStorageManager.makeDirs(bucketPath);
                }
                
                outputStream = this.temporalStorageManager.getOutputStream(entryPath);
                IOUtils.write(v, outputStream);
            } catch (Exception ex) {
                LOG.error("Failed to store an entry - " + k, ex);
            } finally {
                if(outputStream != null) {
                    IOUtils.closeQuietly(outputStream);
                }
            }
        } catch (URISyntaxException ex) {
            LOG.error("Failed to store an entry - " + k, ex);
        }
    }

    @Override
    public synchronized void storeAll(Map<String, String> map) {
        for(Map.Entry<String, String> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public synchronized void delete(String k) {
        try {
            URI entryPath = getEntryFilePath(k);
            if(this.temporalStorageManager.exists(entryPath)) {
                this.temporalStorageManager.remove(entryPath);
            }
        } catch (URISyntaxException ex) {
            LOG.error("Failed to delete an entry - " + k, ex);
        } catch (IOException ex) {
            LOG.error("Failed to delete an entry - " + k, ex);
        }
    }

    @Override
    public synchronized void deleteAll(Collection<String> clctn) {
        for(String key : clctn) {
            delete(key);
        }
    }

    @Override
    public synchronized String load(String k) {
        try {
            URI entryPath = getEntryFilePath(k);
            if(this.temporalStorageManager.exists(entryPath)) {
                InputStream inputStream = null;
                String json = null;
                try {
                    inputStream = this.temporalStorageManager.getInputStream(entryPath);
                    json = IOUtils.toString(inputStream);
                } catch (Exception ex) {
                    LOG.error("Failed to load an entry - " + k, ex);
                } finally {
                    if(inputStream != null) {
                        IOUtils.closeQuietly(inputStream);
                    }
                }
                
                return json;
            }
        } catch (URISyntaxException ex) {
            LOG.error("Failed to delete an entry - " + k, ex);
        } catch (IOException ex) {
            LOG.error("Failed to delete an entry - " + k, ex);
        }
        
        return null;
    }

    @Override
    public synchronized Map<String, String> loadAll(Collection<String> clctn) {
        Map<String, String> map = new HashMap<String, String>();
        for(String key : clctn) {
            String val = load(key);
            map.put(key, val);
        }
        return map;
    }

    @Override
    public synchronized Iterable<String> loadAllKeys() {
        try {
            URI bucketPath = this.getBucketPath();
            List<String> arr = new ArrayList<String>();
            if(this.temporalStorageManager.exists(bucketPath)) {
                Collection<URI> entries = this.temporalStorageManager.listDirectory(bucketPath);
                if(entries != null && !entries.isEmpty()) {
                    for(URI entry : entries) {
                        URI relativePath = bucketPath.relativize(entry);
                        String path = relativePath.getPath();
                        arr.add(PathUtils.decodeHadoopFilename(path));
                    }
                }
            }
            return arr;
        } catch (URISyntaxException ex) {
            LOG.error("Failed to load all entries", ex);
        } catch (IOException ex) {
            LOG.error("Failed to load all entries", ex);
        }
        
        return null;
    }
}
