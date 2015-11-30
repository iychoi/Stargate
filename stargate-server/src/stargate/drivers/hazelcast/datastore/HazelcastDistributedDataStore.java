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
package stargate.drivers.hazelcast.datastore;

import com.hazelcast.core.IMap;
import java.io.IOException;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.common.JsonSerializer;
import stargate.commons.datastore.ADistributedDataStore;

/**
 *
 * @author iychoi
 */
public class HazelcastDistributedDataStore extends ADistributedDataStore {
    
    private static final Log LOG = LogFactory.getLog(HazelcastDistributedDataStore.class);
    
    private IMap<String, Object> internalMap;
    private Class valclass;
    private boolean useJson;
    private JsonSerializer serializer;
    
    public HazelcastDistributedDataStore(IMap<String, Object> map, Class valclass) {
        if(map == null) {
            throw new IllegalArgumentException("map is null");
        }
        
        if(valclass == null) {
            throw new IllegalArgumentException("valclass is null");
        }
        
        this.internalMap = map;
        this.valclass = valclass;
        
        if(valclass == String.class || valclass == Integer.class || valclass == Long.class) {
            this.useJson = false;
        } else {
            this.useJson = true;
        }
        
        this.serializer = new JsonSerializer();
    }
    
    @Override
    public Class getValueClass() {
        return this.valclass;
    }

    @Override
    public synchronized int size() {
        return this.internalMap.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return this.internalMap.isEmpty();
    }

    @Override
    public synchronized boolean containsKey(String key) {
        if(key == null) {
            throw new IllegalArgumentException("key is null");
        }
        
        return this.internalMap.containsKey(key);
    }

    @Override
    public synchronized Object get(String key) throws IOException {
        if(key == null) {
            throw new IllegalArgumentException("key is null");
        }
        
        if(this.useJson) {
            String json = (String) this.internalMap.get(key);
            LOG.info("Get k=" + key + " v=" + json + "\n from " + this.internalMap.getName());
            LOG.info("Total " + this.internalMap.size() + " in the map");
            if(json == null) {
                return null;
            }
            return this.serializer.fromJson(json, this.valclass);
        } else {
            return this.internalMap.get(key);
        }
    }

    @Override
    public synchronized void put(String key, Object value) throws IOException {
        if(key == null) {
            throw new IllegalArgumentException("key is null");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        if(this.useJson) {
            String json = this.serializer.toJson(value);
            LOG.info("Put k=" + key + " v=" + json + "\n from " + this.internalMap.getName());
            this.internalMap.set(key, json);
            LOG.info("Total " + this.internalMap.size() + " in the map");
        } else {
            this.internalMap.set(key, (String) value);
        }
    }

    @Override
    public synchronized void remove(String key) throws IOException {
        if(key == null) {
            throw new IllegalArgumentException("key is null");
        }
        
        this.internalMap.remove(key);
    }

    @Override
    public synchronized Set<String> keySet() throws IOException {
        return this.internalMap.keySet();
    }

    @Override
    public synchronized void clear() {
        this.internalMap.clear();
    }
}
