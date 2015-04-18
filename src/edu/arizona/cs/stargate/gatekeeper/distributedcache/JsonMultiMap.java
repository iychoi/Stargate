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

package edu.arizona.cs.stargate.gatekeeper.distributedcache;

import com.hazelcast.core.BaseMultiMap;
import com.hazelcast.core.MultiMap;
import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class JsonMultiMap<K extends Object, V extends Object> implements BaseMultiMap<K, V> {

    private static final Log LOG = LogFactory.getLog(JsonMultiMap.class);
    
    private MultiMap<K, String> internalMap;
    private JsonSerializer serializer;
    private Class<? extends V> valueClass;
    
    public JsonMultiMap(MultiMap<K, String> internalMap, Class<? extends V> clazz) {
        this.internalMap = internalMap;
        this.valueClass = clazz;
        this.serializer = new JsonSerializer();
    }
    
    public MultiMap<K, String> getInternalMap() {
        return this.internalMap;
    }
    
    @Override
    public int size() {
        return this.internalMap.size();
    }

    @Override
    public boolean put(K key, V value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalMap.put(key, json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public Collection<V> get(K key) {
        try {
            ArrayList<V> arr = new ArrayList<V>();
            Collection<String> jsons = this.internalMap.get(key);
            Iterator<String> iterator = jsons.iterator();
            while(iterator.hasNext()) {
                String json = iterator.next();
                V value = (V) this.serializer.fromJson(json, this.valueClass);
                arr.add(value);
            }
            return arr;
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalMap.remove(key, json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public Collection<V> remove(Object key) {
        try {
            ArrayList<V> arr = new ArrayList<V>();
            Collection<String> jsons = this.internalMap.remove(key);
            Iterator<String> iterator = jsons.iterator();
            while(iterator.hasNext()) {
                String json = iterator.next();
                V value = (V) this.serializer.fromJson(json, this.valueClass);
                arr.add(value);
            }
            return arr;
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    public void clear() {
        this.internalMap.clear();
    }

    @Override
    public int valueCount(K k) {
        return this.internalMap.valueCount(k);
    }

    @Override
    public Object getId() {
        return this.internalMap.getId();
    }

    @Override
    public String getPartitionKey() {
        return this.internalMap.getPartitionKey();
    }

    @Override
    public String getName() {
        return this.internalMap.getName();
    }

    @Override
    public String getServiceName() {
        return this.internalMap.getServiceName();
    }

    @Override
    public void destroy() {
        this.internalMap.destroy();
    }
}
