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

package edu.arizona.cs.stargate.cache.service;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class JsonMap<K extends Object, V extends Object> implements Map<K, V> {

    private static final Log LOG = LogFactory.getLog(JsonMap.class);
    
    private Map<K, String> intermalMap;
    private JsonSerializer serializer;
    private Class<? extends V> valueClass;
    
    public JsonMap(Map<K, String> internalMap, Class<? extends V> clazz) {
        this.intermalMap = internalMap;
        this.valueClass = clazz;
        this.serializer = new JsonSerializer();
    }
    
    public Map<K, String> getInternalMap() {
        return this.intermalMap;
    }
    
    @Override
    public int size() {
        return this.intermalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return this.intermalMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.intermalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        try {
            String json = this.serializer.toJson(value);
            return this.intermalMap.containsValue(json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public V get(Object key) {
        try {
            String json = this.intermalMap.get(key);
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            String json = this.serializer.toJson(value);
            this.intermalMap.put(key, json);
            return value;
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    @Override
    public V remove(Object key) {
        try {
            String json = this.intermalMap.remove(key);
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Set<? extends K> keySet = map.keySet();
        Iterator<? extends K> iterator = keySet.iterator();
        while(iterator.hasNext()) {
            K nextKey = iterator.next();
            put(nextKey, map.get(nextKey));
        }
    }
    
    public void putAll(JsonMap<K, V> map) {
        this.intermalMap.putAll(map.intermalMap);
    }

    @Override
    public void clear() {
        this.intermalMap.clear();
    }

    @Override
    public Set<K> keySet() {
        return this.intermalMap.keySet();
    }
    
    @Override
    public Collection<V> values() {
        ArrayList<V> values = new ArrayList<V>();
        Collection<String> jsonValues = this.intermalMap.values();
        for(String json : jsonValues) {
            try {
                V value = (V) this.serializer.fromJson(json, this.valueClass);
                values.add(value);
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        return Collections.unmodifiableCollection(values);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> set = new HashSet<Entry<K, V>>();
        Set<Entry<K, String>> entrySet = this.intermalMap.entrySet();
        for(Entry<K, String> entry : entrySet) {
            try {
                V value = (V) this.serializer.fromJson(entry.getValue(), this.valueClass);
                set.add(new AbstractMap.SimpleImmutableEntry<K, V>(entry.getKey(), value));
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        return set;
    }
}
