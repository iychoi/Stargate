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

package edu.arizona.cs.stargate.gatekeeper.distributed;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.ReplicatedMap;
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
public class JsonReplicatedMap<K extends Object, V extends Object> implements Map<K, V> {

    private static final Log LOG = LogFactory.getLog(JsonReplicatedMap.class);
    
    private ReplicatedMap<K, String> internalMap;
    private JsonSerializer serializer;
    private Class<? extends V> valueClass;
    
    public JsonReplicatedMap(ReplicatedMap<K, String> internalMap, Class<? extends V> clazz) {
        this.internalMap = internalMap;
        this.valueClass = clazz;
        this.serializer = new JsonSerializer();
    }
    
    public ReplicatedMap<K, String> getInternalMap() {
        return this.internalMap;
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
    public synchronized boolean containsKey(Object key) {
        return this.internalMap.containsKey(key);
    }

    @Override
    public synchronized boolean containsValue(Object value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalMap.containsValue(json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public synchronized V get(Object key) {
        try {
            String json = this.internalMap.get(key);
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public synchronized V put(K key, V value) {
        try {
            String json = this.serializer.toJson(value);
            this.internalMap.put(key, json);
            return value;
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    @Override
    public synchronized V remove(Object key) {
        try {
            String json = this.internalMap.remove(key);
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> map) {
        Set<? extends K> keySet = map.keySet();
        Iterator<? extends K> iterator = keySet.iterator();
        while(iterator.hasNext()) {
            K nextKey = iterator.next();
            put(nextKey, map.get(nextKey));
        }
    }
    
    public synchronized void putAll(JsonReplicatedMap<K, V> map) {
        this.internalMap.putAll(map.internalMap);
    }

    @Override
    public synchronized void clear() {
        this.internalMap.clear();
    }

    @Override
    public synchronized Set<K> keySet() {
        return this.internalMap.keySet();
    }
    
    @Override
    public synchronized Collection<V> values() {
        ArrayList<V> values = new ArrayList<V>();
        Collection<String> jsonValues = this.internalMap.values();
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
    public synchronized Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> set = new HashSet<Entry<K, V>>();
        Set<Entry<K, String>> entrySet = this.internalMap.entrySet();
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
    
    public synchronized V popAnyEntry() {
        if(this.internalMap.size() > 0) {
            Set<K> keySet = this.internalMap.keySet();
            Iterator<K> iterator = keySet.iterator();
            while(iterator.hasNext()) {
                try {
                    K key = iterator.next();
                    String json = this.internalMap.remove(key);
                    return (V) this.serializer.fromJson(json, this.valueClass);
                } catch (IOException ex) {
                    LOG.error(ex);
                }
            }
        }
        return null;
    }
    
    public synchronized V peekEntry() {
        if(this.internalMap.size() > 0) {
            Set<K> keySet = this.internalMap.keySet();
            Iterator<K> iterator = keySet.iterator();
            while(iterator.hasNext()) {
                try {
                    K key = iterator.next();
                    String json = this.internalMap.get(key);
                    return (V) this.serializer.fromJson(json, this.valueClass);
                } catch (IOException ex) {
                    LOG.error(ex);
                }
            }
        }
        return null;
    }
    
    public synchronized void addEntryListener(final EntryListener<K, V> el) {
        this.internalMap.addEntryListener(new EntryListener<K, String>(){

            private EntryEvent<K, V> convEntry(EntryEvent<K, String> ee) {
                String oldValueJson = ee.getOldValue();
                V oldValue = null;
                if(oldValueJson != null) {
                    try {
                        oldValue = (V) serializer.fromJson(oldValueJson, valueClass);
                    } catch (IOException ex) {
                    }
                }
                String valueJson = ee.getValue();
                V value = null;
                if(valueJson != null) {
                    try {
                        value = (V) serializer.fromJson(valueJson, valueClass);
                    } catch (IOException ex) {
                    }
                }
                
                return new EntryEvent<K, V>(ee.getSource(), ee.getMember(), ee.getEventType().getType(), ee.getKey(), oldValue, value);
            }
            
            @Override
            public void entryAdded(EntryEvent<K, String> ee) {
                el.entryAdded(convEntry(ee));
            }

            @Override
            public void entryRemoved(EntryEvent<K, String> ee) {
                el.entryRemoved(convEntry(ee));
            }

            @Override
            public void entryUpdated(EntryEvent<K, String> ee) {
                el.entryUpdated(convEntry(ee));
            }

            @Override
            public void entryEvicted(EntryEvent<K, String> ee) {
                el.entryEvicted(convEntry(ee));
            }

            @Override
            public void mapEvicted(MapEvent me) {
                el.mapEvicted(me);
            }

            @Override
            public void mapCleared(MapEvent me) {
                el.mapEvicted(me);
            }
        });
    }
}
