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

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class JsonQueue<V extends Object> implements Queue<V> {

    private static final Log LOG = LogFactory.getLog(JsonQueue.class);
    
    private Queue<String> internalQueue;
    private JsonSerializer serializer;
    private Class<? extends V> valueClass;
    
    public JsonQueue(Queue<String> internalQueue, Class<? extends V> clazz) {
        this.internalQueue = internalQueue;
        this.valueClass = clazz;
        this.serializer = new JsonSerializer();
    }
    
    public Queue<String> getInternalQueue() {
        return this.internalQueue;
    }
    
    @Override
    public boolean add(V value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalQueue.add(json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public boolean offer(V value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalQueue.offer(json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public V remove() {
        try {
            String json = this.internalQueue.remove();
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public V poll() {
        try {
            String json = this.internalQueue.poll();
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public V element() {
        try {
            String json = this.internalQueue.element();
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public V peek() {
        try {
            String json = this.internalQueue.peek();
            return (V) this.serializer.fromJson(json, this.valueClass);
        } catch (IOException ex) {
            LOG.error(ex);
            return null;
        }
    }

    @Override
    public int size() {
        return this.internalQueue.size();
    }

    @Override
    public boolean isEmpty() {
        return this.internalQueue.isEmpty();
    }

    @Override
    public boolean contains(Object value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalQueue.contains(json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public Iterator<V> iterator() {
        return new JsonQueueIterator<V>(this.internalQueue.iterator(), this.valueClass);
    }

    @Override
    public Object[] toArray() {
        ArrayList<V> arr = new ArrayList<V>();
        Iterator<String> iterator = this.internalQueue.iterator();
        while(iterator.hasNext()) {
            try {
                String json = iterator.next();
                V obj = (V) this.serializer.fromJson(json, this.valueClass);
                arr.add(obj);
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        return arr.toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        ArrayList<T> arr = new ArrayList<T>();
        Iterator<String> iterator = this.internalQueue.iterator();
        while(iterator.hasNext()) {
            try {
                String json = iterator.next();
                T obj = (T) this.serializer.fromJson(json, this.valueClass);
                arr.add(obj);
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        return arr.toArray(ts);
    }

    @Override
    public boolean remove(Object value) {
        try {
            String json = this.serializer.toJson(value);
            return this.internalQueue.remove(json);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public boolean containsAll(Collection<?> clctn) {
        Iterator<?> iterator = clctn.iterator();
        while(iterator.hasNext()) {
            try {
                Object next = iterator.next();
                String json = this.serializer.toJson(next);
                if(!this.internalQueue.contains(json)) {
                    return false;
                }
            } catch (IOException ex) {
                LOG.error(ex);
                return false;
            }
        }
        
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends V> clctn) {
        Iterator<? extends V> iterator = clctn.iterator();
        while(iterator.hasNext()) {
            try {
                V next = iterator.next();
                String json = this.serializer.toJson(next);
                if(!this.internalQueue.add(json)) {
                    return false;
                }
            } catch (IOException ex) {
                LOG.error(ex);
                return false;
            }
        }
        
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> clctn) {
        Iterator<?> iterator = clctn.iterator();
        while(iterator.hasNext()) {
            try {
                Object next = iterator.next();
                String json = this.serializer.toJson(next);
                if(!this.internalQueue.remove(json)) {
                    return false;
                }
            } catch (IOException ex) {
                LOG.error(ex);
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        ArrayList<String> arr = new ArrayList<String>();
        for(Object elem : clctn) {
            try {
                String json = this.serializer.toJson(elem);
                arr.add(json);
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        return this.internalQueue.retainAll(arr);
    }

    @Override
    public void clear() {
        this.internalQueue.clear();
    }
    
}
