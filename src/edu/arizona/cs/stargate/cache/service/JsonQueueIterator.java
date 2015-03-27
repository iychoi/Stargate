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
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class JsonQueueIterator<V> implements Iterator<V> {

    private static final Log LOG = LogFactory.getLog(JsonQueueIterator.class);
    
    private Iterator<String> internalIterator;
    private JsonSerializer serializer;
    private Class<? extends V> valueClass;
    
    public JsonQueueIterator(Iterator<String> internalIterator, Class<? extends V> clazz) {
        this.internalIterator = internalIterator;
        this.valueClass = clazz;
        this.serializer = new JsonSerializer();
    }
    
    public Iterator<String> getInternalIterator() {
        return this.internalIterator;
    }
    
    @Override
    public boolean hasNext() {
        return this.internalIterator.hasNext();
    }

    @Override
    public V next() {
        String json = this.internalIterator.next();
        if(json == null) {
            return null;
        } else {
            try {
                return (V) this.serializer.fromJson(json, this.valueClass);
            } catch (IOException ex) {
                LOG.error(ex);
                return null;
            }
        }
    }

    @Override
    public void remove() {
        this.internalIterator.remove();
    }
    
}
