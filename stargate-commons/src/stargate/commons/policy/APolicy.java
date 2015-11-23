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
package stargate.commons.policy;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import stargate.commons.common.JsonSerializer;
import stargate.commons.datastore.ADataStore;

/**
 *
 * @author iychoi
 */
public abstract class APolicy {
    
    @JsonIgnore
    public long readLongFrom(ADataStore datastore, String key, long default_value) {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        try {
            Object p1 = datastore.get(key);
            if(p1 != null) {
                if(p1 instanceof Long) {
                    return (Long)p1;
                } else if(p1 instanceof Integer) {
                    return (Integer)p1;
                } else if(p1 instanceof String) {
                    String str = (String)p1;
                    return Long.parseLong(str);
                } else {
                    return default_value;
                }
            }
            return default_value;
        } catch (IOException ex) {
            return default_value;
        }
    }
    
    @JsonIgnore
    public int readIntFrom(ADataStore datastore, String key, int default_value) {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        try {
            Object p1 = datastore.get(key);
            if(p1 != null) {
                if(p1 instanceof Long) {
                    Long l = (Long)p1;
                    return l.intValue();
                } else if(p1 instanceof Integer) {
                    return (Integer)p1;
                } else if(p1 instanceof String) {
                    String str = (String)p1;
                    return Integer.parseInt(str);
                } else {
                    return default_value;
                }
            }
            return default_value;
        } catch (IOException ex) {
            return default_value;
        }
    }
    
    @JsonIgnore
    public String readStringFrom(ADataStore datastore, String key, String default_value) throws IOException {
        if(datastore == null) {
            throw new IllegalArgumentException("datastore is null");
        }
        
        try {
            Object p1 = datastore.get(key);
            if(p1 != null) {
                if(p1 instanceof String) {
                    return (String)p1;
                } else {
                    return default_value;
                }
            }
            return default_value;
        } catch (IOException ex) {
            return default_value;
        }
    }
    
    @JsonIgnore
    public abstract void readFrom(ADataStore datastore);
    
    @JsonIgnore
    public abstract void addTo(ADataStore datastore) throws IOException;
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
}
