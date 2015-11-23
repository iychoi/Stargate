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
package stargate.commons.recipe;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class DataObjectMetadata {
    
    private static final Log LOG = LogFactory.getLog(DataObjectMetadata.class);
    
    private DataObjectPath path;
    private long objectSize;
    private boolean directory;
    private long lastModificationTime;
    
    public static DataObjectMetadata createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (DataObjectMetadata) serializer.fromJsonFile(file, DataObjectMetadata.class);
    }
    
    public static DataObjectMetadata createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (DataObjectMetadata) serializer.fromJson(json, DataObjectMetadata.class);
    }
    
    public DataObjectMetadata() {
        this.path = null;
        this.objectSize = 0;
        this.directory = false;
        this.lastModificationTime = 0;
    }
    
    public DataObjectMetadata(DataObjectMetadata that) {
        this.path = that.path;
        this.objectSize = that.objectSize;
        this.directory = that.directory;
        this.lastModificationTime = that.lastModificationTime;
    }
    
    public DataObjectMetadata(DataObjectPath path, long objSize, long lastModificationTime) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(objSize < 0) {
            throw new IllegalArgumentException("objSize is invalid");
        }
        
        if(lastModificationTime < 0) {
            throw new IllegalArgumentException("lastModificationTime is invalid");
        }
        
        initialize(path, objSize, false, lastModificationTime);
    }
    
    public DataObjectMetadata(DataObjectPath path, long objSize, boolean directory, long lastModificationTime) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(objSize < 0) {
            throw new IllegalArgumentException("objSize is invalid");
        }
        
        if(lastModificationTime < 0) {
            throw new IllegalArgumentException("lastModificationTime is invalid");
        }
        
        initialize(path, objSize, directory, lastModificationTime);
    }
    
    private void initialize(DataObjectPath path, long objSize, boolean directory, long lastModificationTime) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(objSize < 0) {
            throw new IllegalArgumentException("objSize is invalid");
        }
        
        if(lastModificationTime < 0) {
            throw new IllegalArgumentException("lastModificationTime is invalid");
        }
        
        this.path = path;
        this.objectSize = objSize;
        this.directory = directory;
        this.lastModificationTime = lastModificationTime;
    }
    
    @JsonProperty("path")
    public DataObjectPath getPath() {
        return this.path;
    }
    
    @JsonProperty("path")
    public void setPath(DataObjectPath path) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        this.path = path;
    }
    
    @JsonProperty("object_size")
    public long getObjectSize() {
        return this.objectSize;
    }
    
    @JsonProperty("object_size")
    public void setObjectSize(long size) {
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        this.objectSize = size;
    }
    
    @JsonProperty("directory")
    public boolean isDirectory() {
        return this.directory;
    }
    
    @JsonProperty("directory")
    public void setDirectory(boolean directory) {
        this.directory = directory;
    }
    
    @JsonProperty("last_modification_time")
    public long getLastModificationTime() {
        return this.lastModificationTime;
    }
    
    @JsonProperty("last_modification_time")
    public void setLastModificationTime(long lastModificationTime) {
        if(lastModificationTime < 0) {
            throw new IllegalArgumentException("lastModificationTime is invalid");
        }
        
        this.lastModificationTime = lastModificationTime;
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.path == null) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        return this.path.toString();
    }
    
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
