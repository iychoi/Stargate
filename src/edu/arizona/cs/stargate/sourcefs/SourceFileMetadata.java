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
package edu.arizona.cs.stargate.sourcefs;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class SourceFileMetadata {
    private static final Log LOG = LogFactory.getLog(SourceFileMetadata.class);
    
    private static final String HADOOP_CONFIG_KEY = SourceFileMetadata.class.getCanonicalName();
    
    private URI path;
    private long fileSize;
    private long lastModificationTime;
    
        public static SourceFileMetadata createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (SourceFileMetadata) serializer.fromJsonFile(file, SourceFileMetadata.class);
    }
    
    public static SourceFileMetadata createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (SourceFileMetadata) serializer.fromJson(json, SourceFileMetadata.class);
    }
    
    public static SourceFileMetadata createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (SourceFileMetadata) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, SourceFileMetadata.class);
    }
    
    public static SourceFileMetadata createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (SourceFileMetadata) serializer.fromJsonFile(fs, file, SourceFileMetadata.class);
    }
    
    public SourceFileMetadata() {
        this.path = null;
        this.fileSize = 0;
        this.lastModificationTime = 0;
    }
    
    public SourceFileMetadata(SourceFileMetadata that) {
        this.path = that.path;
        this.fileSize = that.fileSize;
        this.lastModificationTime = that.lastModificationTime;
    }
    
    public SourceFileMetadata(URI path, long fileSize, long lastModificationTime) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(fileSize < 0) {
            throw new IllegalArgumentException("fileSize is invalid");
        }
        
        if(lastModificationTime < 0) {
            throw new IllegalArgumentException("lastModificationTime is invalid");
        }
        
        initialize(path, fileSize, lastModificationTime);
    }
    
    private void initialize(URI path, long objSize, long lastModificationTime) {
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
        this.fileSize = objSize;
        this.lastModificationTime = lastModificationTime;
    }
    
    @JsonProperty("path")
    public URI getPath() {
        return this.path;
    }
    
    @JsonProperty("path")
    public void setPath(URI path) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        this.path = path;
    }
    
    @JsonProperty("file_size")
    public long getFileSize() {
        return this.fileSize;
    }
    
    @JsonProperty("file_size")
    public void setFileSize(long size) {
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        this.fileSize = size;
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
    
    @JsonIgnore
    public synchronized void saveTo(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(FileSystem fs, org.apache.hadoop.fs.Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
