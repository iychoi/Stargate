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
package stargate.server.blockcache;

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
public class BlockCacheMetadata {
    
    private static final Log LOG = LogFactory.getLog(BlockCacheMetadata.class);
    
    private String hash;
    private long size;
    private long creationTime;
    
    public static BlockCacheMetadata createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (BlockCacheMetadata) serializer.fromJsonFile(file, BlockCacheMetadata.class);
    }
    
    public static BlockCacheMetadata createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (BlockCacheMetadata) serializer.fromJson(json, BlockCacheMetadata.class);
    }
    
    public BlockCacheMetadata() {
        this.hash = null;
        this.size = 0;
        this.creationTime = 0;
    }
    
    public BlockCacheMetadata(BlockCacheMetadata that) {
        this.hash = that.hash;
        this.size = that.size;
        this.creationTime = that.creationTime;
    }
    
    public BlockCacheMetadata(String hash, long size, long creationTime) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        if(creationTime < 0) {
            throw new IllegalArgumentException("creationTime is invalid");
        }
        
        initialize(hash, size, creationTime);
    }
    
    private void initialize(String hash, long size, long creationTime) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        if(creationTime < 0) {
            throw new IllegalArgumentException("creationTime is invalid");
        }
        
        this.hash = hash;
        this.size = size;
        this.creationTime = creationTime;
    }
    
    @JsonProperty("hash")
    public String getHash() {
        return this.hash;
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        this.hash = hash;
    }
    
    @JsonProperty("size")
    public long getSize() {
        return this.size;
    }
    
    @JsonProperty("size")
    public void setSize(long size) {
        this.size = size;
    }
    
    @JsonProperty("creation_time")
    public long getCreationTime() {
        return this.creationTime;
    }
    
    @JsonProperty("creation_time")
    public void setCreationTime(long creationTime) {
        if(creationTime < 0) {
            throw new IllegalArgumentException("creationTime is invalid");
        }
        
        this.creationTime = creationTime;
    }

    @JsonIgnore
    public boolean isEmpty() {
        if(this.hash == null || this.hash.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public String toString() {
        return this.hash + "(" + this.creationTime + ")";
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
