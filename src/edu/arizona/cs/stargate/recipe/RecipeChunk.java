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

package edu.arizona.cs.stargate.recipe;

import edu.arizona.cs.stargate.common.utils.HexaUtils;
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
public class RecipeChunk {
    
    private static final Log LOG = LogFactory.getLog(RecipeChunk.class);
    
    private static final String HADOOP_CONFIG_KEY = RecipeChunk.class.getCanonicalName();
    
    private long offset;
    private int length;
    private byte[] hash;
    
    public static RecipeChunk createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunk) serializer.fromJsonFile(file, RecipeChunk.class);
    }
    
    public static RecipeChunk createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunk) serializer.fromJson(json, RecipeChunk.class);
    }
    
    public static RecipeChunk createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunk) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, RecipeChunk.class);
    }
    
    public static RecipeChunk createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunk) serializer.fromJsonFile(fs, file, RecipeChunk.class);
    }
    
    public RecipeChunk() {
        this.offset = 0;
        this.length = 0;
        this.hash = null;
    }
    
    public RecipeChunk(RecipeChunk that) {
        this.offset = that.offset;
        this.length = that.length;
        this.hash = that.hash;
    }
    
    public RecipeChunk(long offset, int length, byte[] hash) {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(length < 0) {
            throw new IllegalArgumentException("length is invalid");
        }
        
        if(hash == null) {
            throw new IllegalArgumentException("hash is null");
        }
        
        initialize(offset, length, hash);
    }
    
    private void initialize(long offset, int length, byte[] hash) {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(length < 0) {
            throw new IllegalArgumentException("length is invalid");
        }
        
        if(hash == null) {
            throw new IllegalArgumentException("hash is null");
        }
        
        this.offset = offset;
        this.length = length;
        this.hash = hash;
    }
    
    @JsonProperty("offset")
    public long getOffset() {
        return this.offset;
    }
    
    @JsonProperty("offset")
    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    @JsonProperty("length")
    public int getLength() {
        return this.length;
    }
    
    @JsonProperty("length")
    public void setLength(int len) {
        this.length = len;
    }

    @JsonIgnore
    public byte[] getHash() {
        return this.hash;
    }
    
    @JsonProperty("hash")
    public String getHashString() {
        if(this.hash == null) {
            return null;
        }
        return HexaUtils.toHexString(this.hash).toLowerCase();
    }
    
    @JsonIgnore
    public void setHash(byte[] hash) {
        this.hash = hash;
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        if(hash == null) {
            this.hash = null;
        } else {
            this.hash = HexaUtils.hexToBytes(hash);
        }
    }

    @JsonIgnore
    public boolean hasHash(byte[] hash) {
        if(this.hash == null) {
            return false;
        }
        
        if(this.hash.length == hash.length) {
            for(int i=0;i<this.hash.length;i++) {
                if(this.hash[i] != hash[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    @JsonIgnore
    public boolean hasHash(String hash) {
        return hasHash(HexaUtils.hexToBytes(hash));
    }
    
    @Override
    public String toString() {
        return this.offset + ", " + this.length + ", " + HexaUtils.toHexString(this.hash).toLowerCase();
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
