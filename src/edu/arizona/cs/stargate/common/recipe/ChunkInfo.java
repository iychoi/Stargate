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

package edu.arizona.cs.stargate.common.recipe;

import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class ChunkInfo {
    
    private static final String OWNER_HOST_DEFAULT = "*";
    private static final String[] OWNER_HOST_DEFAULT_ARR = new String[] {OWNER_HOST_DEFAULT};
    
    private URI resourcePath;
    private long chunkStart;
    private int chunkLen;
    private byte[] hash;
    private String[] ownerHost;
    
    ChunkInfo() {
        this.resourcePath = null;
        this.chunkStart = -1;
        this.chunkLen = 0;
        this.hash = null;
        this.ownerHost = OWNER_HOST_DEFAULT_ARR;
    }
    
    public static ChunkInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ChunkInfo) serializer.fromJsonFile(file, ChunkInfo.class);
    }
    
    public static ChunkInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ChunkInfo) serializer.fromJson(json, ChunkInfo.class);
    }
    
    public ChunkInfo(ChunkInfo that) {
        this.resourcePath = that.resourcePath;
        this.chunkStart = that.chunkStart;
        this.chunkLen = that.chunkLen;
        this.hash = that.hash.clone();
    }
    
    public ChunkInfo(URI resourcePath, long chunkStart, int chunkLen, byte[] hash) {
        initializeChunkInfo(resourcePath, chunkStart, chunkLen, hash, OWNER_HOST_DEFAULT_ARR);
    }
    
    public ChunkInfo(URI resourcePath, long chunkStart, int chunkLen, byte[] hash, String[] ownerHost) {
        initializeChunkInfo(resourcePath, chunkStart, chunkLen, hash, ownerHost);
    }
    
    private void initializeChunkInfo(URI resourcePath, long chunkStart, int chunkLen, byte[] hash, String[] ownerHost) {
        this.resourcePath = resourcePath;
        this.chunkStart = chunkStart;
        this.chunkLen = chunkLen;
        this.hash = hash;
        this.ownerHost = ownerHost;
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
        return DataFormatter.toHexString(this.hash).toLowerCase();
    }
    
    @JsonIgnore
    public void setHash(byte[] hash) {
        this.hash = hash;
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        if(hash == null) {
            this.hash = null;
        }
        this.hash = DataFormatter.hexToBytes(hash);
    }

    @JsonProperty("path")
    public URI getResourcePath() {
        return this.resourcePath;
    }

    @JsonProperty("path")
    void setResourcePath(URI resourcePath) {
        this.resourcePath = resourcePath;
    }
    
    @JsonProperty("start")
    public long getChunkStart() {
        return this.chunkStart;
    }
    
    @JsonProperty("start")
    void setChunkStart(long offset) {
        this.chunkStart = offset;
    }
    
    @JsonProperty("len")
    public int getChunkLen() {
        return this.chunkLen;
    }
    
    @JsonProperty("len")
    void setChunkLen(int len) {
        this.chunkLen = len;
    }
    
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
    
    public boolean hasHash(String hash) {
        return hasHash(DataFormatter.hexToBytes(hash));
    }
    
    @JsonProperty("ownerHost")
    public String[] getOwnerHost() {
        return this.ownerHost;
    }
    
    @JsonIgnore
    public void setOwnerHost(String ownerHost) {
        this.ownerHost = new String[] {ownerHost};
    }
    
    @JsonProperty("ownerHost")
    public void setOwnerHost(String[] ownerHost) {
        this.ownerHost = ownerHost;
    }
    
    @Override
    public String toString() {
        return this.resourcePath.toString() + "(" + this.chunkStart + ", " + this.chunkLen + ", " + DataFormatter.toHexString(this.hash).toLowerCase() + ")";
    }
    
    public RecipeChunkInfo toRecipeChunk() {
        return new RecipeChunkInfo(this.chunkStart, this.chunkLen, this.hash, this.ownerHost);
    }
}
