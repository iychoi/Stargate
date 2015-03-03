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
public class RecipeChunkInfo {
    private long chunkStart;
    private int chunkLen;
    private byte[] hash;
    
    RecipeChunkInfo() {
        this.chunkStart = -1;
        this.chunkLen = 0;
        this.hash = null;
    }
    
    public static ChunkInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ChunkInfo) serializer.fromJsonFile(file, ChunkInfo.class);
    }
    
    public static ChunkInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (ChunkInfo) serializer.fromJson(json, ChunkInfo.class);
    }
    
    public RecipeChunkInfo(RecipeChunkInfo that) {
        this.chunkStart = that.chunkStart;
        this.chunkLen = that.chunkLen;
        this.hash = that.hash.clone();
    }
    
    public RecipeChunkInfo(long chunkStart, int chunkLen, byte[] hash) {
        initializeRecipeChunk(chunkStart, chunkLen, hash);
    }
    
    public RecipeChunkInfo(long chunkStart, int chunkLen) {
        initializeRecipeChunk(chunkStart, chunkLen, null);
    }
    
    private void initializeRecipeChunk(long chunkStart, int chunkLen, byte[] hash) {
        this.chunkStart = chunkStart;
        this.chunkLen = chunkLen;
        this.hash = hash;
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
        return DataFormatter.toHexString(this.hash);
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
            this.hash = DataFormatter.hexToBytes(hash);
        }
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
    
    @JsonIgnore
    public boolean isHashed() {
        if(this.hash == null) {
            return false;
        }
        return true;
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
        return hasHash(DataFormatter.hexToBytes(hash));
    }
    
    @Override
    public String toString() {
        return this.chunkStart + ", " + this.chunkLen + ", " + DataFormatter.toHexString(this.hash);
    }
    
    @JsonIgnore
    public ChunkInfo toChunk(URI resourcePath) {
        return new ChunkInfo(resourcePath, this.chunkStart, this.chunkLen, this.hash);
    }
}
