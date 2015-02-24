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

package edu.arizona.cs.stargate.common.chunk;

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
public class RecipeChunk {
    private long chunkStart;
    private int chunkLen;
    private byte[] hash;
    
    RecipeChunk() {
        this.chunkStart = -1;
        this.chunkLen = 0;
        this.hash = null;
    }
    
    public static Chunk createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Chunk) serializer.fromJsonFile(file, Chunk.class);
    }
    
    public static Chunk createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Chunk) serializer.fromJson(json, Chunk.class);
    }
    
    public RecipeChunk(RecipeChunk that) {
        this.chunkStart = that.chunkStart;
        this.chunkLen = that.chunkLen;
        this.hash = that.hash.clone();
    }
    
    public RecipeChunk(long chunkStart, int chunkLen, byte[] hash) {
        initializeRecipeChunk(chunkStart, chunkLen, hash);
    }
    
    private void initializeRecipeChunk(long chunkStart, int chunkLen, byte[] hash) {
        this.chunkStart = chunkStart;
        this.chunkLen = chunkLen;
        this.hash = hash;
    }

    @JsonProperty("hash")
    public byte[] getHash() {
        return this.hash;
    }
    
    @JsonIgnore
    public String getHashString() {
        return DataFormatter.toHexString(this.hash);
    }
    
    @JsonProperty("hash")
    void setHash(byte[] hash) {
        this.hash = hash;
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
    
    @Override
    public String toString() {
        return this.chunkStart + ", " + this.chunkLen + ", " + DataFormatter.toHexString(this.hash);
    }
    
    public Chunk toChunk(URI resourcePath) {
        return new Chunk(resourcePath, this.chunkStart, this.chunkLen, this.hash);
    }
}
