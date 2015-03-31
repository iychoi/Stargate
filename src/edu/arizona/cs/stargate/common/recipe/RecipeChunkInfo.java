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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class RecipeChunkInfo {
    
    private static final Log LOG = LogFactory.getLog(RecipeChunkInfo.class);
    
    private static final String OWNER_HOST_DEFAULT = "*";
    private static final String[] OWNER_HOST_DEFAULT_ARR = new String[] {OWNER_HOST_DEFAULT};
    
    private long chunkStart;
    private int chunkLen;
    private byte[] hash;
    private String[] ownerHost;
    
    RecipeChunkInfo() {
        this.chunkStart = -1;
        this.chunkLen = 0;
        this.hash = null;
        this.ownerHost = OWNER_HOST_DEFAULT_ARR;
    }
    
    public static RecipeChunkInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunkInfo) serializer.fromJsonFile(file, RecipeChunkInfo.class);
    }
    
    public static RecipeChunkInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunkInfo) serializer.fromJson(json, RecipeChunkInfo.class);
    }
    
    public RecipeChunkInfo(RecipeChunkInfo that) {
        this.chunkStart = that.chunkStart;
        this.chunkLen = that.chunkLen;
        this.hash = that.hash.clone();
        this.ownerHost = that.ownerHost;
    }
    
    public RecipeChunkInfo(long chunkStart, int chunkLen, byte[] hash) {
        initializeRecipeChunk(chunkStart, chunkLen, hash, OWNER_HOST_DEFAULT_ARR);
    }
    
    public RecipeChunkInfo(long chunkStart, int chunkLen, byte[] hash, String[] ownerHost) {
        initializeRecipeChunk(chunkStart, chunkLen, hash, ownerHost);
    }
    
    public RecipeChunkInfo(long chunkStart, int chunkLen) {
        initializeRecipeChunk(chunkStart, chunkLen, null, OWNER_HOST_DEFAULT_ARR);
    }
    
    public RecipeChunkInfo(long chunkStart, int chunkLen, String[] ownerHost) {
        initializeRecipeChunk(chunkStart, chunkLen, null, ownerHost);
    }
    
    private void initializeRecipeChunk(long chunkStart, int chunkLen, byte[] hash, String[] ownerHost) {
        this.chunkStart = chunkStart;
        this.chunkLen = chunkLen;
        this.hash = hash;
        
        if(ownerHost == null) {
            this.ownerHost = OWNER_HOST_DEFAULT_ARR;
        } else {
            this.ownerHost = ownerHost;
        }
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
        return this.chunkStart + ", " + this.chunkLen + ", " + DataFormatter.toHexString(this.hash).toLowerCase();
    }
    
    @JsonIgnore
    public ChunkInfo toChunk(URI resourcePath) {
        return new ChunkInfo(resourcePath, this.chunkStart, this.chunkLen, this.hash, this.ownerHost);
    }
}
