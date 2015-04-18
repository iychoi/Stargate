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

package edu.arizona.cs.stargate.gatekeeper.recipe;

import edu.arizona.cs.stargate.common.DataFormatUtils;
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
public class Chunk {
    
    private static final String OWNER_HOST_DEFAULT = "*";
    private static final String[] OWNER_HOST_DEFAULT_ARRAY = new String[] {OWNER_HOST_DEFAULT};
    
    private URI resourcePath;
    private long offset;
    private int length;
    private byte[] hash;
    private String[] ownerHosts;
    
    Chunk() {
        this.resourcePath = null;
        this.offset = -1;
        this.length = 0;
        this.hash = null;
        this.ownerHosts = OWNER_HOST_DEFAULT_ARRAY;
    }
    
    public static Chunk createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Chunk) serializer.fromJsonFile(file, Chunk.class);
    }
    
    public static Chunk createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Chunk) serializer.fromJson(json, Chunk.class);
    }
    
    public Chunk(Chunk that) {
        this.resourcePath = that.resourcePath;
        this.offset = that.offset;
        this.length = that.length;
        this.hash = that.hash.clone();
    }
    
    public Chunk(URI resourcePath, long chunkStart, int chunkLen, byte[] hash) {
        initializeChunk(resourcePath, chunkStart, chunkLen, hash, OWNER_HOST_DEFAULT_ARRAY);
    }
    
    public Chunk(URI resourcePath, long chunkStart, int chunkLen, byte[] hash, String[] ownerHosts) {
        initializeChunk(resourcePath, chunkStart, chunkLen, hash, ownerHosts);
    }
    
    private void initializeChunk(URI resourcePath, long offset, int length, byte[] hash, String[] ownerHosts) {
        this.resourcePath = resourcePath;
        this.offset = offset;
        this.length = length;
        this.hash = hash;
        this.ownerHosts = ownerHosts;
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
        return DataFormatUtils.toHexString(this.hash).toLowerCase();
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
        this.hash = DataFormatUtils.hexToBytes(hash);
    }

    @JsonProperty("path")
    public URI getResourcePath() {
        return this.resourcePath;
    }

    @JsonProperty("path")
    void setResourcePath(URI resourcePath) {
        this.resourcePath = resourcePath;
    }
    
    @JsonProperty("offset")
    public long getOffset() {
        return this.offset;
    }
    
    @JsonProperty("offset")
    void setOffset(long offset) {
        this.offset = offset;
    }
    
    @JsonProperty("length")
    public int getLength() {
        return this.length;
    }
    
    @JsonProperty("length")
    void setLength(int len) {
        this.length = len;
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
        return hasHash(DataFormatUtils.hexToBytes(hash));
    }
    
    @JsonProperty("ownerHosts")
    public String[] getOwnerHosts() {
        return this.ownerHosts;
    }
    
    @JsonIgnore
    public void setOwnerHost(String ownerHost) {
        this.ownerHosts = new String[] {ownerHost};
    }
    
    @JsonProperty("ownerHosts")
    public void setOwnerHosts(String[] ownerHosts) {
        this.ownerHosts = ownerHosts;
    }
    
    @Override
    public String toString() {
        return this.resourcePath.toString() + "(" + this.offset + ", " + this.length + ", " + DataFormatUtils.toHexString(this.hash).toLowerCase() + ")";
    }
    
    public RecipeChunk toRecipeChunk() {
        return new RecipeChunk(this.offset, this.length, this.hash, this.ownerHosts);
    }
}
