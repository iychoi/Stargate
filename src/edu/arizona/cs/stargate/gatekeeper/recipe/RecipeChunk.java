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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class RecipeChunk {
    
    private static final Log LOG = LogFactory.getLog(RecipeChunk.class);
    
    private static final String OWNER_HOST_DEFAULT = "*";
    private static final String[] OWNER_HOST_DEFAULT_ARRAY = new String[] {OWNER_HOST_DEFAULT};
    
    private long offset;
    private int length;
    private byte[] hash;
    private String[] ownerHosts;
    
    RecipeChunk() {
        this.offset = -1;
        this.length = 0;
        this.hash = null;
        this.ownerHosts = OWNER_HOST_DEFAULT_ARRAY;
    }
    
    public static RecipeChunk createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunk) serializer.fromJsonFile(file, RecipeChunk.class);
    }
    
    public static RecipeChunk createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeChunk) serializer.fromJson(json, RecipeChunk.class);
    }
    
    public RecipeChunk(RecipeChunk that) {
        this.offset = that.offset;
        this.length = that.length;
        this.hash = that.hash.clone();
        this.ownerHosts = that.ownerHosts;
    }
    
    public RecipeChunk(long offset, int length, byte[] hash) {
        initializeRecipeChunk(offset, length, hash, OWNER_HOST_DEFAULT_ARRAY);
    }
    
    public RecipeChunk(long offset, int length, byte[] hash, String[] ownerHosts) {
        initializeRecipeChunk(offset, length, hash, ownerHosts);
    }
    
    public RecipeChunk(long offset, int length) {
        initializeRecipeChunk(offset, length, null, OWNER_HOST_DEFAULT_ARRAY);
    }
    
    public RecipeChunk(long offset, int length, String[] ownerHosts) {
        initializeRecipeChunk(offset, length, null, ownerHosts);
    }
    
    private void initializeRecipeChunk(long offset, int length, byte[] hash, String[] ownerHosts) {
        this.offset = offset;
        this.length = length;
        this.hash = hash;
        
        if(ownerHosts == null) {
            this.ownerHosts = OWNER_HOST_DEFAULT_ARRAY;
        } else {
            this.ownerHosts = ownerHosts;
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
        } else {
            this.hash = DataFormatUtils.hexToBytes(hash);
        }
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
    
    @JsonProperty("owner_hosts")
    public String[] getOwnerHosts() {
        return this.ownerHosts;
    }
    
    @JsonIgnore
    public void setOwnerHost(String ownerHost) {
        this.ownerHosts = new String[] {ownerHost};
    }
    
    @JsonProperty("owner_hosts")
    public void setOwnerHosts(String[] ownerHost) {
        this.ownerHosts = ownerHost;
    }
    
    @Override
    public String toString() {
        return this.offset + ", " + this.length + ", " + DataFormatUtils.toHexString(this.hash).toLowerCase();
    }
    
    @JsonIgnore
    public Chunk toChunk(URI resourcePath) {
        return new Chunk(resourcePath, this.offset, this.length, this.hash, this.ownerHosts);
    }
}
