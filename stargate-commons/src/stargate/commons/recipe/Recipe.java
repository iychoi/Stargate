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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class Recipe {
    
    private static final Log LOG = LogFactory.getLog(Recipe.class);
    
    private DataObjectMetadata metadata;
    private String hashAlgorithm;
    private int chunkSize;
    private List<RecipeChunk> chunk = new ArrayList<RecipeChunk>();

    public static Recipe createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (Recipe) serializer.fromJsonFile(file, Recipe.class);
    }
    
    public static Recipe createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (Recipe) serializer.fromJson(json, Recipe.class);
    }
    
    public Recipe() {
        this.metadata = null;
        this.hashAlgorithm = null;
        this.chunkSize = 0;
    }
    
    public Recipe(Recipe that) {
        this.metadata = that.metadata;
        this.hashAlgorithm = that.hashAlgorithm;
        this.chunkSize = that.chunkSize;
    }
    
    public Recipe(DataObjectMetadata metadata, String hashAlgorithm, int chunkSize) {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is null or empty");
        }
        
        if(hashAlgorithm == null || hashAlgorithm.isEmpty()) {
            throw new IllegalArgumentException("hashAlgorithm is null or empty");
        }
        
        if(chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize is invalid");
        }
        
        initialize(metadata, hashAlgorithm, chunkSize, null);
    }
    
    public Recipe(DataObjectMetadata metadata, String hashAlgorithm, int chunkSize, Collection<RecipeChunk> chunk) {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is null or empty");
        }
        
        if(hashAlgorithm == null || hashAlgorithm.isEmpty()) {
            throw new IllegalArgumentException("hashAlgorithm is null or empty");
        }
        
        if(chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize is invalid");
        }
        
        if(chunk == null) {
            throw new IllegalArgumentException("chunk is null");
        }
        
        initialize(metadata, hashAlgorithm, chunkSize, chunk);
    }
    
    private void initialize(DataObjectMetadata metadata, String hashAlgorithm, int chunkSize, Collection<RecipeChunk> chunk) {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is null or empty");
        }
        
        if(hashAlgorithm == null || hashAlgorithm.isEmpty()) {
            throw new IllegalArgumentException("hashAlgorithm is null or empty");
        }
        
        if(chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize is invalid");
        }
        
        this.metadata = metadata;
        this.hashAlgorithm = hashAlgorithm;
        this.chunkSize = chunkSize;
        if(chunk != null) {
            this.chunk.addAll(chunk);
        }
    }
    
    @JsonProperty("metadata")
    public DataObjectMetadata getMetadata() {
        return this.metadata;
    }
    
    @JsonProperty("metadata")
    public void setMetadata(DataObjectMetadata metadata) {
        this.metadata = metadata;
    }
    
    @JsonProperty("hash_algorithm")
    public String getHashAlgorithm() {
        return this.hashAlgorithm;
    }
    
    @JsonProperty("hash_algorithm")
    public void setHashAlgorithm(String hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }
    
    @JsonProperty("chunk_size")
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    @JsonProperty("chunk_size")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
    
    @JsonProperty("chunk")
    public Collection<RecipeChunk> getChunk() {
        return Collections.unmodifiableCollection(this.chunk);
    }
    
    @JsonIgnore
    public RecipeChunk getChunk(long offset) throws IOException {
        if(this.chunkSize != 0) {
            int index = (int) (offset / this.chunkSize);
            RecipeChunk chunk = this.chunk.get(index);
            
            if(chunk.getOffset() <= offset && 
                    chunk.getOffset() + chunk.getLength() > offset) {
                return chunk;
            } else {
                throw new IOException("unable to find chunk at " + offset);
            }
        }
        
        RecipeChunk searchKey = new RecipeChunk();
        searchKey.setOffset(offset);
        searchKey.setLength(0);
        int location = Collections.binarySearch(this.chunk, searchKey, new Comparator<RecipeChunk>(){

            @Override
            public int compare(RecipeChunk t, RecipeChunk t1) {
                return (int) (t.getOffset() - t1.getOffset());
            }
        });

        if(location >= 0) {
            return this.chunk.get(location);
        } else {
            RecipeChunk chunk = this.chunk.get(Math.abs(location + 1));
            if(chunk.getOffset() <= offset && 
                    chunk.getOffset() + chunk.getLength() > offset) {
                return chunk;
            } else {
                throw new IOException("unable to find chunk at " + offset);
            }
        }
    }
    
    @JsonProperty("chunk")
    public void addChunk(Collection<RecipeChunk> chunk) {
        this.chunk.addAll(chunk);
    }
    
    @JsonIgnore
    public void addChunk(RecipeChunk chunk) {
        this.chunk.add(chunk);
    }
    
    @JsonIgnore
    public void clearChunk() {
        this.chunk.clear();
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.metadata == null || this.metadata.isEmpty()) {
            return true;
        }
        return false;
    }
    
    @Override
    public String toString() {
        return this.metadata.toString() + ", " + this.hashAlgorithm + ", " + this.chunkSize;
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
