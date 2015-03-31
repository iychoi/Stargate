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

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class RemoteClusterRecipe {
    private String vpath;
    private String hashAlgorithm;
    private ArrayList<RecipeChunkInfo> chunks = new ArrayList<RecipeChunkInfo>();

    RemoteClusterRecipe() {
    }
    
    public static RemoteClusterRecipe createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RemoteClusterRecipe) serializer.fromJsonFile(file, RemoteClusterRecipe.class);
    }
    
    public static RemoteClusterRecipe createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RemoteClusterRecipe) serializer.fromJson(json, RemoteClusterRecipe.class);
    }
    
    public RemoteClusterRecipe(String vpath, String hashAlgorithm, Collection<RecipeChunkInfo> chunks) {
        initializeRecipe(vpath, hashAlgorithm, chunks);
    }
    
    public RemoteClusterRecipe(String vpath, LocalClusterRecipe recipe) {
        initializeRecipe(vpath, recipe.getHashAlgorithm(), recipe.getAllChunk());
    }
    
    private void initializeRecipe(String vpath, String hashAlgorithm, Collection<RecipeChunkInfo> chunks) {
        this.vpath = vpath;
        this.hashAlgorithm = hashAlgorithm;
        if(chunks != null) {
            this.chunks.addAll(chunks);
        }
    }
    
    @JsonProperty("vpath")
    public String getVirtualPath() {
        return this.vpath;
    }
    
    @JsonProperty("vpath")
    void setVirtualPath(String vpath) {
        this.vpath = vpath;
    }
    
    @JsonProperty("algorithm")
    public String getHashAlgorithm() {
        return this.hashAlgorithm;
    }
    
    @JsonProperty("algorithm")
    void setHashAlgorithm(String hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }
    
    @JsonProperty("chunkinfo")
    public Collection<RecipeChunkInfo> getAllChunk() {
        return Collections.unmodifiableCollection(this.chunks);
    }
    
    @JsonProperty("chunkinfo")
    void addChunk(Collection<RecipeChunkInfo> chunks) {
        this.chunks.addAll(chunks);
    }
    
    @JsonIgnore
    void addChunk(RecipeChunkInfo chunk) {
        this.chunks.add(chunk);
    }
    
    public String toString() {
        return this.vpath + ", " + this.hashAlgorithm + ", #entries = " + this.chunks.size();
    }
}
