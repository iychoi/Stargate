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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class LocalClusterRecipe {
    
    private URI resourcePath;
    private String hashAlgorithm;
    private ArrayList<RecipeChunkInfo> chunks = new ArrayList<RecipeChunkInfo>();

    LocalClusterRecipe() {
    }
    
    public static LocalClusterRecipe createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (LocalClusterRecipe) serializer.fromJsonFile(file, LocalClusterRecipe.class);
    }
    
    public static LocalClusterRecipe createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (LocalClusterRecipe) serializer.fromJson(json, LocalClusterRecipe.class);
    }
    
    public LocalClusterRecipe(URI resourcePath, String hashAlgorithm, Collection<RecipeChunkInfo> chunks) {
        initializeRecipe(resourcePath, hashAlgorithm, chunks);
    }
    
    private void initializeRecipe(URI resourcePath, String hashAlgorithm, Collection<RecipeChunkInfo> chunks) {
        this.resourcePath = resourcePath;
        this.hashAlgorithm = hashAlgorithm;
        if(chunks != null) {
            this.chunks.addAll(chunks);
        }
    }
    
    @JsonProperty("path")
    public URI getResourcePath() {
        return this.resourcePath;
    }
    
    @JsonProperty("path")
    void setResourcePath(URI resourcePath) {
        this.resourcePath = resourcePath;
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
        return this.resourcePath.toString() + ", " + this.hashAlgorithm + ", #entries = " + this.chunks.size();
    }
}
