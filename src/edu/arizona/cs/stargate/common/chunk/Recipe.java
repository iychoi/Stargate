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

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class Recipe {
    
    private URI resourcePath;
    private String hashAlgorithm;
    private ArrayList<RecipeChunk> chunks = new ArrayList<RecipeChunk>();

    Recipe() {
    }
    
    public static Recipe createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Recipe) serializer.fromJsonFile(file, Recipe.class);
    }
    
    public static Recipe createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (Recipe) serializer.fromJson(json, Recipe.class);
    }
    
    public Recipe(File file, String hashAlgorithm, Collection<Chunk> chunks) {
        this.resourcePath = file.getAbsoluteFile().toURI();
        this.hashAlgorithm = hashAlgorithm;
        for(Chunk chk : chunks) {
            this.chunks.add(chk.toRecipeChunk());
        }
    }

    public Recipe(URI resourcePath, String hashAlgorithm, Collection<Chunk> chunks) {
        this.resourcePath = resourcePath;
        this.hashAlgorithm = hashAlgorithm;
        for(Chunk chk : chunks) {
            this.chunks.add(chk.toRecipeChunk());
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
    
    @JsonProperty("chunks")
    public Collection<RecipeChunk> getChunks() {
        return Collections.unmodifiableCollection(this.chunks);
    }
    
    @JsonProperty("chunks")
    void addChunk(Collection<RecipeChunk> chunks) {
        this.chunks.addAll(chunks);
    }
    
    public String toString() {
        return this.resourcePath.toString() + ", " + this.hashAlgorithm + ", #entries = " + this.chunks.size();
    }
}
