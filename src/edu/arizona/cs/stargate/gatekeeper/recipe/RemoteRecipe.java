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

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class RemoteRecipe {
    
    private static final Log LOG = LogFactory.getLog(RemoteRecipe.class);
    
    private String virtualPath;
    private String hashAlgorithm;
    private ArrayList<RecipeChunk> chunks = new ArrayList<RecipeChunk>();

    public RemoteRecipe() {
    }
    
    public static RemoteRecipe createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RemoteRecipe) serializer.fromJsonFile(file, RemoteRecipe.class);
    }
    
    public static RemoteRecipe createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (RemoteRecipe) serializer.fromJson(json, RemoteRecipe.class);
    }
    
    public RemoteRecipe(String virtualPath, String hashAlgorithm, Collection<RecipeChunk> chunks) {
        initializeRemoteRecipe(virtualPath, hashAlgorithm, chunks);
    }
    
    public RemoteRecipe(String virtualPath, LocalRecipe recipe) {
        initializeRemoteRecipe(virtualPath, recipe.getHashAlgorithm(), recipe.getAllChunks());
    }
    
    private void initializeRemoteRecipe(String virtualPath, String hashAlgorithm, Collection<RecipeChunk> chunks) {
        this.virtualPath = virtualPath;
        this.hashAlgorithm = hashAlgorithm;
        if(chunks != null) {
            this.chunks.addAll(chunks);
        }
    }
    
    @JsonProperty("virthalPath")
    public String getVirtualPath() {
        return this.virtualPath;
    }
    
    @JsonProperty("virthalPath")
    void setVirtualPath(String virtualPath) {
        this.virtualPath = virtualPath;
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
    public Collection<RecipeChunk> getAllChunks() {
        return Collections.unmodifiableCollection(this.chunks);
    }
    
    @JsonIgnore
    public RecipeChunk getChunk(String hash) {
        Iterator<RecipeChunk> iterator = this.chunks.iterator();
        while(iterator.hasNext()) {
            RecipeChunk chunk = iterator.next();
            if(chunk.hasHash(hash)) {
                return chunk;
            }
        }
        return null;
    }
    
    @JsonProperty("chunks")
    public void addChunks(Collection<RecipeChunk> chunks) {
        this.chunks.addAll(chunks);
    }
    
    @JsonIgnore
    public void addChunk(RecipeChunk chunk) {
        this.chunks.add(chunk);
    }
    
    public String toString() {
        return this.virtualPath + ", " + this.hashAlgorithm + ", #entries = " + this.chunks.size();
    }
}
