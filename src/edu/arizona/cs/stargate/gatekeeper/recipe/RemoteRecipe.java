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
    
    private String clusterName;
    private String virtualPath;
    private String hashAlgorithm;
    private int chunkSize;
    private long size;
    private long modificationTime;
    
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
    
    public RemoteRecipe(String clusterName, String virtualPath, String hashAlgorithm, long size, long modificationTime, int chunkSize, Collection<RecipeChunk> chunks) {
        initializeRemoteRecipe(clusterName, virtualPath, hashAlgorithm, size, modificationTime, chunkSize, chunks);
    }
    
    public RemoteRecipe(String clusterName, String virtualPath, LocalRecipe recipe) {
        initializeRemoteRecipe(clusterName, virtualPath, recipe.getHashAlgorithm(), recipe.getSize(), recipe.getModificationTime(), recipe.getChunkSize(), recipe.getAllChunks());
    }
    
    private void initializeRemoteRecipe(String clusterName, String virtualPath, String hashAlgorithm, long size, long modificationTime, int chunkSize, Collection<RecipeChunk> chunks) {
        this.clusterName = clusterName;
        this.virtualPath = virtualPath;
        this.hashAlgorithm = hashAlgorithm;
        this.size = size;
        this.modificationTime = modificationTime;
        this.chunkSize = chunkSize;
        if(chunks != null) {
            this.chunks.addAll(chunks);
        }
    }
    
    @JsonProperty("cluster_name")
    public String getClusterName() {
        return this.clusterName;
    }
    
    @JsonProperty("cluster_name")
    void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    @JsonProperty("virthal_path")
    public String getVirtualPath() {
        return this.virtualPath;
    }
    
    @JsonProperty("virthal_path")
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
    
    @JsonProperty("size")
    public long getSize() {
        return this.size;
    }
    
    @JsonProperty("size")
    public void setSize(long size) {
        this.size = size;
    }
    
    @JsonProperty("chunk_size")
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    @JsonProperty("chunk_size")
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }
    
    @JsonProperty("modification_time")
    public long getModificationTime() {
        return this.modificationTime;
    }
    
    @JsonProperty("modification_time")
    public void setModificationTime(long time) {
        this.modificationTime = time;
    }
    
    public String toString() {
        return this.clusterName + ", " + this.virtualPath + ", " + this.hashAlgorithm + ", #entries = " + this.chunks.size();
    }
}
