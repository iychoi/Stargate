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
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author iychoi
 */
public class MemoryRecipeStore extends ARecipeStore {
    private Map<URI, Recipe> recipeTable = new HashMap<URI, Recipe>();
    private Map<String, ChunkInfo> hashes = new HashMap<String, ChunkInfo>();

    public MemoryRecipeStore() {
        
    }
    
    @Override
    public synchronized void store(Recipe recipe) {
        this.recipeTable.put(recipe.getResourcePath(), recipe);
        storeAllHashes(recipe);
    }
    
    @Override
    public synchronized void notifyRecipeHashed(Recipe recipe) {
        storeAllHashes(recipe);
    }
    
    @Override
    public synchronized boolean hasRecipe(URI resourceUri) {
        return this.recipeTable.containsKey(resourceUri);
    }

    @Override
    public synchronized Recipe get(URI resourceUri) {
        return this.recipeTable.get(resourceUri);
    }

    @Override
    public synchronized void remove(URI resourceUri) {
        this.recipeTable.remove(resourceUri);
        makeConsistentHashes();
    }
    
    @Override
    public void removeAll() {
        this.recipeTable.clear();
        this.hashes.clear();
    }

    @Override
    public synchronized ChunkInfo find(byte[] hash) {
        return this.hashes.get(DataFormatter.toHexString(hash));
    }
    
    @Override
    public synchronized ChunkInfo find(String hash) {
        return this.hashes.get(hash);
    }

    private synchronized void storeAllHashes(Recipe recipe) {
        Collection<RecipeChunkInfo> chunks = recipe.getAllChunk();
        for(RecipeChunkInfo chunk : chunks) {
            if(chunk.isHashed()) {
                if(!this.hashes.containsKey(chunk.getHashString())) {
                    this.hashes.put(chunk.getHashString(), chunk.toChunk(recipe.getResourcePath()));
                }
            }
        }
    }
    
    private synchronized void makeConsistentHashes() {
        this.hashes.clear();
        
        Set<URI> uris = this.recipeTable.keySet();
        for(URI resourceUri : uris) {
            Recipe recipe = this.recipeTable.get(uris);
            storeAllHashes(recipe);
        }
    }
}
