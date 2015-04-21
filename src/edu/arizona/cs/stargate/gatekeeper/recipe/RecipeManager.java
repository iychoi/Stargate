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

import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import edu.arizona.cs.stargate.gatekeeper.distributedcache.JsonIMap;
import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.distributedcache.DistributedCacheService;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManager {
    private static final Log LOG = LogFactory.getLog(RecipeManager.class);
    
    private static final int RECIPE_BACKGROUND_WORKER_THREADS = 1;
    
    private static final String RECIPEMANAGER_RECIPES_MAP_ID = "RecipeManager_Recipes";
    private static final String RECIPEMANAGER_CHUNKINFO_MAP_ID = "RecipeManager_Chunkinfo";
    private static final String RECIPEMANAGER_PENDING_RECIPES_MAP_ID = "RecipeManager_Pending_Recipes";
    
    private static RecipeManager instance;

    private RecipeManagerConfiguration config;
    
    private JsonIMap<URI, LocalRecipe> recipes;
    private JsonIMap<URI, LocalRecipe> pendingRecipes;
    private JsonIMap<String, URI[]> chunks;
    
    // check message and start hashing
    private ExecutorService backgroundWorker;
    
    public static RecipeManager getInstance(RecipeManagerConfiguration config) {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager(config);
            }
            return instance;
        }
    }
    
    public static RecipeManager getInstance() throws ServiceNotStartedException {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("RecipeManager is not started");
            }
            return instance;
        }
    }
    
    RecipeManager(RecipeManagerConfiguration config) {
        if(config == null) {
            this.config = new RecipeManagerConfiguration();
        } else {
            this.config = config;
            this.config.setImmutable();
        }
        
        try {
            DistributedCacheService distributedCacheService = DistributedCacheService.getInstance();
            this.recipes = new JsonIMap<URI, LocalRecipe>(distributedCacheService.getDistributedMap(RECIPEMANAGER_RECIPES_MAP_ID), LocalRecipe.class);
            this.pendingRecipes = new JsonIMap<URI, LocalRecipe>(distributedCacheService.getDistributedMap(RECIPEMANAGER_PENDING_RECIPES_MAP_ID), LocalRecipe.class);
            this.pendingRecipes.addEntryListener(new EntryListener<URI, LocalRecipe>(){

                @Override
                public void entryAdded(EntryEvent<URI, LocalRecipe> ee) {
                    Runnable worker = new BackgroundWorker(); 
                    backgroundWorker.execute(worker);
                }

                @Override
                public void entryRemoved(EntryEvent<URI, LocalRecipe> ee) {
                }

                @Override
                public void entryUpdated(EntryEvent<URI, LocalRecipe> ee) {
                }

                @Override
                public void entryEvicted(EntryEvent<URI, LocalRecipe> ee) {
                }

                @Override
                public void mapEvicted(MapEvent me) {
                }

                @Override
                public void mapCleared(MapEvent me) {
                }
            }, true);
            
            this.chunks = new JsonIMap<String, URI[]>(distributedCacheService.getDistributedMap(RECIPEMANAGER_CHUNKINFO_MAP_ID), URI[].class);
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            throw new RuntimeException(ex);
        }
        
        this.backgroundWorker = Executors.newFixedThreadPool(RECIPE_BACKGROUND_WORKER_THREADS);
    }
    
    public RecipeManagerConfiguration getConfiguration() {
        return this.config;
    }
    
    public synchronized LocalRecipe generateRecipe(DataExport export) {
        URI resourceUri = export.getResourcePath();
        return generateRecipe(resourceUri);
    }
    
    public synchronized LocalRecipe generateRecipe(URI resourceUri) {
        try {
            if(!this.recipes.containsKey(resourceUri) && 
                    !this.pendingRecipes.containsKey(resourceUri)) {
                LOG.info("Generating a new recipe of " + resourceUri.toASCIIString());
                ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(resourceUri, this.config.getChunkSize());
                LocalRecipe recipe = recipeGenerator.generateRecipe(resourceUri, this.config.getHashAlgorithm());
                this.pendingRecipes.put(resourceUri, recipe);
                return recipe;
            }
        } catch(Exception ex) {
            LOG.error(ex);
            return null;
        }
        return null;
    }
    
    public synchronized LocalRecipe getRecipe(URI resourceUri) {
        LocalRecipe recipe;
        recipe = this.recipes.get(resourceUri);
        if(recipe != null) {
            return recipe;
        }
        
        recipe = this.pendingRecipes.get(resourceUri);
        if(recipe != null) {
            return recipe;
        }
        
        return generateRecipe(resourceUri);
    }
    
    public synchronized void removeRecipe(DataExport export) {
        removeRecipe(export.getResourcePath());
    }
    
    public synchronized void removeRecipe(URI resourceUri) {
        LocalRecipe recipe1 = this.recipes.remove(resourceUri);
        LocalRecipe recipe2 = this.pendingRecipes.remove(resourceUri);
        
        LocalRecipe recipe = null;
        if(recipe1 != null) {
            recipe = recipe1;
        }
        if(recipe2 != null) {
            recipe = recipe2;
        }
        if(recipe != null) {
            Collection<RecipeChunk> chunks = recipe.getAllChunks();
            for(RecipeChunk info : chunks) {
                removeChunk(info.getHashString(), recipe.getResourcePath());
            }
        }
    }
    
    public synchronized void removeAllRecipes() {
        this.recipes.clear();
        this.pendingRecipes.clear();
        this.chunks.clear();
    }
    
    private synchronized void addChunk(String hash, URI resourceURI) {
        URI[] chunksResourceURIs = this.chunks.remove(hash);
        
        ArrayList<URI> newResourceURIs = new ArrayList<URI>();
        if(chunksResourceURIs != null) {
            for(URI chunksResourceURI : chunksResourceURIs) {
                newResourceURIs.add(chunksResourceURI);
            }
        }
        
        newResourceURIs.add(resourceURI);

        // update
        this.chunks.put(hash, newResourceURIs.toArray(new URI[0]));
    }
    
    private synchronized void removeChunk(String hash, URI resourceURI) {
        URI[] chunksResourceURIs = this.chunks.remove(hash);
        
        ArrayList<URI> newResourceURIs = new ArrayList<URI>();
        if(chunksResourceURIs != null) {
            for(URI chunksResourceURI : chunksResourceURIs) {
                if(!chunksResourceURI.equals(resourceURI)) {
                    newResourceURIs.add(chunksResourceURI);
                }
            }
        }

        // update
        if(!newResourceURIs.isEmpty()) {
            this.chunks.put(hash, newResourceURIs.toArray(new URI[0]));
        }
    }
    
    public synchronized Chunk getChunk(String hash) {
        URI[] resourceURIs = this.chunks.get(hash.toLowerCase());
        if(resourceURIs != null) {
            for(URI resourceURI : resourceURIs) {
                LocalRecipe recipe = this.recipes.get(resourceURI);
                if(recipe != null) {
                    RecipeChunk chunk = recipe.getChunk(hash.toLowerCase());
                    if(chunk != null) {
                        return chunk.toChunk(recipe.getResourcePath());
                    }
                }
            }
        }
        return null;
    }
    
    public synchronized Chunk getChunk(byte[] hash) {
        String hashString = DataFormatUtils.toHexString(hash).toLowerCase();
        return getChunk(hashString);
    }
    
    @Override
    public synchronized String toString() {
        return "RecipeManager";
    }

    private class BackgroundWorker implements Runnable {
        
        private int chunkSize;
        
        public BackgroundWorker() {
            chunkSize = config.getChunkSize();
        }
        
        @Override
        public void run() {
            if(pendingRecipes.size() > 0) {
                try {
                    LocalRecipe recipe = pendingRecipes.peekEntry();
                    LocalClusterManager lcm = LocalClusterManager.getInstance();
                    ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(recipe.getResourcePath(), this.chunkSize);
                    
                    recipe = pendingRecipes.get(recipe.getResourcePath());
                    LOG.info("Hashing a recipe of " + recipe.getResourcePath().toASCIIString());

                    recipeGenerator.hashRecipe(recipe);
                    recipes.put(recipe.getResourcePath(), recipe);
                    Collection<RecipeChunk> allChunk = recipe.getAllChunks();
                    for (RecipeChunk chunk : allChunk) {
                        addChunk(chunk.getHashString(), recipe.getResourcePath());
                    }
                } catch (IOException ex) {
                    LOG.error(ex);
                } catch (NoSuchAlgorithmException ex) {
                    LOG.error(ex);
                }
            }
        }
    }
}
