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

package edu.arizona.cs.stargate.gatekeeper.service;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import edu.arizona.cs.stargate.cache.service.JsonIMap;
import edu.arizona.cs.stargate.cache.service.JsonMultiMap;
import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.common.recipe.ARecipeGenerator;
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.FixedSizeLocalFileRecipeGenerator;
import edu.arizona.cs.stargate.common.recipe.LocalClusterRecipe;
import edu.arizona.cs.stargate.common.recipe.RecipeChunkInfo;
import edu.arizona.cs.stargate.common.recipe.RecipeGeneratorFactory;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.service.StargateService;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
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
    
    private JsonIMap<URI, LocalClusterRecipe> recipes;
    private JsonIMap<URI, LocalClusterRecipe> pendingRecipes;
    private JsonMultiMap<String, ChunkInfo> chunkinfo;
    
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
            this.recipes = new JsonIMap<URI, LocalClusterRecipe>(StargateService.getInstance().getDistributedCacheService().getDistributedMap(RECIPEMANAGER_RECIPES_MAP_ID), LocalClusterRecipe.class);
            this.pendingRecipes = new JsonIMap<URI, LocalClusterRecipe>(StargateService.getInstance().getDistributedCacheService().getDistributedMap(RECIPEMANAGER_PENDING_RECIPES_MAP_ID), LocalClusterRecipe.class);
            this.pendingRecipes.addEntryListener(new EntryListener<URI, LocalClusterRecipe>(){

                @Override
                public void entryAdded(EntryEvent<URI, LocalClusterRecipe> ee) {
                    Runnable worker = new BackgroundWorker(); 
                    backgroundWorker.execute(worker);
                }

                @Override
                public void entryRemoved(EntryEvent<URI, LocalClusterRecipe> ee) {
                }

                @Override
                public void entryUpdated(EntryEvent<URI, LocalClusterRecipe> ee) {
                }

                @Override
                public void entryEvicted(EntryEvent<URI, LocalClusterRecipe> ee) {
                }

                @Override
                public void mapEvicted(MapEvent me) {
                }

                @Override
                public void mapCleared(MapEvent me) {
                }
            }, true);
            
            this.chunkinfo = new JsonMultiMap<String, ChunkInfo>(StargateService.getInstance().getDistributedCacheService().getDistributedMultiMap(RECIPEMANAGER_CHUNKINFO_MAP_ID), ChunkInfo.class);
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            throw new RuntimeException(ex);
        }
        
        this.backgroundWorker = Executors.newFixedThreadPool(RECIPE_BACKGROUND_WORKER_THREADS);
    }
    
    public RecipeManagerConfiguration getConfiguration() {
        return this.config;
    }
    
    public synchronized LocalClusterRecipe prepareRecipe(DataExportInfo export) {
        URI resourceUri = export.getResourcePath();
        return prepareRecipe(resourceUri);
    }
    
    public synchronized LocalClusterRecipe prepareRecipe(URI resourceUri) {
        try {
            if(!this.recipes.containsKey(resourceUri) && 
                    !this.pendingRecipes.containsKey(resourceUri)) {
                LOG.info("Preparing a new recipe of " + resourceUri.toASCIIString());
                ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(resourceUri, this.config.getChunkSize());
                LocalClusterRecipe recipe = recipeGenerator.generateRecipeWithoutHash(resourceUri, this.config.getHashAlgorithm());
                this.pendingRecipes.put(resourceUri, recipe);
                return recipe;
            }
        } catch(Exception ex) {
            LOG.error(ex);
            return null;
        }
        return null;
    }
    
    public synchronized LocalClusterRecipe getRecipe(URI resourceUri) {
        LocalClusterRecipe recipe;
        recipe = this.recipes.get(resourceUri);
        if(recipe != null) {
            return recipe;
        }
        
        recipe = this.pendingRecipes.get(resourceUri);
        if(recipe != null) {
            return recipe;
        }
        
        return prepareRecipe(resourceUri);
    }
    
    public synchronized void removeRecipe(DataExportInfo export) {
        removeRecipe(export.getResourcePath());
    }
    
    public synchronized void removeRecipe(URI resourceUri) {
        LocalClusterRecipe recipe1 = this.recipes.remove(resourceUri);
        LocalClusterRecipe recipe2 = this.pendingRecipes.remove(resourceUri);
        
        LocalClusterRecipe recipe = null;
        if(recipe1 != null) {
            recipe = recipe1;
        }
        if(recipe2 != null) {
            recipe = recipe2;
        }
        if(recipe != null) {
            Collection<RecipeChunkInfo> allChunk = recipe.getAllChunk();
            for(RecipeChunkInfo info : allChunk) {
                Collection<ChunkInfo> removedChunks = this.chunkinfo.remove(info.getHashString());
                for(ChunkInfo chunk : removedChunks) {
                    if(!chunk.getResourcePath().equals(recipe.getResourcePath())) {
                        // other's 
                        this.chunkinfo.put(chunk.getHashString(), chunk);
                    }
                }
            }
        }
    }
    
    public synchronized void removeAllRecipe() {
        this.recipes.clear();
        this.pendingRecipes.clear();
        this.chunkinfo.clear();
    }
    
    public synchronized ChunkInfo findChunk(String hash) {
        Collection<ChunkInfo> chunks = this.chunkinfo.get(hash);
        Iterator<ChunkInfo> iterator = chunks.iterator();
        while(iterator.hasNext()) {
            ChunkInfo chunk = iterator.next();
            if(this.recipes.containsKey(chunk.getResourcePath())) {
                return chunk;
            }
        }
        return null;
    }
    
    public synchronized ChunkInfo findChunk(byte[] hash) {
        String hashString = DataFormatter.toHexString(hash);
        return findChunk(hashString);
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
                    LocalClusterRecipe recipe = pendingRecipes.peekEntry();
                    LocalClusterManager lcm = LocalClusterManager.getInstance();
                    ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(recipe.getResourcePath(), this.chunkSize);
                    if(recipeGenerator instanceof FixedSizeLocalFileRecipeGenerator) {
                        LOG.info("local file recipe generation " + recipe.getResourcePath());
                        boolean local = true;
                        Collection<RecipeChunkInfo> chunks = recipe.getAllChunk();
                        Iterator<RecipeChunkInfo> iterator = chunks.iterator();
                        while(iterator.hasNext()) {
                            RecipeChunkInfo chunk = iterator.next();
                            boolean ownerhost = false;
                            String[] ownerHosts = chunk.getOwnerHost();
                            for(String ownerHost : ownerHosts) {
                                if(lcm.getLocalNode().getName().equals(ownerHost)) {
                                    ownerhost = true;
                                }
                            }
                            
                            if(!ownerhost) {
                                local = false;
                                break;
                            }
                        }
                        
                        if(local) {
                            recipe = pendingRecipes.get(recipe.getResourcePath());
                            LOG.info("Hashing a recipe of " + recipe.getResourcePath().toASCIIString());
                            
                            recipeGenerator.hashRecipe(recipe);
                            recipes.put(recipe.getResourcePath(), recipe);
                            Collection<RecipeChunkInfo> allChunk = recipe.getAllChunk();
                            for (RecipeChunkInfo chunk : allChunk) {
                                chunkinfo.put(chunk.getHashString(), chunk.toChunk(recipe.getResourcePath()));
                            }
                        } else {
                            LOG.info("Ignoring hashing " + recipe.getResourcePath().toASCIIString());
                        }
                    } else {
                        LOG.info("hdfs file recipe generation " + recipe.getResourcePath());
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
