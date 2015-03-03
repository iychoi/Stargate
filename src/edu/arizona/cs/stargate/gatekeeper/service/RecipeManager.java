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

import edu.arizona.cs.stargate.common.recipe.MemoryRecipeStore;
import edu.arizona.cs.stargate.common.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.common.recipe.ARecipeGenerator;
import edu.arizona.cs.stargate.common.recipe.ARecipeStore;
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.Recipe;
import edu.arizona.cs.stargate.common.recipe.RecipeGeneratorFactory;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManager {
    private static final Log LOG = LogFactory.getLog(RecipeManager.class);
    
    private static RecipeManager instance;

    static RecipeManager getInstance() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private RecipeManagerConfiguration config;
    private BlockingQueue<Recipe> pendingRecipes = new LinkedBlockingQueue<Recipe>();
    //private Map<URI, Boolean> removedRecipes = new HashMap<URI, Boolean>();
    private ARecipeStore recipeStore;
    
    // check message and start hashing
    private ScheduledExecutorService backgroundWorker;
    
    public static RecipeManager getInstance(RecipeManagerConfiguration config) {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager(config);
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
        
        this.backgroundWorker = Executors.newSingleThreadScheduledExecutor();
        this.backgroundWorker.scheduleAtFixedRate(new BackgroundWorker(), 0, 1, TimeUnit.MINUTES);
        this.recipeStore = new MemoryRecipeStore();
    }
    
    public RecipeManagerConfiguration getConfiguration() {
        return this.config;
    }
    
    public synchronized void prepareRecipe(DataExportInfo info) {
        Collection<DataExportEntry> entries = info.getAllExportEntry();
        for(DataExportEntry entry : entries) {
            prepareRecipe(entry.getResourcePath());
        }
    }
    
    public synchronized void prepareRecipe(URI resourceUri) {
        try {
            if(!this.recipeStore.hasRecipe(resourceUri)) {
                ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(resourceUri, this.config.getChunkSize());
                Recipe recipe = recipeGenerator.generateRecipeWithoutHash(resourceUri, this.config.getHashAlgorithm());
                this.pendingRecipes.offer(recipe);
                this.recipeStore.store(recipe);
            }
        } catch(Exception ex) {
            LOG.error(ex);
        }
    }
    
    public synchronized Recipe getRecipe(URI resourceUri) {
        Recipe recipe = this.recipeStore.get(resourceUri);
        if(recipe == null) {
            prepareRecipe(resourceUri);
            return this.recipeStore.get(resourceUri);
        } else {
            return recipe;
        }
    }
    
    public synchronized void removeRecipe(DataExportInfo info) {
        Collection<DataExportEntry> entries = info.getAllExportEntry();
        for(DataExportEntry entry : entries) {
            removeRecipe(entry.getResourcePath());
        }
    }
    
    public synchronized void removeRecipe(URI resourceUri) {
        this.recipeStore.remove(resourceUri);
        //if(!this.removedRecipes.containsKey(resourceUri)) {
        //    this.removedRecipes.put(resourceUri, true);
        //}
    }
    
    public synchronized void removeAllRecipe() {
        this.recipeStore.removeAll();
        //if(!this.removedRecipes.containsKey(resourceUri)) {
        //    this.removedRecipes.put(resourceUri, true);
        //}
    }
    
    public synchronized ChunkInfo findChunk(String hash) {
        return this.recipeStore.find(hash);
    }
    
    public synchronized ChunkInfo findChunk(byte[] hash) {
        return this.recipeStore.find(hash);
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
            try {
                while(true) {
                    try {
                        Recipe recipe = pendingRecipes.take();
                        //if(removedRecipes.containsKey(recipe.getResourcePath())) {
                        //    removedRecipes.remove(recipe.getResourcePath());
                        //} else {
                            ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(recipe.getResourcePath(), this.chunkSize);
                            recipeGenerator.hashRecipe(recipe);
                            recipeStore.notifyRecipeHashed(recipe);
                        //}
                    } catch (IOException ex) {
                        LOG.error(ex);
                    } catch (NoSuchAlgorithmException ex) {
                        LOG.error(ex);
                    }
                }
            } catch (InterruptedException ex) {
                LOG.debug("BackgroundWorker thread is interrupted");
            }
        }
    }
}
