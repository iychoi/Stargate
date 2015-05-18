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

import edu.arizona.cs.stargate.gatekeeper.distributed.JsonIMap;
import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExportManager;
import edu.arizona.cs.stargate.gatekeeper.dataexport.IDataExportConfigurationChangeEventHandler;
import edu.arizona.cs.stargate.gatekeeper.distributed.DistributedService;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManager {
    private static final Log LOG = LogFactory.getLog(RecipeManager.class);
    
    private static final String RECIPEMANAGER_RECIPES_MAP_ID = "RecipeManager_Recipes";
    private static final String RECIPEMANAGER_CHUNKINFO_MAP_ID = "RecipeManager_Chunkinfo";
    private static final String RECIPEMANAGER_INCOMPLETE_RECIPES_MAP_ID = "RecipeManager_Incomplete_Recipes";
    
    private static RecipeManager instance;

    private RecipeManagerConfiguration config;
    private DistributedService distributedService;
    private LocalClusterManager localClusterManager;
    private DataExportManager dataExportManager;
    
    private JsonIMap<URI, LocalRecipe> recipes;
    private JsonIMap<URI, LocalRecipe> incompleteRecipes;
    private JsonIMap<String, URI[]> chunks;
    
    public static RecipeManager getInstance(RecipeManagerConfiguration config, DistributedService distributedService, LocalClusterManager localClusterManager, DataExportManager dataExportManager) {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager(config, distributedService, localClusterManager, dataExportManager);
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
    
    RecipeManager(RecipeManagerConfiguration config, DistributedService distributedService, LocalClusterManager localClusterManager, DataExportManager dataExportManager) {
        if(config == null) {
            this.config = new RecipeManagerConfiguration();
        } else {
            this.config = config;
            this.config.setImmutable();
        }
        
        this.distributedService = distributedService;
        this.localClusterManager = localClusterManager;
        this.dataExportManager = dataExportManager;
        
        this.recipes = new JsonIMap<URI, LocalRecipe>(this.distributedService.getDistributedMap(RECIPEMANAGER_RECIPES_MAP_ID), LocalRecipe.class);
        this.incompleteRecipes = new JsonIMap<URI, LocalRecipe>(this.distributedService.getDistributedMap(RECIPEMANAGER_INCOMPLETE_RECIPES_MAP_ID), LocalRecipe.class);
        this.chunks = new JsonIMap<String, URI[]>(this.distributedService.getDistributedMap(RECIPEMANAGER_CHUNKINFO_MAP_ID), URI[].class);

        // register eventhandler
        this.dataExportManager.addConfigChangeEventHandler(new IDataExportConfigurationChangeEventHandler(){

            @Override
            public String getName() {
                return "RecipeManager";
            }

            @Override
            public void addDataExport(DataExportManager manager, DataExport info) {
                generateRecipe(info);
            }

            @Override
            public void removeDataExport(DataExportManager manager, DataExport info) {
                removeRecipe(info);
            }
        });
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
                    !this.incompleteRecipes.containsKey(resourceUri)) {
                LOG.info("Generating a new recipe of " + resourceUri.toASCIIString());
                ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(this.localClusterManager, resourceUri, this.config.getChunkSize());
                LocalRecipe recipe = recipeGenerator.generateRecipe(resourceUri, this.config.getHashAlgorithm());
                this.incompleteRecipes.put(resourceUri, recipe);
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
        
        recipe = this.incompleteRecipes.get(resourceUri);
        if(recipe != null) {
            return recipe;
        }
        
        return generateRecipe(resourceUri);
    }
    
    public synchronized Collection<LocalRecipe> getIncompleteRecipes() {
         return this.incompleteRecipes.values();
    }
    
    public synchronized void removeRecipe(DataExport export) {
        removeRecipe(export.getResourcePath());
    }
    
    public synchronized void removeRecipe(URI resourceUri) {
        LocalRecipe recipe1 = this.recipes.remove(resourceUri);
        LocalRecipe recipe2 = this.incompleteRecipes.remove(resourceUri);
        
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
        this.incompleteRecipes.clear();
        this.chunks.clear();
    }
    
    public synchronized void completeRecipe(LocalRecipe recipe) {
        this.recipes.put(recipe.getResourcePath(), recipe);
        Collection<RecipeChunk> allChunk = recipe.getAllChunks();
        for (RecipeChunk chunk : allChunk) {
            addChunk(chunk.getHashString(), recipe.getResourcePath());
        }
        this.incompleteRecipes.remove(recipe.getResourcePath());
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
}
