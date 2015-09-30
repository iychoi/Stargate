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
package edu.arizona.cs.stargate.recipe;

import edu.arizona.cs.stargate.cluster.ClusterManager;
import edu.arizona.cs.stargate.common.utils.DateTimeUtils;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.datastore.ADistributedDataStore;
import edu.arizona.cs.stargate.datastore.AReplicatedDataStore;
import edu.arizona.cs.stargate.datastore.DataStoreManager;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.sourcefs.SourceFileSystemManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManager {

    private static final Log LOG = LogFactory.getLog(RecipeManager.class);
    
    private static final String RECIPEMANAGER_RECIPE_MAP_ID = "RecipeManager_Recipe";
    private static final String RECIPEMANAGER_HASH_MAP_ID = "RecipeManager_Hash";
    
    private static RecipeManager instance;
    
    private DataStoreManager dataStoreManager;
    private ClusterManager clusterManager;
    private RecipeGeneratorManager recipeGeneratorManager;
    private SourceFileSystemManager sourceFileSystemManager;
    private DataExportManager dataExportManager;
    
    private AReplicatedDataStore recipe;
    private ADistributedDataStore hash;
    
    private DataExportChangedEventHandler dataExportChangedHandler;
    protected long lastUpdateTime;
    
    public static RecipeManager getInstance(DataStoreManager dataStoreManager, RecipeGeneratorManager recipeGeneratorManager, SourceFileSystemManager sourceFileSystemManager, ClusterManager clusterManager, DataExportManager dataExportManager) {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager(dataStoreManager, recipeGeneratorManager, sourceFileSystemManager, clusterManager, dataExportManager);
            }
            return instance;
        }
    }
    
    public static RecipeManager getInstance() throws ServiceNotStartedException {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("VolumeManager is not started");
            }
            return instance;
        }
    }
    
    RecipeManager(DataStoreManager dataStoreManager, RecipeGeneratorManager recipeGeneratorManager, SourceFileSystemManager sourceFileSystemManager, ClusterManager clusterManager, DataExportManager dataExportManager) {
        if(dataStoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is null");
        }
        
        if(recipeGeneratorManager == null) {
            throw new IllegalArgumentException("recipeGeneratorManager is null");
        }
        
        if(sourceFileSystemManager == null) {
            throw new IllegalArgumentException("sourceFileSystemManager is null");
        }
        
        if(clusterManager == null) {
            throw new IllegalArgumentException("clusterManager is null");
        }
        
        if(dataExportManager == null) {
            throw new IllegalArgumentException("dataExportManager is null");
        }
        
        this.dataStoreManager = dataStoreManager;
        this.recipeGeneratorManager = recipeGeneratorManager;
        this.sourceFileSystemManager = sourceFileSystemManager;
        this.clusterManager = clusterManager;
        this.dataExportManager = dataExportManager;
        
        this.recipe = this.dataStoreManager.getReplicatedDataStore(RECIPEMANAGER_RECIPE_MAP_ID, String.class, Recipe.class);
        this.hash = this.dataStoreManager.getDistributedDataStore(RECIPEMANAGER_HASH_MAP_ID, String.class, RecipeList.class);
        
        this.dataExportChangedHandler = new DataExportChangedEventHandler(this.sourceFileSystemManager, this.recipeGeneratorManager, this.clusterManager, this);
        this.dataExportManager.addEventHandler(this.dataExportChangedHandler);
    }
    
    public synchronized int getRecipeCount() {
        return this.recipe.size();
    }
    
    public synchronized Collection<Recipe> getRecipe() throws IOException {
        List<Recipe> recipeList = new ArrayList<Recipe>();
        Set<Object> recipeKeySet = this.recipe.keySet();
        for(Object recipeKey : recipeKeySet) {
            Recipe recipe = (Recipe)this.recipe.get((String) recipeKey);
            recipeList.add(recipe);
        }
        
        return Collections.unmodifiableCollection(recipeList);
    }
    
    public synchronized Recipe getRecipe(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        return (Recipe)this.recipe.get(path.toString());
    }
    
    public synchronized Recipe getRecipe(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        RecipeList list = (RecipeList)this.hash.get(hash);
        if(list != null) {
            for(DataObjectPath path : list.getList()) {
                Recipe recipe = getRecipe(path);
                if(recipe != null) {
                    return recipe;
                }
            }
        }
        return null;
    }
    
    public synchronized void addRecipe(Recipe recipe) throws IOException {
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        this.recipe.put(recipe.getMetadata().getPath().toString(), recipe);
        for(RecipeChunk chunk : recipe.getChunk()) {
            String hashString = chunk.getHashString();
            RecipeList list = (RecipeList)this.hash.get(hashString);
            if(list == null) {
                // create new
                list = new RecipeList();
            }
            
            list.addList(recipe.getMetadata().getPath());
            this.hash.put(hashString, list);
        }
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }

    public synchronized void removeRecipe(Recipe recipe) throws IOException {
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        removeRecipe(recipe.getMetadata().getPath());
    }
    
    public synchronized void removeRecipe(DataObjectPath path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Recipe recipe = (Recipe)this.recipe.get(path.toString());
        if(recipe != null) {
            for(RecipeChunk chunk : recipe.getChunk()) {
                String hashString = chunk.getHashString();
                RecipeList list = (RecipeList)this.hash.get(hashString);
                if(list != null) {
                    list.removeList(recipe.getMetadata().getPath());
                }

                this.hash.put(hashString, list);
            }
            
            this.recipe.remove(path.toString());
            this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        }
    }
    
    public synchronized void updateRecipe(Recipe recipe) throws IOException {
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        Recipe existing_recipe = (Recipe)this.recipe.get(recipe.getMetadata().getPath().toString());
        if(existing_recipe != null) {
            for(RecipeChunk chunk : existing_recipe.getChunk()) {
                String hashString = chunk.getHashString();
                RecipeList list = (RecipeList)this.hash.get(hashString);
                if(list != null) {
                    list.removeList(recipe.getMetadata().getPath());
                }

                this.hash.put(hashString, list);
            }
            
            this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        }
        
        this.recipe.put(recipe.getMetadata().getPath().toString(), recipe);
        for(RecipeChunk chunk : recipe.getChunk()) {
            String hashString = chunk.getHashString();
            RecipeList list = (RecipeList)this.hash.get(hashString);
            if(list == null) {
                // create new
                list = new RecipeList();
            }
            
            list.addList(recipe.getMetadata().getPath());
            this.hash.put(hashString, list);
        }
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }

    public synchronized boolean hasRecipe(DataObjectPath path) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        return this.recipe.containsKey(path.toString());
    }
    
    public synchronized boolean isEmpty() {
        if(this.recipe == null || this.recipe.isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
    
    @Override
    public synchronized String toString() {
        return "RecipeManager";
    }
}
