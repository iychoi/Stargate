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
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.cluster.RemoteClusterManager;
import edu.arizona.cs.stargate.gatekeeper.distributed.DistributedService;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RemoteRecipeManager {
    private static final Log LOG = LogFactory.getLog(RemoteRecipeManager.class);
    
    private static final String REMOTERECIPEMANAGER_RECIPES_MAP_ID = "RemoteRecipeManager_Recipes";
    
    private static RemoteRecipeManager instance;

    private DistributedService distributedService;
    private RemoteClusterManager remoteClusterManager;
    
    private JsonIMap<String, RemoteRecipe> recipes;
    
    public static RemoteRecipeManager getInstance(DistributedService distributedService, RemoteClusterManager remoteClusterManager) {
        synchronized (RemoteRecipeManager.class) {
            if(instance == null) {
                instance = new RemoteRecipeManager(distributedService, remoteClusterManager);
            }
            return instance;
        }
    }
    
    public static RemoteRecipeManager getInstance() throws ServiceNotStartedException {
        synchronized (RemoteRecipeManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("RemoteRecipeManager is not started");
            }
            return instance;
        }
    }
    
    RemoteRecipeManager(DistributedService distributedService, RemoteClusterManager remoteClusterManager) {
        this.distributedService = distributedService;
        this.remoteClusterManager = remoteClusterManager;
        
        this.recipes = new JsonIMap<String, RemoteRecipe>(this.distributedService.getDistributedMap(REMOTERECIPEMANAGER_RECIPES_MAP_ID), RemoteRecipe.class);
    }
    
    public Collection<RemoteRecipe> getAllRecipes() {
        return this.recipes.values();
    }
    
    public synchronized RemoteRecipe getRecipe(String virtualPath) {
        RemoteRecipe recipe;
        recipe = this.recipes.get(virtualPath);
        return recipe;
    }
    
    public synchronized void removeRecipe(RemoteRecipe recipe) {
        removeRecipe(recipe.getVirtualPath());
    }
    
    public synchronized void removeRecipe(String virtualPath) {
        this.recipes.remove(virtualPath);
    }
    
    public synchronized void removeAllRecipes() {
        this.recipes.clear();
    }
    
    public synchronized void updateRecipe(RemoteRecipe recipe) {
        RemoteRecipe existingRecipe = this.recipes.get(recipe.getVirtualPath());
        if(existingRecipe != null) {
            if(existingRecipe.getModificationTime() < recipe.getModificationTime()) {
                this.recipes.remove(recipe.getVirtualPath());
                this.recipes.put(recipe.getVirtualPath(), recipe);
            }
        } else {
            this.recipes.put(recipe.getVirtualPath(), recipe);
        }
    }
    
    @Override
    public synchronized String toString() {
        return "RemoteRecipeManager";
    }
}
