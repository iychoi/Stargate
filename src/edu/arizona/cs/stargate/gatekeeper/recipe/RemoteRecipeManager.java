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

import edu.arizona.cs.stargate.common.PathUtils;
import edu.arizona.cs.stargate.gatekeeper.distributed.JsonIMap;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.cluster.RemoteClusterManager;
import edu.arizona.cs.stargate.gatekeeper.distributed.DistributedService;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
    
    private boolean updated;
    
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
    
    public synchronized RemoteRecipe getRecipe(String clusterName, String virtualPath) {
        RemoteRecipe recipe;
        recipe = this.recipes.get(PathUtils.concatPath(clusterName, virtualPath));
        return recipe;
    }
    
    public synchronized void removeRecipe(RemoteRecipe recipe) {
        removeRecipe(recipe.getClusterName(), recipe.getVirtualPath());
    }
    
    public synchronized void removeRecipe(String clusterName, String virtualPath) {
        this.recipes.remove(PathUtils.concatPath(clusterName, virtualPath));
        
        this.updated = true;
    }
    
    public synchronized void removeAllRecipes() {
        this.recipes.clear();
        
        this.updated = true;
    }
    
    public synchronized void updateRecipe(Cluster cluster, Collection<RemoteRecipe> recipes) {
        Set<String> keySet = this.recipes.keySet();
        Set<String> clusterResources = new HashSet<String>();
        for(String key : keySet) {
            if(key.startsWith(cluster.getName() + "/")) {
                clusterResources.add(key);
            }
        }
        
        if(recipes != null) {
            for(RemoteRecipe recipe : recipes) {
                String newKey = PathUtils.concatPath(recipe.getClusterName(), recipe.getVirtualPath());
                if(clusterResources.contains(newKey)) {
                    clusterResources.remove(newKey);
                }
                updateRecipe(recipe);
            }
        }
        
        // remove left resources (removed from remote cluster)
        for(String key : clusterResources) {
            this.recipes.remove(key);
        }
        
        this.updated = true;
    }
    
    public synchronized void updateRecipe(RemoteRecipe recipe) {
        RemoteRecipe existingRecipe = this.recipes.get(PathUtils.concatPath(recipe.getClusterName(), recipe.getVirtualPath()));
        if(existingRecipe != null) {
            if(existingRecipe.getModificationTime() < recipe.getModificationTime()) {
                this.recipes.remove(PathUtils.concatPath(recipe.getClusterName(), recipe.getVirtualPath()));
                this.recipes.put(PathUtils.concatPath(recipe.getClusterName(), recipe.getVirtualPath()), recipe);
            }
        } else {
            this.recipes.put(PathUtils.concatPath(recipe.getClusterName(), recipe.getVirtualPath()), recipe);
        }
        
        this.updated = true;
    }
    
    public synchronized void setUpdated(boolean updated) {
        this.updated = updated;
    }
    
    public synchronized boolean getUpdated() {
        return this.updated;
    }
    
    @Override
    public synchronized String toString() {
        return "RemoteRecipeManager";
    }
}
