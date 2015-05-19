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
import edu.arizona.cs.stargate.gatekeeper.schedule.ALeaderScheduledTask;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeChunkHashTask extends ALeaderScheduledTask {

    private static final Log LOG = LogFactory.getLog(RecipeChunkHashTask.class);
    
    private static final int RECIPE_CHUNK_HASH_PERIOD_SEC = 20;
    
    private LocalClusterManager localClusterManager;
    private LocalRecipeManager localRecipeManager;
    
    public RecipeChunkHashTask(LocalClusterManager localClusterManager, LocalRecipeManager localRecipeManager) {
        this.localClusterManager = localClusterManager;
        this.localRecipeManager = localRecipeManager;
    }
    
    @Override
    public void process() {
        LOG.info("Start - RecipeChunkHashTask");
        
        Collection<LocalRecipe> incompleteRecipes = this.localRecipeManager.getAllIncompleteRecipes();
        if(incompleteRecipes != null && incompleteRecipes.size() > 0) {
            for(LocalRecipe recipe : incompleteRecipes) {
                try {
                    ARecipeGenerator recipeGenerator = RecipeGeneratorFactory.getRecipeGenerator(this.localClusterManager, recipe);
                    LOG.info("Hashing a recipe of " + recipe.getResourcePath().toASCIIString());
                    
                    recipeGenerator.hashRecipe(recipe);
                    this.localRecipeManager.completeRecipeHash(recipe);
                    this.localRecipeManager.setUpdated(true);
                } catch (IOException ex) {
                    LOG.error(ex);
                } catch (NoSuchAlgorithmException ex) {
                    LOG.error(ex);
                }
            }
        }
        
        LOG.info("Done - RecipeChunkHashTask");
    }

    @Override
    public String getName() {
        return "RecipeChunkHashTask";
    }

    @Override
    public boolean isRepeatedTask() {
        return true;
    }

    @Override
    public long getDelay() {
        return 0;
    }

    @Override
    public long getPeriod() {
        return RECIPE_CHUNK_HASH_PERIOD_SEC;
    }
    
}
