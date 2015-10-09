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
package edu.arizona.cs.stargate.tasks;

import edu.arizona.cs.stargate.cluster.ClusterManager;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.schedule.AScheduledLeaderTask;
import edu.arizona.cs.stargate.policy.PolicyManager;
import edu.arizona.cs.stargate.policy.VolumePolicy;
import edu.arizona.cs.stargate.recipe.DataObjectMetadata;
import edu.arizona.cs.stargate.recipe.DataObjectPath;
import edu.arizona.cs.stargate.recipe.Recipe;
import edu.arizona.cs.stargate.recipe.RecipeFactory;
import edu.arizona.cs.stargate.recipe.RecipeGeneratorManager;
import edu.arizona.cs.stargate.recipe.RecipeManager;
import edu.arizona.cs.stargate.sourcefs.SourceFileSystemManager;
import java.io.IOException;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeSyncTask extends AScheduledLeaderTask {

    private static final Log LOG = LogFactory.getLog(RecipeSyncTask.class);
    
    private PolicyManager policyManager;
    private SourceFileSystemManager sourceFileSystemManager;
    private ClusterManager clusterManager;
    private DataExportManager dataExportManager;
    private RecipeManager recipeManager;
    private RecipeGeneratorManager recipeGeneratorManager;
    
    private long syncInterval;
    
    public RecipeSyncTask(PolicyManager policyManager, SourceFileSystemManager sourceFileSystemManager, ClusterManager clusterManager, DataExportManager dataExportManager, RecipeManager recipeManager, RecipeGeneratorManager recipeGeneratorManager) throws IOException {
        if(policyManager == null) {
            throw new IllegalArgumentException("policyManager is null");
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
        
        if(recipeManager == null) {
            throw new IllegalArgumentException("recipeManager is null");
        }
        
        if(recipeGeneratorManager == null) {
            throw new IllegalArgumentException("recipeGeneratorManager is null");
        }
        
        this.policyManager = policyManager;
        this.sourceFileSystemManager = sourceFileSystemManager;
        this.clusterManager = clusterManager;
        this.dataExportManager = dataExportManager;
        this.recipeManager = recipeManager;
        this.recipeGeneratorManager = recipeGeneratorManager;
        
        this.syncInterval = VolumePolicy.getLocalClusterRecipeSyncPeriod(this.policyManager);
    }
    
    @Override
    public void run() {
        LOG.info("Start - RecipeSyncTask");

        try {
            Collection<Recipe> localRecipe = this.recipeManager.getRecipe();
            for(Recipe recipe : localRecipe) {
                try {
                    DataExportEntry dataExportEntry = this.dataExportManager.getDataExport(recipe.getMetadata().getPath().getPath());
                    if(dataExportEntry != null) {
                        // remove
                        this.recipeManager.removeRecipe(recipe);
                    } else {
                        // check updated
                        DataObjectMetadata metadata = RecipeFactory.createDataObjectMetadata(this.sourceFileSystemManager, this.clusterManager.getLocalClusterManager(), dataExportEntry);
                        if(recipe.getMetadata().getObjectSize() != metadata.getObjectSize() ||
                                recipe.getMetadata().getLastModificationTime() != metadata.getLastModificationTime()) {
                            // updated
                            Recipe newRecipe = RecipeFactory.createRecipe(this.sourceFileSystemManager, this.recipeGeneratorManager, this.clusterManager.getLocalClusterManager(), dataExportEntry);
                            this.recipeManager.updateRecipe(newRecipe);
                        }
                    }
                } catch (IOException ex) {
                    LOG.error(ex);
                    // remove
                    this.recipeManager.removeRecipe(recipe);
                }
            }
            
            Collection<DataExportEntry> dataExportEntry = this.dataExportManager.getDataExport();
            for(DataExportEntry entry : dataExportEntry) {
                try {
                    DataObjectPath dataObjectPath = RecipeFactory.createDataObjectPath(this.clusterManager.getLocalClusterManager(), entry);
                    if(!this.recipeManager.hasRecipe(dataObjectPath)) {
                        // add
                        Recipe newRecipe = RecipeFactory.createRecipe(this.sourceFileSystemManager, this.recipeGeneratorManager, this.clusterManager.getLocalClusterManager(), entry);
                        this.recipeManager.addRecipe(newRecipe);
                    }
                } catch (IOException ex) {
                    LOG.error(ex);
                }
            }
        } catch (IOException ex) {
            LOG.error(ex);
        }
            
        LOG.info("Done - RecipeSyncTask");
    }
    
    @Override
    public String getName() {
        return "RecipeSyncTask";
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
    public long getInterval() {
        return this.syncInterval;
    }
}
