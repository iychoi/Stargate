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
package stargate.server.recipe;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.server.cluster.ClusterManager;
import stargate.server.dataexport.DataExportManager;
import stargate.server.dataexport.IDataExportEventHandler;
import stargate.server.sourcefs.SourceFileSystemManager;

/**
 *
 * @author iychoi
 */
public class DataExportChangedEventHandler implements IDataExportEventHandler {
    
    private static final Log LOG = LogFactory.getLog(DataExportChangedEventHandler.class);
    
    private SourceFileSystemManager sourceFileSystemManager;
    private RecipeGeneratorManager recipeGeneratorManager;
    private ClusterManager clusterManager;
    private RecipeManager recipeManager;

    public DataExportChangedEventHandler(SourceFileSystemManager sourceFileSystemManager, RecipeGeneratorManager recipeGeneratorManager, ClusterManager clusterManager, RecipeManager recipeManager) {
        if(sourceFileSystemManager == null) {
            throw new IllegalArgumentException("sourceFileSystemManager is null");
        }
        
        if(recipeGeneratorManager == null) {
            throw new IllegalArgumentException("recipeGeneratorManager is null");
        }
        
        if(clusterManager == null) {
            throw new IllegalArgumentException("clusterManager is null");
        }
        
        if(recipeManager == null) {
            throw new IllegalArgumentException("recipeManager is null");
        }
        
        this.sourceFileSystemManager = sourceFileSystemManager;
        this.recipeGeneratorManager = recipeGeneratorManager;
        this.clusterManager = clusterManager;
        this.recipeManager = recipeManager;
    }
    
    @Override
    public String getName() {
        return "DataExportChangedEventHandler";
    }

    @Override
    public void dataExportEntryAdded(DataExportManager manager, DataExportEntry entry) {
        try {
            // generate recipe
            Recipe recipe = RecipeFactory.createRecipe(this.sourceFileSystemManager, this.recipeGeneratorManager, this.clusterManager.getLocalClusterManager(), entry);
            this.recipeManager.addRecipe(recipe);
        } catch (IOException ex) {
            LOG.error("Exception occurred while adding a data export entry", ex);
        }
    }

    @Override
    public void dataExportEntryRemoved(DataExportManager manager, DataExportEntry entry) {
        try {
            DataObjectPath path = RecipeFactory.createDataObjectPath(this.clusterManager.getLocalClusterManager(), entry);
            this.recipeManager.removeRecipe(path);
        } catch (IOException ex) {
            LOG.error("Exception occurred while removing a data export entry", ex);
        }
    }
}
