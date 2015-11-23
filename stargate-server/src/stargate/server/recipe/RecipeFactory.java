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
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.recipe.ARecipeGenerator;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.commons.sourcefs.ASourceFileSystem;
import stargate.commons.sourcefs.SourceFileMetadata;
import stargate.server.cluster.LocalClusterManager;
import stargate.server.sourcefs.SourceFileSystemManager;

/**
 *
 * @author iychoi
 */
public class RecipeFactory {

    public static DataObjectPath createDataObjectPath(LocalClusterManager localClusterManager, DataExportEntry dataExportEntry) {
        if(localClusterManager == null || localClusterManager.isEmpty()) {
            throw new IllegalArgumentException("localClusterManager is null");
        }
        
        if(dataExportEntry == null) {
            throw new IllegalArgumentException("dataExportEntry is null");
        }
        
        return new DataObjectPath(localClusterManager.getName(), dataExportEntry.getVirtualPath());
    }
    
    public static DataObjectMetadata createDataObjectMetadata(SourceFileSystemManager sourceFileSystemManager, LocalClusterManager localClusterManager, DataExportEntry dataExportEntry) throws IOException {
        if(sourceFileSystemManager == null) {
            throw new IllegalArgumentException("sourceFileSystemManager is null");
        }
        
        if(localClusterManager == null) {
            throw new IllegalArgumentException("localClusterManager is null");
        }
        
        if(dataExportEntry == null) {
            throw new IllegalArgumentException("dataExportEntry is null");
        }
        
        DataObjectPath dataObjectPath = createDataObjectPath(localClusterManager, dataExportEntry);
        ASourceFileSystem sourceFileSystem = sourceFileSystemManager.getFileSystem();
        SourceFileMetadata metadata = sourceFileSystem.getMetadata(dataExportEntry.getResourcePath());
        return new DataObjectMetadata(dataObjectPath, metadata.getFileSize(), false, metadata.getLastModificationTime());
    }
    
    public static Recipe createRecipe(SourceFileSystemManager sourceFileSystemManager, RecipeGeneratorManager recipeGeneratorManager, LocalClusterManager localClusterManager, DataExportEntry dataExportEntry) throws IOException {
        if(sourceFileSystemManager == null) {
            throw new IllegalArgumentException("sourceFileSystemManager is null");
        }
        
        if(recipeGeneratorManager == null) {
            throw new IllegalArgumentException("recipeGeneratorManager is null");
        }
        
        if(localClusterManager == null) {
            throw new IllegalArgumentException("localClusterManager is null");
        }
        
        if(dataExportEntry == null) {
            throw new IllegalArgumentException("dataExportEntry is null");
        }
        
        DataObjectPath dataObjectPath = createDataObjectPath(localClusterManager, dataExportEntry);
        ASourceFileSystem sourceFileSystem = sourceFileSystemManager.getFileSystem();
        SourceFileMetadata metadata = sourceFileSystem.getMetadata(dataExportEntry.getResourcePath());
        DataObjectMetadata dataObjectMetadata = new DataObjectMetadata(dataObjectPath, metadata.getFileSize(), false, metadata.getLastModificationTime());
        
        ARecipeGenerator recipeGenerator = recipeGeneratorManager.getRecipeGenerator();
        return recipeGenerator.getRecipe(dataObjectMetadata, sourceFileSystem.getInputStream(dataExportEntry.getResourcePath()));
    }
}
