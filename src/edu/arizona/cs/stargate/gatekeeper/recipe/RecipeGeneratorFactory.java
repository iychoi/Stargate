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
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class RecipeGeneratorFactory {
    
    private static final Log LOG = LogFactory.getLog(RecipeGeneratorFactory.class);
    
    private static FixedSizeHDFSFileRecipeGenerator cachedFixedSizeHDFSFileRecipeGenerator;
    
    public static ARecipeGenerator getRecipeGenerator(LocalClusterManager localClusterManager, URI resourceUri, int chunkSize) throws IOException {
        if(resourceUri.getScheme().equalsIgnoreCase("hdfs") || resourceUri.getScheme().equalsIgnoreCase("dfs")) {
            return getFixedSizeHDFSFileRecipeGenerator(localClusterManager, chunkSize);
        }
        
        throw new IOException("Unknown resource scheme");
    }
    
    public static ARecipeGenerator getRecipeGenerator(LocalClusterManager localClusterManager, LocalRecipe recipe) throws IOException {
        if(recipe.getResourcePath().getScheme().equalsIgnoreCase("hdfs") || recipe.getResourcePath().getScheme().equalsIgnoreCase("dfs")) {
            if(recipe.getChunkSize() > 0) {
                return getFixedSizeHDFSFileRecipeGenerator(localClusterManager, recipe.getChunkSize());
            }
        }
        
        throw new IOException("Unknown resource scheme");
    }
    
    public static FixedSizeHDFSFileRecipeGenerator getFixedSizeHDFSFileRecipeGenerator(LocalClusterManager localClusterManager, int chunkSize) {
        synchronized(RecipeGeneratorFactory.class) {
            if(cachedFixedSizeHDFSFileRecipeGenerator == null) {
                cachedFixedSizeHDFSFileRecipeGenerator = new FixedSizeHDFSFileRecipeGenerator(localClusterManager, new Configuration(), chunkSize);
            }
            return cachedFixedSizeHDFSFileRecipeGenerator;
        }
    }
}
