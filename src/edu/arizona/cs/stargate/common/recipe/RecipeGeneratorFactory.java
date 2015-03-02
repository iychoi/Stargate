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

package edu.arizona.cs.stargate.common.recipe;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class RecipeGeneratorFactory {
    
    private static FixedSizeHDFSFileRecipeGenerator cachedFixedSizeHDFSFileRecipeGenerator;
    private static FixedSizeLocalFileRecipeGenerator cachedFixedSizeLocalFileRecipeGenerator;
    
    public static ARecipeGenerator getRecipeGenerator(URI resourceUri, int chunkSize) throws IOException {
        if(resourceUri.getScheme().equalsIgnoreCase("hdfs") || resourceUri.getScheme().equalsIgnoreCase("dfs")) {
            return getFixedSizeHDFSFileRecipeGenerator(chunkSize);
        } else if(resourceUri.getScheme().equalsIgnoreCase("file")) {
            return getFixedSizeLocalFileRecipeGenerator(chunkSize);
        }
        
        throw new IOException("Unknown resource scheme");
    }
    
    public static FixedSizeHDFSFileRecipeGenerator getFixedSizeHDFSFileRecipeGenerator(int chunkSize) {
        synchronized(cachedFixedSizeHDFSFileRecipeGenerator) {
            if(cachedFixedSizeHDFSFileRecipeGenerator == null) {
                cachedFixedSizeHDFSFileRecipeGenerator = new FixedSizeHDFSFileRecipeGenerator(new Configuration(), chunkSize);
            }
            return cachedFixedSizeHDFSFileRecipeGenerator;
        }
    }

    public static FixedSizeLocalFileRecipeGenerator getFixedSizeLocalFileRecipeGenerator(int chunkSize) {
        synchronized(cachedFixedSizeLocalFileRecipeGenerator) {
            if(cachedFixedSizeLocalFileRecipeGenerator == null) {
                cachedFixedSizeLocalFileRecipeGenerator = new FixedSizeLocalFileRecipeGenerator(chunkSize);
            }
            return cachedFixedSizeLocalFileRecipeGenerator;
        }
    }
}
