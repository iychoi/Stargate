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
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.recipe.ARecipeGeneratorDriver;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.Recipe;
import stargate.commons.service.ServiceNotStartedException;

/**
 *
 * @author iychoi
 */
public class RecipeGeneratorManager {

    private static final Log LOG = LogFactory.getLog(RecipeGeneratorManager.class);
    
    private static RecipeGeneratorManager instance;

    private ARecipeGeneratorDriver driver;
    
    public static RecipeGeneratorManager getInstance(ARecipeGeneratorDriver driver) {
        synchronized (RecipeGeneratorManager.class) {
            if(instance == null) {
                instance = new RecipeGeneratorManager(driver);
            }
            return instance;
        }
    }
    
    public static RecipeGeneratorManager getInstance() throws ServiceNotStartedException {
        synchronized (RecipeGeneratorManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("DataStoreManager is not started");
            }
            return instance;
        }
    }

    RecipeGeneratorManager(ARecipeGeneratorDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        this.driver = driver;
    }
    
    public synchronized ARecipeGeneratorDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        this.driver.startDriver();
    }

    public synchronized void stop() throws IOException {
        this.driver.stopDriver();
    }
    
    public synchronized int getChunkSize() {
        return this.driver.getChunkSize();
    }
    
    public synchronized String getHashAlgorithm() {
        return this.driver.getHashAlgorithm();
    }
    
    public synchronized String getHash(byte[] buffer) throws IOException {
        return this.driver.getHash(buffer);
    }
    
    public synchronized Recipe getRecipe(DataObjectMetadata metadata, InputStream is) throws IOException {
        return this.driver.getRecipe(metadata, is);
    }
    
    @Override
    public synchronized String toString() {
        return "RecipeGeneratorManager";
    }
}
