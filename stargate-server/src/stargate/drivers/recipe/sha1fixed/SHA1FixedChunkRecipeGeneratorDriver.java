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
package stargate.drivers.recipe.sha1fixed;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.recipe.ARecipeGenerator;
import stargate.commons.recipe.ARecipeGeneratorDriver;
import stargate.commons.recipe.ARecipeGeneratorDriverConfiguration;

/**
 *
 * @author iychoi
 */
public class SHA1FixedChunkRecipeGeneratorDriver extends ARecipeGeneratorDriver {

    private static final Log LOG = LogFactory.getLog(SHA1FixedChunkRecipeGeneratorDriver.class);
    
    private SHA1FixedChunkRecipeGeneratorDriverConfiguration config;
    private int chunkSize;
    
    public SHA1FixedChunkRecipeGeneratorDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof SHA1FixedChunkRecipeGeneratorDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of SHA1FixedChunkRecipeGeneratorDriverConfiguration");
        }
        
        this.config = (SHA1FixedChunkRecipeGeneratorDriverConfiguration) config;
    }
    
    public SHA1FixedChunkRecipeGeneratorDriver(ARecipeGeneratorDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof SHA1FixedChunkRecipeGeneratorDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of SHA1FixedChunkRecipeGeneratorDriverConfiguration");
        }
        
        this.config = (SHA1FixedChunkRecipeGeneratorDriverConfiguration) config;
    }
    
    public SHA1FixedChunkRecipeGeneratorDriver(SHA1FixedChunkRecipeGeneratorDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        this.chunkSize = this.config.getChunkSize();
    }

    @Override
    public synchronized void stopDriver() throws IOException {
    }
    
    public synchronized int getChunkSize() {
        return this.chunkSize;
    }
    
    @Override
    public String getDriverName() {
        return "SHA1FixedChunkRecipeGeneratorDriver";
    }

    @Override
    public synchronized ARecipeGenerator getRecipeGenerator() {
        return new SHA1FixedChunkRecipeGenerator(this.chunkSize);
    }
}
