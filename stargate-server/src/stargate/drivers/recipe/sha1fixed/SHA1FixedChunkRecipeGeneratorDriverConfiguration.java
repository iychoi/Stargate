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

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.recipe.ARecipeGeneratorDriverConfiguration;

/**
 *
 * @author iychoi
 */
public class SHA1FixedChunkRecipeGeneratorDriverConfiguration extends ARecipeGeneratorDriverConfiguration {
    
    private static final Log LOG = LogFactory.getLog(SHA1FixedChunkRecipeGeneratorDriverConfiguration.class);
    
    private static final int DEFAULT_CHUNK_SIZE = 1024*1024;
    
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    
    public static SHA1FixedChunkRecipeGeneratorDriverConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (SHA1FixedChunkRecipeGeneratorDriverConfiguration) serializer.fromJsonFile(file, SHA1FixedChunkRecipeGeneratorDriverConfiguration.class);
    }
    
    public static SHA1FixedChunkRecipeGeneratorDriverConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (SHA1FixedChunkRecipeGeneratorDriverConfiguration) serializer.fromJson(json, SHA1FixedChunkRecipeGeneratorDriverConfiguration.class);
    }
    
    public SHA1FixedChunkRecipeGeneratorDriverConfiguration() {
        this.chunkSize = DEFAULT_CHUNK_SIZE;
    }
    
    @JsonProperty("chunk_size")
    public void setChunkSize(int chunkSize) {
        if(chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize is invalid");
        }
        
        super.verifyMutable();
        
        this.chunkSize = chunkSize;
    }
    
    @JsonProperty("chunk_size")
    public int getChunkSize() {
        return this.chunkSize;
    }
}
