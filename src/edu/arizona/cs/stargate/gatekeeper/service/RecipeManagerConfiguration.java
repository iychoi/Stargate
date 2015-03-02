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

package edu.arizona.cs.stargate.gatekeeper.service;

import edu.arizona.cs.stargate.common.ImmutableConfiguration;
import java.io.File;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class RecipeManagerConfiguration extends ImmutableConfiguration {
    
    private static final int DEFAULT_CHUNK_SIZE = 1024*1024;
    private static final String DEFAULT_HASH_ALGORITHM = "SHA-1";
    
    private File recipePath;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private boolean immutable = false;
    private String hashAlgorithm = DEFAULT_HASH_ALGORITHM;
    
    @JsonIgnore
    public File getRecipePath() {
        return this.recipePath;
    }
    
    @JsonProperty("recipePath")
    public String getRecipePathString() {
        return this.recipePath.getPath();
    }
    
    @JsonIgnore
    public void setRecipePath(File recipePath) {
        super.verifyMutable();
        
        this.recipePath = recipePath;
    }
    
    @JsonProperty("recipePath")
    public void setRecipePath(String recipePath) {
        super.verifyMutable();
        
        this.recipePath = new File(recipePath);
    }
    
    @JsonProperty("chunkSize")
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    @JsonProperty("chunkSize")
    public void setChunkSize(int chunkSize) {
        super.verifyMutable();
        
        this.chunkSize = chunkSize;
    }
    
    @JsonProperty("hashAlgorithm")
    public String getHashAlgorithm() {
        return this.hashAlgorithm;
    }
    
    @JsonProperty("hashAlgorithm")
    public void setHashAlgorithm(String hashAlgorithm) {
        super.verifyMutable();
        
        this.hashAlgorithm = hashAlgorithm;
    }
}