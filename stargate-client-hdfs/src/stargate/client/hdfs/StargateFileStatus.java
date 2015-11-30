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

package stargate.client.hdfs;

import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.Recipe;

/**
 *
 * @author iychoi
 */
public class StargateFileStatus {
    
    private static final Log LOG = LogFactory.getLog(StargateFileStatus.class);
    
    private URI path;
    private DataObjectMetadata metadata;
    private long blockSize;
    private URI redirectionPath;
    
    public StargateFileStatus() {
    }
    
    public StargateFileStatus(DataObjectMetadata metadata, long blockSize, URI path, URI redirectionPath) {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is null or empty");
        }
        
        if(blockSize <= 0) {
            throw new IllegalArgumentException("blockSize is invalid");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(redirectionPath == null) {
            throw new IllegalArgumentException("redirectionPath is null");
        }
        
        initialize(metadata, blockSize, path, redirectionPath);
    }
    
    public StargateFileStatus(DataObjectMetadata metadata, long blockSize, URI path) {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is null or empty");
        }
        
        if(blockSize <= 0) {
            throw new IllegalArgumentException("blockSize is invalid");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        initialize(metadata, blockSize, path, null);
    }
    
    public StargateFileStatus(Recipe recipe, URI path, URI redirectionPath) {
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(redirectionPath == null) {
            throw new IllegalArgumentException("redirectionPath is null");
        }
        
        initialize(recipe.getMetadata(), recipe.getChunkSize(), path, redirectionPath);
    }
    
    public StargateFileStatus(Recipe recipe, URI path) {
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        initialize(recipe.getMetadata(), recipe.getChunkSize(), null, path);
    }
    
    private void initialize(DataObjectMetadata metadata, long blockSize, URI path, URI redirectionPath) {
        this.metadata = metadata;
        this.blockSize = blockSize;
        this.path = path;
        this.redirectionPath = redirectionPath;
    }
    
    @JsonProperty("path")
    public URI getPath() {
        return this.path;
    }
    
    @JsonProperty("path")
    public void setPath(URI path) {
        this.path = path;
    }
    
    @JsonProperty("metadata")
    public DataObjectMetadata getMetadata() {
        return this.metadata;
    }
    
    @JsonProperty("metadata")
    public void setMetadata(DataObjectMetadata metadata) {
        this.metadata = metadata;
    }
    
    @JsonProperty("block_size")
    public long getBlockSize() {
        return this.blockSize;
    }
    
    @JsonProperty("block_size")
    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }
    
    @JsonProperty("redirection_path")
    public URI getRedirectionPath() {
        return this.redirectionPath;
    }
    
    @JsonProperty("redirection_path")
    public void setRedirectionPath(URI redirectionPath) {
        this.redirectionPath = redirectionPath;
    }
}
