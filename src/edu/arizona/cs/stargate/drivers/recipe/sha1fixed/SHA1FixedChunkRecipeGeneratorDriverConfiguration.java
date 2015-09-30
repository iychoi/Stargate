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

package edu.arizona.cs.stargate.drivers.recipe.sha1fixed;

import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.recipe.ARecipeGeneratorDriverConfiguration;
import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class SHA1FixedChunkRecipeGeneratorDriverConfiguration extends ARecipeGeneratorDriverConfiguration {
    
    private static final Log LOG = LogFactory.getLog(SHA1FixedChunkRecipeGeneratorDriverConfiguration.class);
    
    private static final String HADOOP_CONFIG_KEY = SHA1FixedChunkRecipeGeneratorDriverConfiguration.class.getCanonicalName();
    
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
    
    public static SHA1FixedChunkRecipeGeneratorDriverConfiguration createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (SHA1FixedChunkRecipeGeneratorDriverConfiguration) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, SHA1FixedChunkRecipeGeneratorDriverConfiguration.class);
    }
    
    public static SHA1FixedChunkRecipeGeneratorDriverConfiguration createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (SHA1FixedChunkRecipeGeneratorDriverConfiguration) serializer.fromJsonFile(fs, file, SHA1FixedChunkRecipeGeneratorDriverConfiguration.class);
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
    
    @Override
    public void setImmutable() {
        super.setImmutable();
    }
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
