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

package edu.arizona.cs.stargate.recipe;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
public class RecipeList {
    
    private static final Log LOG = LogFactory.getLog(RecipeList.class);
    
    private static final String HADOOP_CONFIG_KEY = RecipeList.class.getCanonicalName();
    
    private List<DataObjectPath> path = new ArrayList<DataObjectPath>();
    
    public static RecipeList createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (RecipeList) serializer.fromJsonFile(file, RecipeList.class);
    }
    
    public static RecipeList createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeList) serializer.fromJson(json, RecipeList.class);
    }
    
    public static RecipeList createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeList) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, RecipeList.class);
    }
    
    public static RecipeList createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeList) serializer.fromJsonFile(fs, file, RecipeList.class);
    }
    
    public RecipeList() {
    }
    
    public RecipeList(RecipeList that) {
        this.path.addAll(that.path);
    }
    
    @JsonProperty("list")
    public Collection<DataObjectPath> getList() {
        return Collections.unmodifiableCollection(this.path);
    }
    
    @JsonProperty("list")
    public void addList(Collection<DataObjectPath> path) {
        this.path.addAll(path);
    }
    
    @JsonIgnore
    public void addList(DataObjectPath path) {
        this.path.add(path);
    }
    
    @JsonIgnore
    public void removeList(DataObjectPath path) {
        this.path.remove(path);
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
    public synchronized void saveTo(FileSystem fs, org.apache.hadoop.fs.Path file) throws IOException {
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
