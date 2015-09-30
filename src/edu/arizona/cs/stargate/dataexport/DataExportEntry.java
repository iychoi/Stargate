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

package edu.arizona.cs.stargate.dataexport;

import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.common.utils.PathUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
public class DataExportEntry {
    
    private static final Log LOG = LogFactory.getLog(DataExportEntry.class);
    
    private static final String HADOOP_CONFIG_KEY = DataExportEntry.class.getCanonicalName();
    
    private String mappingPath;
    private URI resourcePath;
    
    public static DataExportEntry createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (DataExportEntry) serializer.fromJsonFile(file, DataExportEntry.class);
    }
    
    public static DataExportEntry createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportEntry) serializer.fromJson(json, DataExportEntry.class);
    }
    
    public static DataExportEntry createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportEntry) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, DataExportEntry.class);
    }
    
    public static DataExportEntry createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportEntry) serializer.fromJsonFile(fs, file, DataExportEntry.class);
    }
    
    public DataExportEntry() {
        this.mappingPath = null;
        this.resourcePath = null;
    }
    
    public DataExportEntry(DataExportEntry that) {
        this.mappingPath = that.mappingPath;
        this.resourcePath = that.resourcePath;
    }
    
    public DataExportEntry(String mappingPath, URI resourceUri) {
        if(mappingPath == null || mappingPath.isEmpty()) {
            throw new IllegalArgumentException("mappingPath is empty or null");
        }
        
        if(resourceUri == null) {
            throw new IllegalArgumentException("resourceUri is null");
        }
        
        initialize(mappingPath, resourceUri);
    }
    
    public DataExportEntry(String mappingPath, String resourceUri) throws URISyntaxException {
        if(mappingPath == null || mappingPath.isEmpty()) {
            throw new IllegalArgumentException("mappingPath is empty or null");
        }
        
        if(resourceUri == null || resourceUri.isEmpty()) {
            throw new IllegalArgumentException("resourceUri is empty or null");
        }
        
        initialize(mappingPath, new URI(resourceUri));
    }
    
    private void initialize(String mappingPath, URI resourceUri) {
        if(mappingPath == null || mappingPath.isEmpty()) {
            throw new IllegalArgumentException("mappingPath is empty or null");
        }
        
        if(resourceUri == null) {
            throw new IllegalArgumentException("resourceUri is null");
        }
        
        this.mappingPath = mappingPath;
        this.resourcePath = resourceUri;
    }
    
    @JsonProperty("mapping_path")
    public String getMappingPath() {
        return this.mappingPath;
    }
    
    @JsonProperty("mapping_path")
    public void setMappingPath(String mappingPath) {
        if(mappingPath == null || mappingPath.isEmpty()) {
            throw new IllegalArgumentException("mappingPath is empty or null");
        }
        
        this.mappingPath = mappingPath;
    }

    @JsonProperty("resource_path")
    public URI getResourcePath() {
        return resourcePath;
    }

    @JsonProperty("resource_path")
    public void setResourceUri(URI resourcePath) {
        if(resourcePath == null) {
            throw new IllegalArgumentException("resourcePath is null");
        }
        
        this.resourcePath = resourcePath;
    }
    
    @JsonIgnore
    public void setResourceUri(String resourcePath) throws URISyntaxException {
        if(resourcePath == null) {
            throw new IllegalArgumentException("resourcePath is null");
        }
        
        this.resourcePath = new URI(resourcePath);
    }
    
    @JsonIgnore
    public String getVirtualPath() {
        String fileName = PathUtils.getFileName(this.resourcePath);
        return PathUtils.concatPath(this.mappingPath, fileName);
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.mappingPath == null || this.mappingPath.isEmpty()) {
            return true;
        }
        
        if(this.resourcePath == null || this.resourcePath.getPath().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        return this.resourcePath.toString() + "(" + this.mappingPath + ")";
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
