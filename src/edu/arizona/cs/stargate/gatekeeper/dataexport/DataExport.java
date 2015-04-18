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

package edu.arizona.cs.stargate.gatekeeper.dataexport;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class DataExport {
    private String mappingPath;
    private URI resourcePath;
    
    DataExport() {
        this.mappingPath = null;
        this.resourcePath = null;
    }
    
    public static DataExport createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DataExport) serializer.fromJsonFile(file, DataExport.class);
    }
    
    public static DataExport createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DataExport) serializer.fromJson(json, DataExport.class);
    }
    
    public DataExport(DataExport that) {
        this.mappingPath = that.mappingPath;
        this.resourcePath = that.resourcePath;
    }
    
    public DataExport(String mappingPath, URI resourceUri) {
        initializeDataExport(mappingPath, resourceUri);
    }
    
    public DataExport(String mappingPath, String resourceUri) throws URISyntaxException {
        initializeDataExport(mappingPath, new URI(resourceUri));
    }
    
    private void initializeDataExport(String mappingPath, URI resourceUri) {
        setMappingPath(mappingPath);
        setResourceUri(resourceUri);
    }
    
    @JsonIgnore
    private String buildVirtualPath(String mappingPath, URI resourceUri) {
        String path = resourceUri.normalize().getPath();
        int idx = path.lastIndexOf("/");
        String filename = path;
        if(idx >= 0) {
            filename = path.substring(idx+1, path.length());
        }
        
        if(mappingPath.endsWith("/")) {
            return mappingPath + filename;
        } else {
            return mappingPath + "/" + filename;
        }
    }

    @JsonProperty("mappingPath")
    public String getMappingPath() {
        return this.mappingPath;
    }
    
    @JsonProperty("mappingPath")
    void setMappingPath(String mappingPath) {
        this.mappingPath = mappingPath;
    }

    @JsonProperty("resourcePath")
    public URI getResourcePath() {
        return resourcePath;
    }

    @JsonProperty("resourcePath")
    void setResourceUri(URI resourcePath) {
        this.resourcePath = resourcePath;
    }
    
    @JsonIgnore
    void setResourceUri(String resourcePath) throws URISyntaxException {
        this.resourcePath = new URI(resourcePath);
    }
    
    @JsonIgnore
    public String getVirtualPath() {
        return buildVirtualPath(this.mappingPath, this.resourcePath);
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
}
