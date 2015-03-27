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

package edu.arizona.cs.stargate.common.dataexport;

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
public class DataExportInfo {
    private String mountPath;
    private URI resourcePath;
    
    DataExportInfo() {
        this.mountPath = null;
        this.resourcePath = null;
    }
    
    public static DataExportInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportInfo) serializer.fromJsonFile(file, DataExportInfo.class);
    }
    
    public static DataExportInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportInfo) serializer.fromJson(json, DataExportInfo.class);
    }
    
    public DataExportInfo(DataExportInfo that) {
        this.mountPath = that.mountPath;
        this.resourcePath = that.resourcePath;
    }
    
    public DataExportInfo(String mountPath, URI resourceUri) {
        initializeDataExportEntry(mountPath, resourceUri);
    }
    
    public DataExportInfo(String mountPath, String resourceUri) throws URISyntaxException {
        initializeDataExportEntry(mountPath, new URI(resourceUri));
    }
    
    private void initializeDataExportEntry(String mountPath, URI resourceUri) {
        setMountPath(mountPath);
        setResourceUri(resourceUri);
    }
    
    private String buildVirtualPath(String mountPath, URI resourceUri) {
        String path = resourceUri.normalize().getPath();
        int idx = path.lastIndexOf("/");
        String filename = path;
        if(idx >= 0) {
            filename = path.substring(idx+1, path.length());
        }
        
        if(mountPath.endsWith("/")) {
            return mountPath + filename;
        } else {
            return mountPath + "/" + filename;
        }
    }

    @JsonProperty("mount")
    public String getMountPath() {
        return this.mountPath;
    }
    
    @JsonProperty("mount")
    void setMountPath(String mountPath) {
        this.mountPath = mountPath;
    }

    @JsonProperty("path")
    public URI getResourcePath() {
        return resourcePath;
    }

    @JsonProperty("path")
    void setResourceUri(URI resourcePath) {
        this.resourcePath = resourcePath;
    }
    
    void setResourceUri(String resourcePath) throws URISyntaxException {
        this.resourcePath = new URI(resourcePath);
    }
    
    @JsonIgnore
    public String getVirtualPath() {
        return buildVirtualPath(this.mountPath, this.resourcePath);
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.mountPath == null || this.mountPath.isEmpty()) {
            return true;
        }
        
        if(this.resourcePath == null || this.resourcePath.getPath().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        return this.resourcePath.toString() + "(" + this.mountPath + ")";
    }
}
