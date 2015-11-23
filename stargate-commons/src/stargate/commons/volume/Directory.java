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
package stargate.commons.volume;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.recipe.DataObjectPath;

/**
 *
 * @author iychoi
 */
public class Directory {
    private static final Log LOG = LogFactory.getLog(Directory.class);
    
    private DataObjectPath path;
    private List<String> entry = new ArrayList<String>();

    public static Directory createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (Directory) serializer.fromJsonFile(file, Directory.class);
    }
    
    public static Directory createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (Directory) serializer.fromJson(json, Directory.class);
    }
    
    public Directory() {
        this.path = null;
    }
    
    public Directory(Directory that) {
        this.path = that.path;
        this.entry.addAll(that.entry);
    }
    
    public Directory(DataObjectPath path) {
        if(path == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        initialize(path, null);
    }
    
    public Directory(DataObjectPath path, Collection<String> entry) {
        if(path == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        if(entry == null) {
            throw new IllegalArgumentException("entry is null or empty");
        }
        
        initialize(path, entry);
    }
    
    private void initialize(DataObjectPath path, Collection<String> entry) {
        if(path == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        this.path = path;
        if(entry != null) {
            this.entry.addAll(entry);
        }
    }
    
    @JsonProperty("path")
    public DataObjectPath getPath() {
        return this.path;
    }
    
    @JsonProperty("path")
    public void setPath(DataObjectPath path) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        this.path = path;
    }
    
    @JsonProperty("entry")
    public Collection<String> getEntry() {
        return Collections.unmodifiableCollection(this.entry);
    }
    
    @JsonProperty("entry")
    public void addEntry(Collection<String> entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        for(String e : entry) {
            addEntry(e);
        }
    }
    
    @JsonIgnore
    public void addEntry(String entry) {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is null or empty");
        }
        
        this.entry.add(entry);
    }
    
    @JsonIgnore
    public void addEntry(DataObjectPath entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        String relPath = this.path.toUri().relativize(entry.toUri()).getPath();
        if(relPath == null || relPath.isEmpty()) {
            throw new IllegalArgumentException("relPath is null or empty");
        }
        
        addEntry(relPath);
    }
    
    @JsonIgnore
    public void removeEntry(String entry) {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is null or empty");
        }
        
        this.entry.remove(entry);
    }
    
    @JsonIgnore
    public void removeEntry(DataObjectPath entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        String relPath = this.path.toUri().relativize(entry.toUri()).getPath();
        if(relPath == null || relPath.isEmpty()) {
            throw new IllegalArgumentException("relPath is null or empty");
        }
        
        this.entry.remove(relPath);
    }
    
    @JsonIgnore
    public void clearEntry() {
        this.entry.clear();
    }
    
    @JsonIgnore
    public boolean isEmpty() {
        if(this.entry == null || this.entry.isEmpty()) {
            return true;
        }
        return false;
    }
    
    @Override
    public String toString() {
        return this.path.toString();
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
}
