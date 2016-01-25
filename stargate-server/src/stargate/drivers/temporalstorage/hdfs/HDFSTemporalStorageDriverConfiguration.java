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

package stargate.drivers.temporalstorage.hdfs;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.temporalstorage.APersistentTemporalStorageDriverConfiguration;
import stargate.commons.common.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class HDFSTemporalStorageDriverConfiguration extends APersistentTemporalStorageDriverConfiguration {
    
    private static final Log LOG = LogFactory.getLog(HDFSTemporalStorageDriverConfiguration.class);
    
    private Path rootPath = new Path("/");
    
    public static HDFSTemporalStorageDriverConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (HDFSTemporalStorageDriverConfiguration) serializer.fromJsonFile(file, HDFSTemporalStorageDriverConfiguration.class);
    }
    
    public static HDFSTemporalStorageDriverConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (HDFSTemporalStorageDriverConfiguration) serializer.fromJson(json, HDFSTemporalStorageDriverConfiguration.class);
    }
    
    public HDFSTemporalStorageDriverConfiguration() {
    }
    
    @JsonProperty("root_path")
    public void setRootPath(String rootPath) {
        if(rootPath == null || rootPath.isEmpty()) {
            throw new IllegalArgumentException("rootPath is null or empty");
        }
        
        super.verifyMutable();
        
        this.rootPath = new Path(rootPath);
    }
    
    @JsonIgnore
    public void setRootPath(Path rootPath) {
        if(rootPath == null) {
            throw new IllegalArgumentException("rootPath is null or empty");
        }
        
        super.verifyMutable();
        
        this.rootPath = rootPath;
    }
    
    @JsonProperty("root_path")
    public String getRootPathString() {
        return this.rootPath.toString();
    }
    
    @JsonIgnore
    public Path getRootPath() {
        return this.rootPath;
    }
}
