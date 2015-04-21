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

package edu.arizona.cs.stargate.gatekeeper.recipe;

import java.net.URI;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class VirtualFileStatus {

    private String clusterName;
    private String virtualPath;
    private boolean dir;
    private long length;
    private long blockSize;
    private long modificationTime;
    private URI localHDFSResourcePath;
    
    public VirtualFileStatus() {
    }
    
    public VirtualFileStatus(String clusterName, String vpath, boolean dir, long length, long blockSize, long modificationTime) {
        initialize(clusterName, vpath, dir, length, blockSize, modificationTime, null);
    }
    
    public VirtualFileStatus(String clusterName, String vpath, boolean dir, long length, long blockSize, long modificationTime, URI localHDFSResourcePath) {
        initialize(clusterName, vpath, dir, length, blockSize, modificationTime, localHDFSResourcePath);
    }
    
    private void initialize(String clusterName, String vpath, boolean dir, long length, long blockSize, long modificationTime, URI localHDFSResourcePath) {
        this.clusterName = clusterName;
        this.virtualPath = vpath;
        this.dir = dir;
        this.length = length;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.localHDFSResourcePath = localHDFSResourcePath;
    }
    
    @JsonProperty("modificationTime")
    public long getModificationTime() {
        return this.modificationTime;
    }
    
    @JsonProperty("modificationTime")
    public void setModificationTime(long time) {
        this.modificationTime = time;
    }
 
    @JsonProperty("blockSize")
    public long getBlockSize() {
        return this.blockSize;
    }
    
    @JsonProperty("blockSize")
    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }
    
    @JsonProperty("dir")
    public boolean isDir() {
        return dir;
    }
    
    @JsonProperty("dir")
    public void setDir(boolean dir) {
        this.dir = dir;
    }
    
    @JsonProperty("length")
    public long getLength() {
        return this.length;
    }
    
    @JsonProperty("length")
    public void setLength(long length) {
        this.length = length;
    }
    
    @JsonProperty("clusterName")
    public String getClusterName() {
        return this.clusterName;
    }
    
    @JsonProperty("clusterName")
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    @JsonProperty("virtualPath")
    public String getVirtualPath() {
        return this.virtualPath;
    }
    
    @JsonProperty("virtualPath")
    public void setVirtualPath(String virtualPath) {
        this.virtualPath = virtualPath;
    }

    @JsonProperty("localHDFSResourcePath")
    public URI getLocalHDFSResourcePath() {
        return this.localHDFSResourcePath;
    }
    
    @JsonProperty("localHDFSResourcePath")
    public void setLocalHDFSResourcePath(URI localHDFSResourcePath) {
        this.localHDFSResourcePath = localHDFSResourcePath;
    }
}
