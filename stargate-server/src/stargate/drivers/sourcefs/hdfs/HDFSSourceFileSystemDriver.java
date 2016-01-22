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
package stargate.drivers.sourcefs.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.sourcefs.ASourceFileSystemDriver;
import stargate.commons.sourcefs.ASourceFileSystemDriverConfiguration;
import stargate.commons.sourcefs.SourceFileMetadata;

/**
 *
 * @author iychoi
 */
public class HDFSSourceFileSystemDriver extends ASourceFileSystemDriver {

    private static final Log LOG = LogFactory.getLog(HDFSSourceFileSystemDriver.class);
    
    private HDFSSourceFileSystemDriverConfiguration config;
    private Configuration hadoopConfig;
    private Path rootPath;
    private FileSystem filesystem;
    
    public HDFSSourceFileSystemDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HDFSSourceFileSystemDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HDFSSourceFileSystemDriverConfiguration");
        }
        
        this.config = (HDFSSourceFileSystemDriverConfiguration) config;
    }
    
    public HDFSSourceFileSystemDriver(ASourceFileSystemDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HDFSSourceFileSystemDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HDFSSourceFileSystemDriverConfiguration");
        }
        
        this.config = (HDFSSourceFileSystemDriverConfiguration) config;
    }
    
    public HDFSSourceFileSystemDriver(HDFSSourceFileSystemDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        this.hadoopConfig = new Configuration();
        Path rootPath = this.config.getRootPath();
        String hdfsRoot = this.hadoopConfig.get("fs.default.name");
        LOG.info("hdfs root - " + hdfsRoot);
        this.rootPath = new Path(hdfsRoot, rootPath);
        LOG.info("creating a filesystem for " + this.rootPath);
        this.filesystem = this.rootPath.getFileSystem(this.hadoopConfig);
    }

    @Override
    public synchronized void stopDriver() throws IOException {
    }
    
    @Override
    public String getDriverName() {
        return "HDFSSourceFileSystemDriver";
    }

    @Override
    public SourceFileMetadata getMetadata(URI path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new IOException("file (" + hdfsPath.toString() + ") not exist");
        }
        return new SourceFileMetadata(path, status.getLen(), status.getModificationTime());
    }

    @Override
    public InputStream getInputStream(URI path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new IOException("file (" + hdfsPath.toString() + ") not exist");
        }
        if(status.isDir()) {
            throw new IOException("cannot open a directory (" + hdfsPath.toString() + ")");
        }
        
        return this.filesystem.open(hdfsPath);
    }

    @Override
    public InputStream getInputStream(URI path, long offset, int size) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new IOException("file (" + hdfsPath.toString() + ") not exist");
        }
        if(status.isDir()) {
            throw new IOException("cannot open a directory (" + hdfsPath.toString() + ")");
        }
        
        return new HDFSChunkReader(this.filesystem, hdfsPath, offset, size);
    }
}
