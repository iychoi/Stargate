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

import java.io.FileNotFoundException;
import stargate.drivers.sourcefs.hdfs.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import stargate.commons.temporalstorage.APersistentTemporalStorageDriver;
import stargate.commons.temporalstorage.APersistentTemporalStorageDriverConfiguration;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.sourcefs.SourceFileMetadata;
import stargate.commons.temporalstorage.TemporalFileMetadata;

/**
 *
 * @author iychoi
 */
public class HDFSTemporalStorageDriver extends APersistentTemporalStorageDriver {

    private static final Log LOG = LogFactory.getLog(HDFSTemporalStorageDriver.class);
    
    private HDFSTemporalStorageDriverConfiguration config;
    private Configuration hadoopConfig;
    private Path rootPath;
    private FileSystem filesystem;
    
    public HDFSTemporalStorageDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HDFSTemporalStorageDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HDFSTemporalStorageDriverConfiguration");
        }
        
        this.config = (HDFSTemporalStorageDriverConfiguration) config;
    }
    
    public HDFSTemporalStorageDriver(APersistentTemporalStorageDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HDFSTemporalStorageDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HDFSTemporalStorageDriverConfiguration");
        }
        
        this.config = (HDFSTemporalStorageDriverConfiguration) config;
    }
    
    public HDFSTemporalStorageDriver(HDFSTemporalStorageDriverConfiguration config) {
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
        return "HDFSTemporalStorageDriver";
    }

    @Override
    public TemporalFileMetadata getMetadata(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") not exist");
        }
        return new TemporalFileMetadata(path, status.isDir(), status.getLen(), status.getModificationTime());
    }

    @Override
    public boolean exists(URI path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        return this.filesystem.exists(hdfsPath);
    }

    @Override
    public boolean isDirectory(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new FileNotFoundException("directory (" + hdfsPath.toString() + ") not exist");
        }
        return status.isDir();
    }

    @Override
    public boolean isFile(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") not exist");
        }
        return !status.isDir();
    }

    @Override
    public Collection<URI> listDirectory(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new FileNotFoundException("directory (" + hdfsPath.toString() + ") not exist");
        }
        
        FileStatus[] listStatus = this.filesystem.listStatus(hdfsPath);
        List<URI> entries = new ArrayList<URI>();
        if(listStatus != null && listStatus.length > 0) {
            for(FileStatus status : listStatus) {
                URI entryPath = status.getPath().toUri();
                entries.add(entryPath);
            }
        }
        
        return entries;
    }
    
    @Override
    public Collection<TemporalFileMetadata> listDirectoryWithMetadata(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new FileNotFoundException("directory (" + hdfsPath.toString() + ") not exist");
        }
        
        FileStatus[] listStatus = this.filesystem.listStatus(hdfsPath);
        List<TemporalFileMetadata> entries = new ArrayList<TemporalFileMetadata>();
        if(listStatus != null && listStatus.length > 0) {
            for(FileStatus status : listStatus) {
                TemporalFileMetadata metadata = new TemporalFileMetadata(status.getPath().toUri(), status.isDir(), status.getLen(), status.getModificationTime());
                entries.add(metadata);
            }
        }
        
        return entries;
    }
    
    @Override
    public boolean remove(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        return this.filesystem.delete(hdfsPath, true);
    }

    @Override
    public boolean removeDir(URI path, boolean recursive) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        return this.filesystem.delete(hdfsPath, recursive);
    }
    
    @Override
    public boolean makeDirs(URI path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        return this.filesystem.mkdirs(hdfsPath);
    }

    @Override
    public OutputStream getOutputStream(URI path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        return this.filesystem.create(hdfsPath, true);
    }

    @Override
    public InputStream getInputStream(URI path) throws IOException, FileNotFoundException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        Path hdfsPath = new Path(path);
        FileStatus status = this.filesystem.getFileStatus(hdfsPath);
        if(!this.filesystem.exists(hdfsPath)) {
            throw new FileNotFoundException("file (" + hdfsPath.toString() + ") not exist");
        }
        if(status.isDir()) {
            throw new IOException("cannot open a directory (" + hdfsPath.toString() + ")");
        }
        
        return this.filesystem.open(hdfsPath);
    }
}
