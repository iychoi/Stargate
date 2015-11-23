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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import stargate.commons.sourcefs.ASourceFileSystem;
import stargate.commons.sourcefs.SourceFileMetadata;

/**
 *
 * @author iychoi
 */
public class HDFSSourceFileSystem extends ASourceFileSystem {
    
    private static final Log LOG = LogFactory.getLog(HDFSSourceFileSystem.class);
    
    private FileSystem filesystem;
    
    public HDFSSourceFileSystem(FileSystem fs) {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        this.filesystem = fs;
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
