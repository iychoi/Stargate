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

package edu.arizona.cs.stargate.drivers.userinterface.http.hadoop;

import edu.arizona.cs.stargate.recipe.DataObjectMetadata;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfiguration;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author iychoi
 */
public class StargateHDFS extends FileSystem {

    private static final Log LOG = LogFactory.getLog(StargateHDFS.class);
    
    private StargateFileSystem filesystem;
    private URI uri;
    private Path workingDir;
    private FileSystem localHDFS;
    
    public StargateHDFS() {
    }
    
    @Override
    public URI getUri() {
        return this.uri;
    }
    
    private String getStargateHost(URI uri) {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        String host = "localhost";
        int port = HTTPUserInterfaceDriverConfiguration.DEFAULT_SERVICE_PORT;
        
        if(uri.getHost() != null && !uri.getHost().isEmpty()) {
            host = uri.getHost();
        }
        
        if(uri.getPort() > 0) {
            port = uri.getPort();
        }
        
        return host + ":" + port;
    }
    
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        super.initialize(uri, conf);
        
        LOG.info("initializing uri for StargateFS : " + uri.toString());
        
        if(this.filesystem == null) {
            this.filesystem = new StargateFileSystem(getStargateHost(uri));
        }
        
        setConf(conf);
        this.uri = uri;
        
        this.workingDir = new Path("/").makeQualified(this);
        this.localHDFS = this.workingDir.getFileSystem(conf);
    }
    
    @Override
    public String getName() {
        return getUri().toString();
    }

    @Override
    public Path getWorkingDirectory() {
        return this.workingDir;
    }
    
    @Override
    public void setWorkingDirectory(Path path) {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        this.workingDir = makeAbsolute(path);
    }
    
    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(this.workingDir, path);
    }
    
    private URI makeAbsoluteURI(Path path) {
        return makeAbsolute(path).toUri();
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        URI absPath = makeAbsoluteURI(path);
        StargateFileStatus status = this.filesystem.getFileStatus(absPath);
        URI redirectionPath = status.getRedirectionPath();
        if(redirectionPath != null) {
            // read from local 
            return this.localHDFS.open(new Path(redirectionPath), bufferSize);
        } else {
            // pass to stargate
            return this.filesystem.open(absPath, bufferSize);
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        URI absPath = makeAbsoluteURI(path);
        StargateFileStatus status = this.filesystem.getFileStatus(absPath);
        return makeFileStatus(status);
    }
    
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        if(path == null) {
            throw new IllegalArgumentException("path is null");
        }
        
        URI absPath = makeAbsoluteURI(path);
        Collection<StargateFileStatus> status = this.filesystem.listStatus(absPath);
        if(status != null) {
            FileStatus[] statusArr = new FileStatus[status.size()];
            int i = 0;
            for(StargateFileStatus s : status) {
                statusArr[i] = makeFileStatus(s);
            }
        }
        return null;
    }
    
    private FileStatus makeFileStatus(StargateFileStatus status) {
        DataObjectMetadata metadata = status.getMetadata();
        return new FileStatus(metadata.getObjectSize(), metadata.isDirectory(), 1, status.getBlockSize(), metadata.getLastModificationTime(), new Path(status.getPath()));
    }
    
    @Override
    public long getDefaultBlockSize() {
        return this.filesystem.getBlockSize();
    }
    
    @Override
    public void close() throws IOException {
        this.filesystem.close();
        this.localHDFS.close();
        
        super.close();
    }
    
    // followings are not supported
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean delete(Path path) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
}
