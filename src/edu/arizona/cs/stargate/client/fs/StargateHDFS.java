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

package edu.arizona.cs.stargate.client.fs;

import edu.arizona.cs.stargate.gatekeeper.filesystem.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.runtime.GateKeeperRuntimeInfo;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
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
    
    private static StargateFileSystem filesystem;
    
    private URI uri;
    private Path workingDir;
    private FileSystem localClusterHDFS;
    
    public StargateHDFS() {
    }
    
    @Override
    public URI getUri() {
        return this.uri;
    }
    
    private static String getGateKeeperServiceAddress(String[] hosts) throws IOException {
        if(hosts == null || hosts.length == 0) {
            GateKeeperRuntimeInfo rc = new GateKeeperRuntimeInfo();
            rc.load();

            String gatekeeperHost = "localhost:" + rc.getServicePort();
            return gatekeeperHost;
        } else {
            // random try
            Random random = new Random();
            int rnd = random.nextInt(hosts.length);
            return hosts[rnd];
        }
    }
    
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        
        LOG.info("initializing uri for StargateFS : " + uri.toString());
        
        if(filesystem == null) {
            String[] gateKeeperHosts = StargateHDFSConfigurationUtils.getGateKeeperHosts(conf);
            this.filesystem = new StargateFileSystem(getGateKeeperServiceAddress(gateKeeperHosts));
        }
        
        setConf(conf);
        this.uri = uri;
        
        this.workingDir = new Path("/").makeQualified(this);
        this.localClusterHDFS = this.workingDir.getFileSystem(conf);
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
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public boolean isFile(Path path) throws IOException {
        FileStatus fileStatus = getFileStatus(path);
        if(fileStatus.isDir()) {
            return false;
        }
        return true;
    }
    
    private FileStatus convFileStatus(VirtualFileStatus status) {
        return new FileStatus(status.getLength(), status.isDir(), 1, status.getBlockSize(), status.getModificationTime(), new Path(status.getVirtualPath()));
    }
    
    private FileStatus[] convFileStatusArray(VirtualFileStatus[] status) {
        FileStatus[] filestatus = new FileStatus[status.length];
        
        for(int i=0;i<status.length;i++) {
            filestatus[i] = convFileStatus(status[i]);
        }
        
        return filestatus;
    }
    
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        VirtualFileStatus[] status = filesystem.listStatus(makeAbsoluteURI(path));
        return convFileStatusArray(status);
    }
    
    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        URI makeAbsoluteURI = makeAbsoluteURI(path);
        if(filesystem.isLocalClusterPath(makeAbsoluteURI)) {
            // read from local 
            URI localClusterPath = filesystem.getLocalClusterResourcePath(makeAbsoluteURI);
            return this.localClusterHDFS.open(new Path(localClusterPath), bufferSize);
        } else {
            // ask to gatekeeper
            return filesystem.open2(makeAbsoluteURI(path), bufferSize);
        }
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

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        VirtualFileStatus status = filesystem.getFileStatus(makeAbsoluteURI(path));
        return convFileStatus(status);
    }
    
    @Override
    public long getDefaultBlockSize() {
        return filesystem.getBlockSize();
    }
    
    @Override
    public void close() throws IOException {
        this.localClusterHDFS.close();
        
        super.close();
    }
}
