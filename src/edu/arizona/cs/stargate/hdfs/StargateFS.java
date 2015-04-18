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

package edu.arizona.cs.stargate.hdfs;

import edu.arizona.cs.stargate.gatekeeper.runtime.GateKeeperRuntimeInfo;
import edu.arizona.cs.stargate.gatekeeper.restful.client.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.restful.client.GateKeeperRestfulClientConfiguration;
import edu.arizona.cs.stargate.gatekeeper.restful.client.TransportRestfulClient;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperServiceConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
public class StargateFS extends FileSystem {

    private static final Log LOG = LogFactory.getLog(StargateFS.class);
    
    private URI uri;
    private GateKeeperClient gatekeeperClient;
    private TransportRestfulClient client;
    private Path workingDir;
    
    public StargateFS() {
    }
    
    private GateKeeperClient createLocalGateKeeperClient() throws IOException {
        GateKeeperRuntimeInfo rc = new GateKeeperRuntimeInfo();
        rc.load();
        
        String localGatekeeperServiceURL = "http://localhost:" + rc.getServicePort();
        LOG.info("connecting to local GateKeeper : " + localGatekeeperServiceURL);
        try {
            GateKeeperRestfulClientConfiguration config = new GateKeeperRestfulClientConfiguration(new URI(localGatekeeperServiceURL));
            return new GateKeeperClient(config);
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    private GateKeeperClient createGateKeeperClient(String host, int port) throws IOException {
        if(host == null) {
            host = "localhost";
        }
        
        if(port <= 0) {
            port = GateKeeperServiceConfiguration.DEFAULT_SERVICE_PORT;
        }
        
        String remoteGatekeeperServiceURL = "http://" + host + ":" + port;
        LOG.info("connecting to remote GateKeeper : " + remoteGatekeeperServiceURL);
        try {
            GateKeeperRestfulClientConfiguration config = new GateKeeperRestfulClientConfiguration(new URI(remoteGatekeeperServiceURL));
            return new GateKeeperClient(config);
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public URI getUri() {
        return this.uri;
    }
    
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        LOG.info("initializing uri for StargateFS : " + uri.toString());
        
        if (this.gatekeeperClient == null) {
            if(uri.getHost() == null || uri.getHost().equalsIgnoreCase("localhost")) {
                this.gatekeeperClient = createLocalGateKeeperClient();
            } else {
                this.gatekeeperClient = createGateKeeperClient(uri.getHost(), uri.getPort());
            }
        }
        
        if(this.client == null) {
            this.client = this.gatekeeperClient.getTransportManagerClient();
        }
        
        setConf(conf);
        this.uri = uri;
        this.workingDir = new Path("/").makeQualified(this);
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
    
    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public boolean isFile(Path path) throws IOException {
        FileStatus fileStatus = getFileStatus(path);
        if(fileStatus.isDir()) {
            return false;
        }
        return true;
    }
    
    private FileStatus convFileStatus(edu.arizona.cs.stargate.hdfs.StargateFileStatus status) {
        return new FileStatus(status.getLength(), status.isDir(), status.getBlockReplication(), status.getBlockSize(), status.getModificationTime(), new Path(status.getCluster() + "/" + status.getVPath()));
    }
    
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        /*
        Path absolutePath = makeAbsolute(path);
        try {
            edu.arizona.cs.stargate.hdfs.StargateFileStatus[] listStatus = this.client.listStatus(absolutePath.toUri());
            
        } catch (Exception ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
        */
        return null;
    }
    
    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    /**
     * This optional operation is not yet supported.
     */
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        /*
        Path absolutePath = makeAbsolute(path);
        return this.remoteFilesystemAPI.open(absolutePath, bufferSize);
        */
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        throw new IOException("Not supported");
    }
    
    @Override
    public boolean delete(Path path) throws IOException {
        throw new IOException("Not supported");
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        /*
        Path absolutePath = makeAbsolute(path);
        try {
            return this.remoteFilesystemAPI.getFileStatus(absolutePath);
        } catch (Exception ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
        */
        return null;
    }
    
    @Override
    public long getDefaultBlockSize() {
        /*
        try {
            return this.remoteFilesystemAPI.getDefaultBlockSize();
        } catch (Exception ex) {
            LOG.error(ex);
            return 1024*1024;
        }
        */
        return 0;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
    }
}
