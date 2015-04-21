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

package edu.arizona.cs.stargate.gatekeeper.restful.client;

import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.dataexport.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeChunk;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AFileSystemRestfulAPI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FileSystemRestfulClient extends AFileSystemRestfulAPI {
    private static final Log LOG = LogFactory.getLog(FileSystemRestfulClient.class);
    
    private GateKeeperRestfulClient gatekeeperRestfulClient;

    public FileSystemRestfulClient(GateKeeperRestfulClient gatekeeperRestfulClient) {
        this.gatekeeperRestfulClient = gatekeeperRestfulClient;
    }
    
    public String getResourcePath(String path) {
        return AFileSystemRestfulAPI.BASE_PATH + path;
    }
    
    public String getResourcePath(String path, String subpath) {
        if(path.endsWith("/")) {
            return AFileSystemRestfulAPI.BASE_PATH + path + subpath;
        } else {
            return AFileSystemRestfulAPI.BASE_PATH + path + "/" + subpath;
        }
    }

    @Override
    public Collection<VirtualFileStatus> getAllVirtualFileStatus() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long getBlockSize() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Cluster getLocalCluster() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] readChunkData(String clusterName, String virtualPath, long offset, long size) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
