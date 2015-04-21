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

import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.common.PathUtils;
import edu.arizona.cs.stargate.common.WebParamBuilder;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.recipe.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AFileSystemRestfulAPI;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FileSystemRestfulClient extends AFileSystemRestfulAPI {
    private static final Log LOG = LogFactory.getLog(FileSystemRestfulClient.class);
    
    public static final int DEFAULT_BLOCK_SIZE = 1024*1024;
    
    private GateKeeperRestfulClient gatekeeperRestfulClient;

    public FileSystemRestfulClient(GateKeeperRestfulClient gatekeeperRestfulClient) {
        this.gatekeeperRestfulClient = gatekeeperRestfulClient;
    }
    
    public String getResourcePath(String path) {
        return PathUtils.concatPath(AFileSystemRestfulAPI.BASE_PATH, path);
    }
    
    public String getResourcePath(String path, String subpath) {
        String str1 = PathUtils.concatPath(AFileSystemRestfulAPI.BASE_PATH, path);
        return PathUtils.concatPath(str1, subpath);
    }
    
    @Override
    public Collection<VirtualFileStatus> getAllVirtualFileStatus() throws Exception {
        RestfulResponse<Collection<VirtualFileStatus>> response;
        try {
            String url = getResourcePath(AFileSystemRestfulAPI.VIRTUAL_FILE_STATUS_PATH);
            response = (RestfulResponse<Collection<VirtualFileStatus>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<VirtualFileStatus>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            return response.getResponse();
        }
    }

    @Override
    public long getBlockSize() throws Exception {
        RestfulResponse<Long> response;
        try {
            String url = getResourcePath(AFileSystemRestfulAPI.FS_BLOCK_SIZE_PATH);
            response = (RestfulResponse<Long>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Long>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            return DEFAULT_BLOCK_SIZE;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            return response.getResponse().longValue();
        }
    }

    @Override
    public Cluster getLocalCluster() throws Exception {
        RestfulResponse<Cluster> response;
        try {
            String url = getResourcePath(AFileSystemRestfulAPI.LOCAL_CLUSTER_PATH);
            response = (RestfulResponse<Cluster>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Cluster>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            return response.getResponse();
        }
    }

    @Override
    public byte[] readChunkData(String clusterName, String virtualPath, long offset, long size) throws Exception {
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AFileSystemRestfulAPI.CHUNK_DATA_PATH, PathUtils.concatPath(clusterName, virtualPath)));
            builder.addParam("offset", Long.toString(offset));
            builder.addParam("len", Long.toString(size));
            String url = builder.build();
            InputStream is = this.gatekeeperRestfulClient.download(url);
            if(is == null) {
                return null;
            }
            return IOUtils.toByteArray(is); 
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
    }
}
