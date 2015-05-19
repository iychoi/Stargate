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
import edu.arizona.cs.stargate.gatekeeper.filesystem.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AFileSystemRestfulAPI;
import java.io.IOException;
import java.io.InputStream;
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
        return PathUtils.concatPath(AFileSystemRestfulAPI.BASE_PATH, path);
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
    public Collection<VirtualFileStatus> listStatus(String mappedPath) throws Exception {
        RestfulResponse<Collection<VirtualFileStatus>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AFileSystemRestfulAPI.LIST_FILE_STATUS_PATH));
            builder.addParam("mpath", mappedPath);
            String url = builder.build();
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
    public VirtualFileStatus getFileStatus(String mappedPath) throws Exception {
        RestfulResponse<VirtualFileStatus> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AFileSystemRestfulAPI.FILE_STATUS_PATH));
            builder.addParam("mpath", mappedPath);
            String url = builder.build();
            response = (RestfulResponse<VirtualFileStatus>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<VirtualFileStatus>>(){});
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
    public InputStream getDataChunk(String mappedPath, long offset, int size) throws Exception {
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(AFileSystemRestfulAPI.DATA_CHUNK_PATH));
            builder.addParam("mpath", mappedPath);
            builder.addParam("offset", Long.toString(offset));
            builder.addParam("len", Integer.toString(size));
            String url = builder.build();
            return this.gatekeeperRestfulClient.download(url);
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
    }
}
