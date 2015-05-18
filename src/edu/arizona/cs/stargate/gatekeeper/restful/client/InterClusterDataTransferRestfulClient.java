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

import edu.arizona.cs.stargate.common.WebParamBuilder;
import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.common.PathUtils;
import edu.arizona.cs.stargate.gatekeeper.recipe.RemoteRecipe;
import edu.arizona.cs.stargate.gatekeeper.restful.api.InterClusterDataTransferRestfulAPI;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class InterClusterDataTransferRestfulClient extends InterClusterDataTransferRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(InterClusterDataTransferRestfulClient.class);
    
    private GateKeeperRestfulClient gatekeeperRestfulClient;

    public InterClusterDataTransferRestfulClient(GateKeeperRestfulClient gatekeeperRestfulClient) {
        this.gatekeeperRestfulClient = gatekeeperRestfulClient;
    }
    
    public String getResourcePath(String path) {
        return PathUtils.concatPath(InterClusterDataTransferRestfulAPI.BASE_PATH, path);
    }
    
    public String getResourcePath(String path, String subpath) {
        String str1 = PathUtils.concatPath(InterClusterDataTransferRestfulAPI.BASE_PATH, path);
        return PathUtils.concatPath(str1, subpath);
    }
    
    @Override
    public Collection<RemoteRecipe> getAllRecipes() throws Exception {
        RestfulResponse<Collection<RemoteRecipe>> response;
        try {
            String url = getResourcePath(InterClusterDataTransferRestfulAPI.RECIPE_PATH, "*");
            response = (RestfulResponse<Collection<RemoteRecipe>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<RemoteRecipe>>>(){});
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
    public RemoteRecipe getRecipe(String vpath) throws Exception {
        RestfulResponse<Collection<RemoteRecipe>> response;
        try {
            String url = getResourcePath(InterClusterDataTransferRestfulAPI.RECIPE_PATH, vpath);
            response = (RestfulResponse<Collection<RemoteRecipe>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<RemoteRecipe>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            Collection<RemoteRecipe> recipes = response.getResponse();
            Iterator<RemoteRecipe> iterator = recipes.iterator();
            if(iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }
    }

    @Override
    public InputStream getDataChunk(String vpath, long offset, int len) throws Exception {
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(InterClusterDataTransferRestfulAPI.DATA_CHUNK_PATH, vpath));
            builder.addParam("offset", Long.toString(offset));
            builder.addParam("len", Integer.toString(len));
            String url = builder.build();
            return this.gatekeeperRestfulClient.download(url);
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
    }

    @Override
    public InputStream getDataChunk(String hash) throws Exception {
        try {
            String url = getResourcePath(InterClusterDataTransferRestfulAPI.DATA_CHUNK_PATH, hash);
            return this.gatekeeperRestfulClient.download(url);
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
    }
}
