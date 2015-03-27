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

package edu.arizona.cs.stargate.gatekeeper.client;

import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.Recipe;
import edu.arizona.cs.stargate.gatekeeper.ARecipeManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManagerClient extends ARecipeManagerAPI {
    
    private static final Log LOG = LogFactory.getLog(RecipeManagerClient.class);
    
    private GateKeeperClient gatekeeperClient;
    private GateKeeperRPCClient gatekeeperRPCClient;

    public RecipeManagerClient(GateKeeperClient gatekeeperClient) {
        this.gatekeeperClient = gatekeeperClient;
        this.gatekeeperRPCClient = gatekeeperClient.getRPCClient();
    }
    
    public String getResourcePath(String path) {
        return ARecipeManagerAPI.PATH + path;
    }
    
    public String getResourcePath(String path, Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, String>> entrySet = params.entrySet();
        for(Map.Entry<String, String> entry : entrySet) {
            sb.append(entry.getKey() + "=" + entry.getValue() + "&");
        }
        return ARecipeManagerAPI.PATH + path + "?" + sb.toString();
    }
    
    @Override
    public Recipe getRecipe(URI resourceURI) throws Exception {
        RestfulResponse<Recipe> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("name", resourceURI.toASCIIString());
            response = (RestfulResponse<Recipe>) this.gatekeeperRPCClient.get(getResourcePath(ARecipeManagerAPI.GET_RECIPE_PATH, params), new GenericType<RestfulResponse<Recipe>>(){});
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
    public ChunkInfo getChunkInfo(String hash) throws Exception {
        RestfulResponse<ChunkInfo> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("hash", hash);
            response = (RestfulResponse<ChunkInfo>) this.gatekeeperRPCClient.get(getResourcePath(ARecipeManagerAPI.GET_CHUNK_INFO_PATH, params), new GenericType<RestfulResponse<ChunkInfo>>(){});
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
    public void removeRecipe(URI resourceURI) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("name", resourceURI.toASCIIString());
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(getResourcePath(ARecipeManagerAPI.DELETE_RECIPE_PATH, params), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
    
    @Override
    public void removeAllRecipe() throws Exception {
        RestfulResponse<Boolean> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("name", "*");
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(getResourcePath(ARecipeManagerAPI.DELETE_RECIPE_PATH, params), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }

    @Override
    public InputStream getDataChunk(String hash) throws Exception {
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("hash", hash);
            return this.gatekeeperRPCClient.download(getResourcePath(ARecipeManagerAPI.GET_DATA_CHUNK_PATH, params));
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
    }
}
