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
import edu.arizona.cs.stargate.gatekeeper.recipe.Chunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.restful.api.ARecipeManagerRestfulAPI;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManagerRestfulClient extends ARecipeManagerRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(RecipeManagerRestfulClient.class);
    
    private GateKeeperRestfulClient gatekeeperRestfulClient;

    public RecipeManagerRestfulClient(GateKeeperRestfulClient gatekeeperRestfulClient) {
        this.gatekeeperRestfulClient = gatekeeperRestfulClient;
    }
    
    public String getResourcePath(String path) {
        return PathUtils.concatPath(ARecipeManagerRestfulAPI.BASE_PATH, path);
    }
    
    @Override
    public Collection<LocalRecipe> getAllRecipes() throws Exception {
        RestfulResponse<Collection<LocalRecipe>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ARecipeManagerRestfulAPI.RECIPE_PATH));
            builder.addParam("name", "*");
            String url = builder.build();
            response = (RestfulResponse<Collection<LocalRecipe>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<LocalRecipe>>>(){});
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
    public LocalRecipe getRecipe(URI resourceURI) throws Exception {
        RestfulResponse<Collection<LocalRecipe>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ARecipeManagerRestfulAPI.RECIPE_PATH));
            builder.addParam("name", resourceURI.toASCIIString());
            String url = builder.build();
            response = (RestfulResponse<Collection<LocalRecipe>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<LocalRecipe>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            Collection<LocalRecipe> recipes = response.getResponse();
            Iterator<LocalRecipe> iterator = recipes.iterator();
            if(iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }
    }

    @Override
    public Chunk getChunk(String hash) throws Exception {
        RestfulResponse<Chunk> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ARecipeManagerRestfulAPI.CHUNK_PATH));
            builder.addParam("hash", hash);
            String url = builder.build();
            response = (RestfulResponse<Chunk>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Chunk>>(){});
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
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ARecipeManagerRestfulAPI.RECIPE_PATH));
            builder.addParam("name", resourceURI.toASCIIString());
            String url = builder.build();
            response = (RestfulResponse<Boolean>) this.gatekeeperRestfulClient.delete(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
    
    @Override
    public void removeAllRecipes() throws Exception {
        RestfulResponse<Boolean> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ARecipeManagerRestfulAPI.RECIPE_PATH));
            builder.addParam("name", "*");
            String url = builder.build();
            response = (RestfulResponse<Boolean>) this.gatekeeperRestfulClient.delete(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
}
