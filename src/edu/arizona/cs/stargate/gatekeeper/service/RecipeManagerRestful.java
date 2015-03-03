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

package edu.arizona.cs.stargate.gatekeeper.service;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.Recipe;
import edu.arizona.cs.stargate.gatekeeper.ARecipeManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.io.IOException;
import java.net.URI;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
@Path(ARecipeManagerAPI.PATH)
@Singleton
public class RecipeManagerRestful extends ARecipeManagerAPI {
    
    private static final Log LOG = LogFactory.getLog(RecipeManagerRestful.class);
    
    @GET
    @Path(ARecipeManagerAPI.GET_RECIPE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRecipeText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatter.toJSONFormat(responseGetRecipeJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ARecipeManagerAPI.GET_RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Recipe> responseGetRecipeJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return new RestfulResponse<Recipe>(getRecipe(new URI(name)));
        } catch(Exception ex) {
            return new RestfulResponse<Recipe>(ex);
        }
    }
    
    @Override
    public Recipe getRecipe(URI resourceURI) throws Exception {
        RecipeManager rm = getRecipeManager();
        return rm.getRecipe(resourceURI);
    }
    
    @GET
    @Path(ARecipeManagerAPI.GET_CHUNK_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetChunkInfoText(
            @DefaultValue("null") @QueryParam("hash") String hash
    ) {
        try {
            return DataFormatter.toJSONFormat(responseGetChunkInfoJSON(hash));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ARecipeManagerAPI.GET_CHUNK_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<ChunkInfo> responseGetChunkInfoJSON(
            @DefaultValue("null") @QueryParam("hash") String hash
    ) {
        try {
            return new RestfulResponse<ChunkInfo>(getChunkInfo(hash));
        } catch(Exception ex) {
            return new RestfulResponse<ChunkInfo>(ex);
        }
    }
    
    @Override
    public ChunkInfo getChunkInfo(String hash) throws Exception {
        RecipeManager rm = getRecipeManager();
        return rm.findChunk(hash);
    }
    
    @DELETE
    @Path(ARecipeManagerAPI.DELETE_RECIPE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteRecipeText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatter.toJSONFormat(responseDeleteRecipeJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(ARecipeManagerAPI.DELETE_RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseDeleteRecipeJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            if(name != null) {
                if(name.equals("*")) {
                    removeAllRecipe();
                } else {
                    removeRecipe(new URI(name));
                }
                return new RestfulResponse<Boolean>(true);
            } else {
                return new RestfulResponse<Boolean>(false);
            }
        } catch(Exception ex) {
            return new RestfulResponse<Boolean>(ex);
        }
    }
    
    @Override
    public void removeRecipe(URI resourceURI) throws Exception {
        RecipeManager rm = getRecipeManager();
        rm.removeRecipe(resourceURI);
    }

    @Override
    public void removeAllRecipe() throws Exception {
        RecipeManager rm = getRecipeManager();
        rm.removeAllRecipe();
    }

    private RecipeManager getRecipeManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getRecipeManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
