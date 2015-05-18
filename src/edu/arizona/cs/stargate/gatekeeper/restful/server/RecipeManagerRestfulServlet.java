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

package edu.arizona.cs.stargate.gatekeeper.restful.server;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.gatekeeper.recipe.Chunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperService;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipeManager;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.ARecipeManagerRestfulAPI;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
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
@Path(ARecipeManagerRestfulAPI.BASE_PATH)
@Singleton
public class RecipeManagerRestfulServlet extends ARecipeManagerRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(RecipeManagerRestfulServlet.class);
    
    @GET
    @Path(ARecipeManagerRestfulAPI.RECIPE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetRecipeText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseGetRecipeJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ARecipeManagerRestfulAPI.RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<LocalRecipe> responseGetRecipeJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return new RestfulResponse<LocalRecipe>(getRecipe(new URI(name)));
        } catch(Exception ex) {
            return new RestfulResponse<LocalRecipe>(ex);
        }
    }
    
    @Override
    public LocalRecipe getRecipe(URI resourceURI) throws Exception {
        LocalRecipeManager lrm = getLocalRecipeManager();
        return lrm.getRecipe(resourceURI);
    }
    
    @GET
    @Path(ARecipeManagerRestfulAPI.CHUNK_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetChunkText(
            @DefaultValue("null") @QueryParam("hash") String hash
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseGetChunkJSON(hash));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ARecipeManagerRestfulAPI.CHUNK_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Chunk> responseGetChunkJSON(
            @DefaultValue("null") @QueryParam("hash") String hash
    ) {
        try {
            return new RestfulResponse<Chunk>(getChunk(hash));
        } catch(Exception ex) {
            return new RestfulResponse<Chunk>(ex);
        }
    }
    
    @Override
    public Chunk getChunk(String hash) throws Exception {
        LocalRecipeManager lrm = getLocalRecipeManager();
        return lrm.getChunk(hash);
    }
    
    @DELETE
    @Path(ARecipeManagerRestfulAPI.RECIPE_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteRecipeText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseDeleteRecipeJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(ARecipeManagerRestfulAPI.RECIPE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseDeleteRecipeJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            if(name != null) {
                if(name.equals("*")) {
                    removeAllRecipes();
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
        LocalRecipeManager lrm = getLocalRecipeManager();
        lrm.removeRecipe(resourceURI);
    }

    @Override
    public void removeAllRecipes() throws Exception {
        LocalRecipeManager lrm = getLocalRecipeManager();
        lrm.removeAllRecipes();
    }
    
    private LocalRecipeManager getLocalRecipeManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getLocalRecipeManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
