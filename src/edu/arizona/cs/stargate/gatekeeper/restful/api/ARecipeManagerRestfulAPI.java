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

package edu.arizona.cs.stargate.gatekeeper.restful.api;

import edu.arizona.cs.stargate.gatekeeper.recipe.Chunk;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import java.net.URI;

/**
 *
 * @author iychoi
 */
public abstract class ARecipeManagerRestfulAPI {
    
    public static final String BASE_PATH = "/recipemgr";
    public static final String RECIPE_PATH = "/recipe";
    public static final String CHUNK_PATH = "/chunk";
    
    public abstract LocalRecipe getRecipe(URI resourceURI) throws Exception;
    
    public abstract void removeRecipe(URI resourceURI) throws Exception;
    
    public abstract void removeAllRecipes() throws Exception;
    
    public abstract Chunk getChunk(String hash) throws Exception;
}