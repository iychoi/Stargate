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

package edu.arizona.cs.stargate.gatekeeper;

import edu.arizona.cs.stargate.common.recipe.RemoteClusterRecipe;
import java.io.InputStream;

/**
 *
 * @author iychoi
 */
public abstract class ATransportManagerAPI {
    public static final String PATH = "/transportmgr";
    public static final String GET_RECIPE_PATH = "/recipe";
    public static final String GET_RECIPE_URL_PATH = "/recipeu";
    public static final String GET_DATA_CHUNK_PATH = "/chunk";
    public static final String GET_DATA_CHUNK_URL_PATH = "/chunku";
    
    public abstract RemoteClusterRecipe getRecipe(String vpath) throws Exception;
    
    public abstract InputStream getDataChunk(String vpath, long offset, int len) throws Exception;
    
    public abstract InputStream getDataChunk(String hash) throws Exception;
}
