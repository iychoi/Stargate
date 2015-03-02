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

import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.Recipe;
import java.io.File;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RecipeManager {
    private static final Log LOG = LogFactory.getLog(RecipeManager.class);
    
    private static RecipeManager instance;
    
    //TODO: Need a background thread for preparing Recipe
    
    public static RecipeManager getInstance() {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager();
            }
            return instance;
        }
    }
    
    RecipeManager() {
        
    }
    
    public synchronized void prepareRecipe(URI resourceUri) {
        //TODO: Create a recipe for a resource
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public synchronized Recipe getRecipe(URI resourceUri) {
        //TODO: Check existing recipe
        // if there's no existing recipe, create
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public synchronized ChunkInfo findChunk(String hash) {
        return findChunk(DataFormatter.hexToBytes(hash));
    }
    
    public synchronized ChunkInfo findChunk(byte[] hash) {
        //TODO: Find chunk that has a hash
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.        
    }
    
    @Override
    public synchronized String toString() {
        return "RecipeManager";
    }

    void setRecipePath(File recipePath) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    void prepareRecipe(DataExportInfo info) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    void removeRecipe(DataExportInfo info) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
