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

import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.common.recipe.ChunkInfo;
import edu.arizona.cs.stargate.common.recipe.LocalClusterRecipe;
import edu.arizona.cs.stargate.common.recipe.RemoteClusterRecipe;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class TransportManager {
    private static final Log LOG = LogFactory.getLog(TransportManager.class);
    
    private static TransportManager instance;
    
    public static TransportManager getInstance() {
        synchronized (TransportManager.class) {
            if(instance == null) {
                instance = new TransportManager();
            }
            return instance;
        }
    }
    
    TransportManager() {
    }
    
    public synchronized RemoteClusterRecipe getRecipe(String vpath) {
        DataExportManager dem = DataExportManager.getInstance();
        DataExportInfo export = dem.getDataExportInfo(vpath);
        if(export != null) {
            try {
                RecipeManager rm = RecipeManager.getInstance();
                LocalClusterRecipe recipe = rm.getRecipe(export.getResourcePath());
                
                return new RemoteClusterRecipe(export.getVirtualPath(), recipe);
            } catch (ServiceNotStartedException ex) {
                LOG.error(ex);
                return null;
            }
        } else {
            return null;
        }
    }
    
    public synchronized ChunkInfo getChunkInfo(String hash) {
        try {
            RecipeManager rm = RecipeManager.getInstance();
            return rm.findChunk(hash);
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
    
    public synchronized URI getResourcePath(String vpath) {
        DataExportManager dem = DataExportManager.getInstance();
        DataExportInfo export = dem.getDataExportInfo(vpath);
        if(export != null) {
            return export.getResourcePath();
        }
        return null;
    }
}
