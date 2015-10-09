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
package edu.arizona.cs.stargate.volume;

import edu.arizona.cs.stargate.cluster.ClusterManager;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.dataexport.IDataExportEventHandler;
import edu.arizona.cs.stargate.recipe.DataObjectPath;
import edu.arizona.cs.stargate.recipe.RecipeFactory;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class DataExportChangedEventHandler implements IDataExportEventHandler {
    
    private static final Log LOG = LogFactory.getLog(DataExportChangedEventHandler.class);
    
    private ClusterManager clusterManager;
    private VolumeManager volumeManager;

    public DataExportChangedEventHandler(ClusterManager clusterManager, VolumeManager volumeManager) {
        if(clusterManager == null) {
            throw new IllegalArgumentException("clusterManager is null");
        }
        
        if(volumeManager == null) {
            throw new IllegalArgumentException("volumeManager is null");
        }
        
        this.clusterManager = clusterManager;
        this.volumeManager = volumeManager;
    }
    
    @Override
    public String getName() {
        return "DataExportChangedEventHandler";
    }

    @Override
    public void dataExportEntryAdded(DataExportManager manager, DataExportEntry entry) {
        try {
            // generate volume entrypath
            DataObjectPath dataObjectPath = RecipeFactory.createDataObjectPath(this.clusterManager.getLocalClusterManager(), entry);
            this.volumeManager.addLocalDirectoryEntry(dataObjectPath);
        } catch (IOException ex) {
            LOG.error(ex);
        }
    }

    @Override
    public void dataExportEntryRemoved(DataExportManager manager, DataExportEntry entry) {
        try {
            DataObjectPath dataObjectPath = RecipeFactory.createDataObjectPath(this.clusterManager.getLocalClusterManager(), entry);
            this.volumeManager.removeLocalDirectoryEntry(dataObjectPath);
        } catch (IOException ex) {
            LOG.error(ex);
        }
    }
}
