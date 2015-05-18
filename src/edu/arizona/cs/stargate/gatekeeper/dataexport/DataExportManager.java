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

package edu.arizona.cs.stargate.gatekeeper.dataexport;

import edu.arizona.cs.stargate.gatekeeper.distributed.JsonReplicatedMap;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.distributed.DistributedService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class DataExportManager {
    private static final Log LOG = LogFactory.getLog(DataExportManager.class);
    
    private static final String DATAEXPORTMANAGER_MAP_ID = "DataExportManager";
    
    private static DataExportManager instance;
    
    private DistributedService distributedService;

    private JsonReplicatedMap<String, DataExport> dataExports;
    private ArrayList<IDataExportConfigurationChangeEventHandler> configChangeEventHandlers = new ArrayList<IDataExportConfigurationChangeEventHandler>();
    
    public static DataExportManager getInstance(DistributedService distributedService) {
        synchronized (DataExportManager.class) {
            if(instance == null) {
                instance = new DataExportManager(distributedService);
            }
            return instance;
        }
    }
    
    public static DataExportManager getInstance() throws ServiceNotStartedException {
        synchronized (DataExportManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("DataExportManager is not started");
            }
            return instance;
        }
    }
    
    DataExportManager(DistributedService distributedService) {
        this.distributedService = distributedService;
        this.dataExports = new JsonReplicatedMap<String, DataExport>(this.distributedService.getReplicatedMap(DATAEXPORTMANAGER_MAP_ID), DataExport.class);
    }
    
    public synchronized Collection<DataExport> getAllDataExports() {
        return Collections.unmodifiableCollection(this.dataExports.values());
    }
    
    public synchronized DataExport getDataExport(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        return this.dataExports.get(vpath);
    }
    
    public synchronized boolean hasDataExport(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        return this.dataExports.containsKey(vpath);
    }
    
    public synchronized void removeAllDataExports() {
        ArrayList<String> toberemoved = new ArrayList<String>();
        Set<String> keys = this.dataExports.keySet();
        toberemoved.addAll(keys);
        
        for(String key : toberemoved) {
            DataExport export = this.dataExports.get(key);

            if(export != null) {
                removeDataExport(export);
            }
        }
    }
    
    public synchronized void addDataExports(Collection<DataExport> exports) throws DataExportAlreadyAddedException {
        for(DataExport export : exports) {
            addDataExport(export);
        }
    }
    
    public synchronized void addDataExport(DataExport export) throws DataExportAlreadyAddedException {
        if(export == null || export.isEmpty()) {
            throw new IllegalArgumentException("data export is empty or null");
        }
        
        if(this.dataExports.containsKey(export.getVirtualPath())) {
            throw new DataExportAlreadyAddedException("data export " + export.getVirtualPath() + " is already added");
        }
        
        DataExport exportAdded = this.dataExports.put(export.getVirtualPath(), export);
        if(exportAdded != null) {
            raiseEventForAddDataExport(exportAdded);
        }
    }
    
    public synchronized void addConfigChangeEventHandler(IDataExportConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(IDataExportConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<IDataExportConfigurationChangeEventHandler> toberemoved = new ArrayList<IDataExportConfigurationChangeEventHandler>();
        
        for(IDataExportConfigurationChangeEventHandler handler : this.configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IDataExportConfigurationChangeEventHandler handler : toberemoved) {
            this.configChangeEventHandlers.remove(handler);
        }
    }

    public synchronized void removeDataExport(DataExport export) {
        if(export == null || export.isEmpty()) {
            throw new IllegalArgumentException("data export is empty or null");
        }
        
        removeDataExport(export.getVirtualPath());
    }
    
    public synchronized void removeDataExport(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        DataExport removed = this.dataExports.remove(vpath);
        if(removed != null) {
            raiseEventForRemoveDataExport(removed);
        }
    }

    private synchronized void raiseEventForAddDataExport(DataExport export) {
        LOG.debug("data export added : " + export.toString());
        
        for(IDataExportConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addDataExport(this, export);
        }
    }
    
    private synchronized void raiseEventForRemoveDataExport(DataExport export) {
        LOG.debug("data export removed : " + export.toString());
        
        for(IDataExportConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeDataExport(this, export);
        }
    }
    
    @Override
    public synchronized String toString() {
        return "DataExportManager";
    }
}
