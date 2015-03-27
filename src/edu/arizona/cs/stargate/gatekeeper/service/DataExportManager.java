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

import edu.arizona.cs.stargate.cache.service.JsonMap;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.service.StargateService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
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
    
    private Map<String, DataExportInfo> dataExports;
    private ArrayList<IDataExportConfigurationChangeEventHandler> configChangeEventHandlers = new ArrayList<IDataExportConfigurationChangeEventHandler>();
    
    public static DataExportManager getInstance() {
        synchronized (DataExportManager.class) {
            if(instance == null) {
                instance = new DataExportManager();
            }
            return instance;
        }
    }
    
    DataExportManager() {
        try {
            this.dataExports = new JsonMap<String, DataExportInfo>(StargateService.getInstance().getDistributedCacheService().getReplicatedMap(DATAEXPORTMANAGER_MAP_ID), DataExportInfo.class);
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            throw new RuntimeException(ex);
        }
    }
    
    public synchronized Collection<DataExportInfo> getAllDataExportInfo() {
        return Collections.unmodifiableCollection(this.dataExports.values());
    }
    
    public synchronized DataExportInfo getDataExportInfo(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        return this.dataExports.get(vpath);
    }
    
    public synchronized boolean hasDataExportInfo(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        return this.dataExports.containsKey(vpath);
    }
    
    public synchronized void removeAllDataExport() {
        ArrayList<String> toberemoved = new ArrayList<String>();
        Set<String> keys = this.dataExports.keySet();
        toberemoved.addAll(keys);
        
        for(String key : toberemoved) {
            DataExportInfo info = this.dataExports.get(key);

            if(info != null) {
                removeDataExport(info);
            }
        }
    }
    
    public synchronized void addDataExport(Collection<DataExportInfo> exports) throws DataExportAlreadyAddedException {
        for(DataExportInfo export : exports) {
            addDataExport(export);
        }
    }
    
    public synchronized void addDataExport(DataExportInfo export) throws DataExportAlreadyAddedException {
        if(export == null || export.isEmpty()) {
            throw new IllegalArgumentException("data export is empty or null");
        }
        
        if(this.dataExports.containsKey(export.getVirtualPath())) {
            throw new DataExportAlreadyAddedException("data export " + export.getVirtualPath() + " is already added");
        }
        
        DataExportInfo exportAdded = this.dataExports.put(export.getVirtualPath(), export);
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

    public synchronized void removeDataExport(DataExportInfo export) {
        if(export == null || export.isEmpty()) {
            throw new IllegalArgumentException("data export is empty or null");
        }
        
        removeDataExport(export.getVirtualPath());
    }
    
    public synchronized void removeDataExport(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        DataExportInfo removed = this.dataExports.remove(vpath);
        if(removed != null) {
            raiseEventForRemoveDataExport(removed);
        }
    }

    private synchronized void raiseEventForAddDataExport(DataExportInfo export) {
        LOG.debug("data export added : " + export.toString());
        
        for(IDataExportConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addDataExport(this, export);
        }
    }
    
    private synchronized void raiseEventForRemoveDataExport(DataExportInfo export) {
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
