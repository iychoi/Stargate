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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    
    private static DataExportManager instance;
    
    private Map<String, DataExportInfo> dataExportTable = new HashMap<String, DataExportInfo>();
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
        
    }
    
    public synchronized Collection<DataExportInfo> getAllDataExportInfo() {
        return Collections.unmodifiableCollection(this.dataExportTable.values());
    }
    
    public synchronized DataExportInfo getDataExportInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.dataExportTable.get(name);
    }
    
    public synchronized boolean hasDataExportInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.dataExportTable.containsKey(name);
    }
    
    public synchronized void removeAllDataExport() {
        ArrayList<String> toberemoved = new ArrayList<String>();
        Set<String> keys = this.dataExportTable.keySet();
        toberemoved.addAll(keys);
        
        for(String key : toberemoved) {
            DataExportInfo info = this.dataExportTable.get(key);

            if(info != null) {
                removeDataExport(info);
            }
        }
    }
    
    public synchronized void addDataExport(Collection<DataExportInfo> info) throws DataExportAlreadyAddedException {
        for(DataExportInfo i : info) {
            addDataExport(i);
        }
    }
    
    public synchronized void addDataExport(DataExportInfo info) throws DataExportAlreadyAddedException {
        if(info == null || info.isEmpty()) {
            throw new IllegalArgumentException("data export is empty or null");
        }
        
        if(this.dataExportTable.containsKey(info.getName())) {
            throw new DataExportAlreadyAddedException("data export " + info.getName() + " is already added");
        }
        
        DataExportInfo put = this.dataExportTable.put(info.getName(), info);
        if(put != null) {
            raiseEventForAddDataExport(put);
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

    public synchronized void removeDataExport(DataExportInfo info) {
        if(info == null || info.isEmpty()) {
            throw new IllegalArgumentException("data export is empty or null");
        }
        
        removeDataExport(info.getName());
    }
    
    public synchronized void removeDataExport(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        DataExportInfo removed = this.dataExportTable.remove(name);
        if(removed != null) {
            raiseEventForRemoveDataExport(removed);
        }
    }

    private synchronized void raiseEventForAddDataExport(DataExportInfo info) {
        LOG.debug("data export added : " + info.toString());
        
        for(IDataExportConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addDataExport(this, info);
        }
    }
    
    private synchronized void raiseEventForRemoveDataExport(DataExportInfo info) {
        LOG.debug("data export removed : " + info.toString());
        
        for(IDataExportConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeDataExport(this, info);
        }
    }
    
    @Override
    public synchronized String toString() {
        return "DataExportManager";
    }
}
