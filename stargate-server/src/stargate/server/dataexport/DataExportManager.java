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

package stargate.server.dataexport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.datastore.AReplicatedDataStore;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.server.datastore.DataStoreManager;

/**
 *
 * @author iychoi
 */
public class DataExportManager {
    private static final Log LOG = LogFactory.getLog(DataExportManager.class);
    
    private static final String DATAEXPORTMANAGER_MAP_ID = "DataExportManager_Data_Export";
    
    private static DataExportManager instance;
    
    private DataStoreManager datastoreManager;

    private AReplicatedDataStore dataExport;
    private ArrayList<IDataExportEventHandler> eventHandlers = new ArrayList<IDataExportEventHandler>();
    protected long lastUpdateTime;
    
    public static DataExportManager getInstance(DataStoreManager datastoreManager) {
        synchronized (DataExportManager.class) {
            if(instance == null) {
                instance = new DataExportManager(datastoreManager);
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
    
    DataExportManager(DataStoreManager datastoreManager) {
        if(datastoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is null");
        }
        
        this.datastoreManager = datastoreManager;
        
        this.dataExport = this.datastoreManager.getReplicatedDataStore(DATAEXPORTMANAGER_MAP_ID, DataExportEntry.class);
    }
    
    public synchronized int getDataExportCount() {
        return this.dataExport.size();
    }
    
    public synchronized Collection<DataExportEntry> getDataExport() throws IOException {
        List<DataExportEntry> entries = new ArrayList<DataExportEntry>();
        Set<String> keySet = this.dataExport.keySet();
        for(String key : keySet) {
            DataExportEntry dee = (DataExportEntry) this.dataExport.get(key);
            entries.add(dee);
        }

        return Collections.unmodifiableCollection(entries);
    }
    
    public synchronized DataExportEntry getDataExport(String vpath) throws IOException {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        return (DataExportEntry) this.dataExport.get(vpath);
    }
    
    public synchronized void addDataExport(Collection<DataExportEntry> entry) throws DataExportEntryAlreadyAddedException, IOException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        List<DataExportEntry> failedEntry = new ArrayList<DataExportEntry>();
        
        for(DataExportEntry e : entry) {
            try {
                addDataExport(e);
            } catch(DataExportEntryAlreadyAddedException ex) {
                failedEntry.add(e);
            }
        }
        
        if(!failedEntry.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(DataExportEntry n : failedEntry) {
                if(sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(n.getVirtualPath());
            }
            throw new DataExportEntryAlreadyAddedException("dataexport entries (" + sb.toString() + ") are already added");
        }
    }
    
    public synchronized void addDataExport(DataExportEntry entry) throws DataExportEntryAlreadyAddedException, IOException {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is empty or null");
        }

        if(this.dataExport.containsKey(entry.getVirtualPath())) {
            throw new DataExportEntryAlreadyAddedException("dataexport entry " + entry.getVirtualPath() + " is already added");
        }
        
        this.dataExport.put(entry.getVirtualPath(), entry);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        
        raiseEventForDataExportEntryAdded(entry);
    }
    
    public synchronized void clearDataExport() throws IOException {
        Set<String> keys = this.dataExport.keySet();
        for(String key : keys) {
            removeDataExport((String) key);
        }
    }
    
    public synchronized void removeDataExport(DataExportEntry entry) throws IOException {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is empty or null");
        }
        
        removeDataExport(entry.getVirtualPath());
    }
    
    public synchronized void removeDataExport(String vpath) throws IOException {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        DataExportEntry removedEntry = (DataExportEntry) this.dataExport.get(vpath);
        if(removedEntry != null) {
            this.dataExport.remove(vpath);

            this.lastUpdateTime = DateTimeUtils.getCurrentTime();

            raiseEventForDataExportEntryRemoved(removedEntry);
        }
    }
    
    public synchronized boolean hasDataExport(String vpath) {
        if(vpath == null || vpath.isEmpty()) {
            throw new IllegalArgumentException("vpath is empty or null");
        }
        
        return this.dataExport.containsKey(vpath);
    }
    
    public synchronized boolean isEmpty() {
        if(this.dataExport == null || this.dataExport.isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    public synchronized void addEventHandler(IDataExportEventHandler eventHandler) {
        this.eventHandlers.add(eventHandler);
    }
    
    public synchronized void removeEventHandler(IDataExportEventHandler eventHandler) {
        this.eventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeEventHandler(String handlerName) {
        List<IDataExportEventHandler> toberemoved = new ArrayList<IDataExportEventHandler>();
        
        for(IDataExportEventHandler handler : this.eventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IDataExportEventHandler handler : toberemoved) {
            this.eventHandlers.remove(handler);
        }
    }

    private synchronized void raiseEventForDataExportEntryAdded(DataExportEntry entry) {
        LOG.debug("data export entry added : " + entry.toString());
        
        for(IDataExportEventHandler handler: this.eventHandlers) {
            handler.dataExportEntryAdded(this, entry);
        }
    }
    
    private synchronized void raiseEventForDataExportEntryRemoved(DataExportEntry entry) {
        LOG.debug("data export entry removed : " + entry.toString());
        
        for(IDataExportEventHandler handler: this.eventHandlers) {
            handler.dataExportEntryRemoved(this, entry);
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
    
    @Override
    public synchronized String toString() {
        return "DataExportManager";
    }
}
