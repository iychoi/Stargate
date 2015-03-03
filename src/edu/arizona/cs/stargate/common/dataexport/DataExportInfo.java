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

package edu.arizona.cs.stargate.common.dataexport;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class DataExportInfo {
    private static final Log LOG = LogFactory.getLog(DataExportInfo.class);
    
    private Map<String, DataExportEntry> mappingTable = new HashMap<String, DataExportEntry>();
    
    private ArrayList<IDataExportConfigChangeEventHandler> configChangeEventHandlers = new ArrayList<IDataExportConfigChangeEventHandler>();
    
    private String name;
    
    DataExportInfo() {
        this.name = null;
    }
    
    public static DataExportInfo createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportInfo) serializer.fromJsonFile(file, DataExportInfo.class);
    }
    
    public static DataExportInfo createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (DataExportInfo) serializer.fromJson(json, DataExportInfo.class);
    }
    
    public DataExportInfo(DataExportInfo that) {
        this.name = that.name;
        this.mappingTable.putAll(that.mappingTable);
        this.configChangeEventHandlers.addAll(that.configChangeEventHandlers);
    }
    
    public DataExportInfo(String name) {
        this.name = name;
    }
    
    public DataExportInfo(String name, DataExportEntry[] entries) throws DataExportEntryAlreadyAddedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.name = name;
        
        if(entries != null) {
            for(DataExportEntry entry : entries) {
                addExportEntry(entry);
            }
        }
    }
    
    public synchronized String getName() {
        return this.name;
    }
    
    synchronized void setName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.name = name;
    }
    
    @JsonIgnore
    public synchronized int getExportEntryNumber() {
        return this.mappingTable.keySet().size();
    }
    
    @JsonProperty("entry")
    public synchronized Collection<DataExportEntry> getAllExportEntry() {
        return Collections.unmodifiableCollection(this.mappingTable.values());
    }
    
    @JsonIgnore
    public synchronized DataExportEntry getExportEntry(String virtualPath) {
        if(virtualPath == null || virtualPath.isEmpty()) {
            throw new IllegalArgumentException("virtualPath is empty or null");
        }
        
        return this.mappingTable.get(virtualPath);
    }
    
    @JsonIgnore
    public synchronized boolean hasExportEntry(String virtualPath) {
        if(virtualPath == null || virtualPath.isEmpty()) {
            throw new IllegalArgumentException("virtualPath is empty or null");
        }
        
        return this.mappingTable.containsKey(virtualPath);
    }
    
    public synchronized void removeAllExportEntry() {
        ArrayList<String> keys = new ArrayList<String>();
        
        Collection<DataExportEntry> values = this.mappingTable.values();
        for(DataExportEntry entry : values) {
            keys.add(entry.getVirtualPath());
        }
        
        for(String name : keys) {
            removeExportEntry(name);
        }
        
        keys.clear();
    }
    
    @JsonProperty("entry")
    public synchronized void addExportEntry(Collection<DataExportEntry> entries) throws DataExportEntryAlreadyAddedException {
        for(DataExportEntry entry : entries) {
            addExportEntry(entry);
        }
    }
    
    public synchronized void addExportEntry(DataExportEntry entry) throws DataExportEntryAlreadyAddedException {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is empty or null");
        }
        
        if(this.mappingTable.containsKey(entry.getVirtualPath())) {
            throw new DataExportEntryAlreadyAddedException("entry " + entry.getVirtualPath() + "is already added");
        }
        
        DataExportEntry deEntry = this.mappingTable.put(entry.getVirtualPath(), entry);
        if(deEntry != null) {
            raiseEventForAddExportEntry(deEntry);
        }
    }
    
    public synchronized void addConfigChangeEventHandler(IDataExportConfigChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(IDataExportConfigChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<IDataExportConfigChangeEventHandler> toberemoved = new ArrayList<IDataExportConfigChangeEventHandler>();
        
        for(IDataExportConfigChangeEventHandler handler : this.configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IDataExportConfigChangeEventHandler handler : toberemoved) {
            this.configChangeEventHandlers.remove(handler);
        }
    }

    public synchronized void removeExportEntry(DataExportEntry entry) {
        if(entry == null || entry.isEmpty()) {
            throw new IllegalArgumentException("entry is empty or null");
        }
        
        removeExportEntry(entry.getVirtualPath());
    }
    
    public synchronized void removeExportEntry(String virtualPath) {
        if(virtualPath == null || virtualPath.isEmpty()) {
            throw new IllegalArgumentException("virtualPath is empty or null");
        }
        
        DataExportEntry removed = this.mappingTable.remove(virtualPath);
        if(removed != null) {
            raiseEventForRemoveExportEntry(removed);
        }
    }

    private synchronized void raiseEventForAddExportEntry(DataExportEntry entry) {
        LOG.debug("export entry added : " + entry.toString());
        
        for(IDataExportConfigChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addDataExport(this, entry);
        }
    }
    
    private synchronized void raiseEventForRemoveExportEntry(DataExportEntry entry) {
        LOG.debug("export entry removed : " + entry.toString());
        
        for(IDataExportConfigChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeDataExport(this, entry);
        }
    }
    
    @Override
    public String toString() {
        return this.name;
    }

    @JsonIgnore
    public boolean isEmpty() {
        if(this.name == null || this.name.isEmpty()) {
            return true;
        }
        return false;
    }
}
