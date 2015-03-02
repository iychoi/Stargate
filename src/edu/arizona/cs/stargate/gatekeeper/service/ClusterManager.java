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

import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
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
public class ClusterManager {

    private static final Log LOG = LogFactory.getLog(ClusterManager.class);
    
    private static ClusterManager instance;
    
    private Map<String, ClusterInfo> remoteClusterTable = new HashMap<String, ClusterInfo>();
    private ArrayList<IClusterConfigurationChangeEventHandler> configChangeEventHandlers = new ArrayList<IClusterConfigurationChangeEventHandler>();
    private ClusterInfo localCluster;
    
    public static ClusterManager getInstance() {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                instance = new ClusterManager();
            }
            return instance;
        }
    }
    
    ClusterManager() {
        this.localCluster = null;
    }
    
    public synchronized ClusterInfo getLocalClusterInfo() {
        return this.localCluster;
    }
    
    public synchronized void setLocalCluster(ClusterInfo cluster) {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is null or empty");
        }
        
        this.localCluster = cluster;
        raiseEventForSetLocalCluster(cluster);
    }
    
    public synchronized int getRemoteClusterNumber() {
        return this.remoteClusterTable.keySet().size();
    }
    
    public synchronized Collection<ClusterInfo> getAllRemoteClusterInfo() {
        return Collections.unmodifiableCollection(this.remoteClusterTable.values());
    }
    
    public synchronized ClusterInfo getRemoteClusterInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.remoteClusterTable.get(name);
    }
    
    public synchronized boolean hasRemoteClusterInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.remoteClusterTable.containsKey(name);
    }
    
    public synchronized void removeAllRemoteCluster() {
        Set<String> keys = this.remoteClusterTable.keySet();
        for(String key : keys) {
            ClusterInfo cluster = this.remoteClusterTable.get(key);
            
            removeRemoteCluster(cluster);
        }
    }
    
    public synchronized void addRemoteCluster(ClusterInfo cluster) throws ClusterAlreadyAddedException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        if(this.remoteClusterTable.containsKey(cluster.getName())) {
            throw new ClusterAlreadyAddedException("cluster " + cluster.getName() + "is already added");
        }
        
        ClusterInfo put = this.remoteClusterTable.put(cluster.getName(), cluster);
        if(put != null) {
            raiseEventForAddRemoteCluster(put);
        }
    }
    
    public synchronized void addConfigChangeEventHandler(IClusterConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(IClusterConfigurationChangeEventHandler eventHandler) {
        this.configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<IClusterConfigurationChangeEventHandler> toberemoved = new ArrayList<IClusterConfigurationChangeEventHandler>();
        
        for(IClusterConfigurationChangeEventHandler handler : this.configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IClusterConfigurationChangeEventHandler handler : toberemoved) {
            this.configChangeEventHandlers.remove(handler);
        }
    }

    public synchronized void removeRemoteCluster(ClusterInfo cluster) {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        removeRemoteCluster(cluster.getName());
    }
    
    public synchronized void removeRemoteCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        ClusterInfo removed = this.remoteClusterTable.remove(name);
        if(removed != null) {
            raiseEventForRemoveRemoteCluster(removed);
        }
    }

    private synchronized void raiseEventForAddRemoteCluster(ClusterInfo cluster) {
        LOG.debug("remoted cluster added : " + cluster.toString());
        
        for(IClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.addRemoteCluster(this, cluster);
        }
    }
    
    private synchronized void raiseEventForRemoveRemoteCluster(ClusterInfo cluster) {
        LOG.debug("remoted cluster removed : " + cluster.toString());
        
        for(IClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.removeRemoteCluster(this, cluster);
        }
    }
    
    private synchronized void raiseEventForSetLocalCluster(ClusterInfo cluster) {
        LOG.debug("local cluster set : " + cluster.toString());
        
        for(IClusterConfigurationChangeEventHandler handler: this.configChangeEventHandlers) {
            handler.setLocalCluster(this, cluster);
        }
    }
    
    @Override
    public synchronized String toString() {
        return "ClusterManager : " + this.localCluster.toString();
    }
}
