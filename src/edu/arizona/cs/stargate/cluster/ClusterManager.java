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

package edu.arizona.cs.stargate.cluster;

import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.common.utils.DateTimeUtils;
import edu.arizona.cs.stargate.datastore.AReplicatedDataStore;
import edu.arizona.cs.stargate.datastore.DataStoreManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ClusterManager {

    private static final Log LOG = LogFactory.getLog(ClusterManager.class);
    
    private static final String CLUSTERMANAGER_REMOTE_CLUSTER_MAP_ID = "ClusterManager_Remote_Cluster";
    
    private static ClusterManager instance;

    private DataStoreManager datastoreManager;
    
    private LocalClusterManager localClusterManager;
    private AReplicatedDataStore remoteCluster;
    private ArrayList<IRemoteClusterEventHandler> eventHandlers = new ArrayList<IRemoteClusterEventHandler>();
    protected long lastUpdateTime;
    
    public static ClusterManager getInstance(DataStoreManager datastoreManager) {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                instance = new ClusterManager(datastoreManager);
            }
            return instance;
        }
    }
    
    public static ClusterManager getInstance() throws ServiceNotStartedException {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("ClusterManager is not started");
            }
            return instance;
        }
    }
    
    ClusterManager(DataStoreManager datastoreManager) {
        if(datastoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is null");
        }
        
        this.datastoreManager = datastoreManager;
        
        this.localClusterManager = LocalClusterManager.getInstance(this.datastoreManager);
        this.remoteCluster = this.datastoreManager.getReplicatedDataStore(CLUSTERMANAGER_REMOTE_CLUSTER_MAP_ID, String.class, RemoteCluster.class);
    }
    
    public synchronized LocalClusterManager getLocalClusterManager() {
        return this.localClusterManager;
    }
    
    public synchronized int getRemoteClusterCount() {
        return this.remoteCluster.size();
    }
    
    public synchronized Collection<RemoteCluster> getRemoteCluster() throws IOException {
        List<RemoteCluster> clusters = new ArrayList<RemoteCluster>();
        Set<Object> keySet = this.remoteCluster.keySet();
        for(Object key : keySet) {
            RemoteCluster rc = (RemoteCluster) this.remoteCluster.get(key);
            clusters.add(rc);
        }

        return Collections.unmodifiableCollection(clusters);
    }
    
    public synchronized RemoteCluster getRemoteCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return (RemoteCluster) this.remoteCluster.get(name);
    }
    
    public synchronized boolean hasRemoteCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.remoteCluster.containsKey(name);
    }
    
    public synchronized void clearRemoteCluster() throws IOException {
        Set<Object> keys = this.remoteCluster.keySet();
        for(Object key : keys) {
            RemoteCluster cluster = (RemoteCluster) this.remoteCluster.get(key);
            if(cluster != null) {
                removeRemoteCluster(cluster);
            }
        }
    }
    
    public synchronized void addRemoteCluster(Collection<RemoteCluster> cluster) throws ClusterAlreadyAddedException, IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        List<RemoteCluster> failedCluster = new ArrayList<RemoteCluster>();
        
        for(RemoteCluster c : cluster) {
            try {
                addRemoteCluster(c);
            } catch(ClusterAlreadyAddedException ex) {
                failedCluster.add(c);
            }
        }
        
        if(!failedCluster.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(RemoteCluster c : failedCluster) {
                if(sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(c.getName());
            }
            throw new ClusterAlreadyAddedException("clusters (" + sb.toString() + ") are already added");
        }
    }
    
    public synchronized void addRemoteCluster(RemoteCluster cluster) throws ClusterAlreadyAddedException, IOException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        if(this.remoteCluster.containsKey(cluster.getName())) {
            throw new ClusterAlreadyAddedException("cluster " + cluster.getName() + " is already added");
        }
        
        this.remoteCluster.put(cluster.getName(), cluster);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        
        raiseEventForRemoteClusterAdded(cluster);
    }
    
    public synchronized void removeRemoteCluster(RemoteCluster cluster) throws IOException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        removeRemoteCluster(cluster.getName());
    }
    
    public synchronized void removeRemoteCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.remoteCluster.remove(name);
        
        RemoteCluster removedCluster = (RemoteCluster) this.remoteCluster.get(name);
        if(removedCluster != null) {
            this.remoteCluster.remove(name);

            this.lastUpdateTime = DateTimeUtils.getCurrentTime();

            raiseEventForRemoteClusterRemoved(removedCluster);
        }
    }
    
    public synchronized void updateRemoteCluster(RemoteCluster cluster) throws IOException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        this.remoteCluster.put(cluster.getName(), cluster);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        
        raiseEventForRemoteClusterUpdated(cluster);
    }
    
    public synchronized void addEventHandler(IRemoteClusterEventHandler eventHandler) {
        this.eventHandlers.add(eventHandler);
    }
    
    public synchronized void removeEventHandler(IRemoteClusterEventHandler eventHandler) {
        this.eventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeEventHandler(String handlerName) {
        List<IRemoteClusterEventHandler> toberemoved = new ArrayList<IRemoteClusterEventHandler>();
        
        for(IRemoteClusterEventHandler handler : this.eventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IRemoteClusterEventHandler handler : toberemoved) {
            this.eventHandlers.remove(handler);
        }
    }

    private synchronized void raiseEventForRemoteClusterAdded(RemoteCluster cluster) {
        LOG.debug("remote cluster added : " + cluster.getName());
        
        for(IRemoteClusterEventHandler handler: this.eventHandlers) {
            handler.remoteClusterAdded(this, cluster);
        }
    }
    
    private synchronized void raiseEventForRemoteClusterRemoved(RemoteCluster cluster) {
        LOG.debug("remote cluster removed : " + cluster.getName());
        
        for(IRemoteClusterEventHandler handler: this.eventHandlers) {
            handler.remoteClusterRemoved(this, cluster);
        }
    }
    
    private synchronized void raiseEventForRemoteClusterUpdated(RemoteCluster cluster) {
        LOG.debug("remote cluster updated : " + cluster.getName());
        
        for(IRemoteClusterEventHandler handler: this.eventHandlers) {
            handler.remoteClusterUpdated(this, cluster);
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
        return "ClusterManager";
    }
}
