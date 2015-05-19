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

package edu.arizona.cs.stargate.gatekeeper.cluster;

import edu.arizona.cs.stargate.gatekeeper.distributed.JsonReplicatedMap;
import edu.arizona.cs.stargate.common.ClusterAlreadyAddedException;
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
public class RemoteClusterManager {

    private static final Log LOG = LogFactory.getLog(RemoteClusterManager.class);
    
    private static final String REMOTECLUSTERMANAGER_CLUSTERS_MAP_ID = "RemoteClusterManager_Clusters";
    private static final String REMOTECLUSTERMANAGER_PENDING_CLUSTERS_MAP_ID = "RemoteClusterManager_Pending_Clusters";
    
    private static RemoteClusterManager instance;

    private DistributedService distributedService;
    
    private JsonReplicatedMap<String, Cluster> clusters;
    private JsonReplicatedMap<String, Cluster> pendingClusters;
    
    private boolean updated;
    
    public static RemoteClusterManager getInstance(DistributedService distributedService) {
        synchronized (RemoteClusterManager.class) {
            if(instance == null) {
                instance = new RemoteClusterManager(distributedService);
            }
            return instance;
        }
    }
    
    public static RemoteClusterManager getInstance() throws ServiceNotStartedException {
        synchronized (RemoteClusterManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("RemoteClusterManager is not started");
            }
            return instance;
        }
    }
    
    RemoteClusterManager(DistributedService distributedService) {
        this.distributedService = distributedService;
        this.clusters = new JsonReplicatedMap<String, Cluster>(this.distributedService.getReplicatedMap(REMOTECLUSTERMANAGER_CLUSTERS_MAP_ID), Cluster.class);
        this.pendingClusters = new JsonReplicatedMap<String, Cluster>(this.distributedService.getReplicatedMap(REMOTECLUSTERMANAGER_PENDING_CLUSTERS_MAP_ID), Cluster.class);
    }
    
    public synchronized Collection<Cluster> getAllClusters() {
        return Collections.unmodifiableCollection(this.clusters.values());
    }
    
    public synchronized Cluster getCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.clusters.get(name);
    }
    
    public synchronized boolean hasCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.clusters.containsKey(name);
    }
    
    public synchronized void removeAllClusters() {
        ArrayList<String> toberemoved = new ArrayList<String>();
        Set<String> keys = this.clusters.keySet();
        toberemoved.addAll(keys);
        
        for(String key : toberemoved) {
            Cluster cluster = this.clusters.get(key);

            if(cluster != null) {
                removeCluster(cluster);
            }
        }
        
        this.updated = true;
    }
    
    public synchronized void addCluster(Collection<Cluster> clusters) throws ClusterAlreadyAddedException {
        for(Cluster cluster : clusters) {
            addCluster(cluster);
        }
    }
    
    public synchronized void addCluster(Cluster cluster) throws ClusterAlreadyAddedException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        if(this.clusters.containsKey(cluster.getName())) {
            throw new ClusterAlreadyAddedException("cluster " + cluster.getName() + " is already added");
        }
        
        this.clusters.put(cluster.getName(), cluster);
        
        this.updated = true;
    }
    
    public synchronized void removeCluster(Cluster cluster) {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        removeCluster(cluster.getName());
    }
    
    public synchronized void removeCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.clusters.remove(name);
        
        this.updated = true;
    }
    
    public synchronized void updateCluster(Cluster cluster) {
        this.clusters.remove(cluster.getName());
        this.clusters.put(cluster.getName(), cluster);
        
        this.updated = true;
    }
    
    public synchronized void addPendingCluster(Collection<Cluster> clusters) throws ClusterAlreadyAddedException {
        for(Cluster cluster : clusters) {
            addPendingCluster(cluster);
        }
    }
    
    public synchronized void addPendingCluster(Cluster cluster) throws ClusterAlreadyAddedException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        if(this.pendingClusters.containsKey(cluster.getName())) {
            throw new ClusterAlreadyAddedException("cluster " + cluster.getName() + " is already added");
        }
        
        this.pendingClusters.put(cluster.getName(), cluster);
    }
    
    public synchronized Collection<Cluster> getAllPendingClusters() {
        return this.pendingClusters.values();
    }
    
    public synchronized void completeClusterSync(Cluster prev, Cluster cluster) {
        if(!this.clusters.containsKey(cluster.getName())) {
            this.clusters.put(cluster.getName(), cluster);
        } else {
            updateCluster(cluster);
        }
        
        this.pendingClusters.remove(prev.getName());
        
        this.updated = true;
    }
    
    public synchronized void setUpdated(boolean updated) {
        this.updated = updated;
    }
    
    public synchronized boolean getUpdated() {
        return this.updated;
    }

    @Override
    public synchronized String toString() {
        return "RemoteClusterManager";
    }
}
