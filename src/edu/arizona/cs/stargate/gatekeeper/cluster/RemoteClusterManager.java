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

import edu.arizona.cs.stargate.gatekeeper.distributedcache.JsonReplicatedMap;
import edu.arizona.cs.stargate.common.ClusterAlreadyAddedException;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.distributedcache.DistributedCacheService;
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
    
    private static final String REMOTECLUSTERMANAGER_MAP_ID = "RemoteClusterManager";
    
    private static RemoteClusterManager instance;
    
    private JsonReplicatedMap<String, Cluster> remoteClusters;
    
    public static RemoteClusterManager getInstance() {
        synchronized (RemoteClusterManager.class) {
            if(instance == null) {
                instance = new RemoteClusterManager();
            }
            return instance;
        }
    }
    
    RemoteClusterManager() {
        try {
            DistributedCacheService distributedCacheService = DistributedCacheService.getInstance();
            this.remoteClusters = new JsonReplicatedMap<String, Cluster>(distributedCacheService.getReplicatedMap(REMOTECLUSTERMANAGER_MAP_ID), Cluster.class);
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            throw new RuntimeException(ex);
        }
    }
    
    public synchronized Collection<Cluster> getAllClusters() {
        return Collections.unmodifiableCollection(this.remoteClusters.values());
    }
    
    public synchronized Cluster getCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.remoteClusters.get(name);
    }
    
    public synchronized boolean hasCluster(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.remoteClusters.containsKey(name);
    }
    
    public synchronized void removeAllClusters() {
        ArrayList<String> toberemoved = new ArrayList<String>();
        Set<String> keys = this.remoteClusters.keySet();
        toberemoved.addAll(keys);
        
        for(String key : toberemoved) {
            Cluster cluster = this.remoteClusters.get(key);

            if(cluster != null) {
                removeCluster(cluster);
            }
        }
    }
    
    public synchronized void addClusters(Collection<Cluster> clusters) throws ClusterAlreadyAddedException {
        for(Cluster cluster : clusters) {
            addCluster(cluster);
        }
    }
    
    public synchronized void addCluster(Cluster cluster) throws ClusterAlreadyAddedException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty or null");
        }
        
        if(this.remoteClusters.containsKey(cluster.getName())) {
            throw new ClusterAlreadyAddedException("cluster " + cluster.getName() + " is already added");
        }
        
        this.remoteClusters.put(cluster.getName(), cluster);
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
        
        this.remoteClusters.remove(name);
    }
    
    public synchronized void updateCluster(Cluster cluster) {
        this.remoteClusters.remove(cluster.getName());
        this.remoteClusters.put(cluster.getName(), cluster);
    }

    @Override
    public synchronized String toString() {
        return "RemoteClusterManager";
    }
}
