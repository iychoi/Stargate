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

import edu.arizona.cs.stargate.schedule.AScheduledLeaderTask;
import edu.arizona.cs.stargate.policy.ClusterPolicy;
import edu.arizona.cs.stargate.policy.PolicyManager;
import edu.arizona.cs.stargate.transport.ATransportClient;
import edu.arizona.cs.stargate.transport.TransportManager;
import java.io.IOException;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RemoteClusterSyncTask extends AScheduledLeaderTask {

    private static final Log LOG = LogFactory.getLog(RemoteClusterSyncTask.class);
    
    private PolicyManager policyManager;
    private ClusterManager clusterManager;
    private TransportManager transportManager;
    
    private long syncInterval;
    
    public RemoteClusterSyncTask(PolicyManager policyManager, ClusterManager clusterManager, TransportManager transportManager) throws IOException {
        if(policyManager == null) {
            throw new IllegalArgumentException("policyManager is null");
        }
        
        if(clusterManager == null) {
            throw new IllegalArgumentException("clusterManager is null");
        }
        
        if(transportManager == null) {
            throw new IllegalArgumentException("transportManager is null");
        }
        
        this.policyManager = policyManager;
        this.clusterManager = clusterManager;
        this.transportManager = transportManager;
        
        this.syncInterval = ClusterPolicy.getClusterNodeSyncInterval(this.policyManager);
    }
    
    @Override
    public void run() {
        LOG.info("Start - RemoteClusterSyncTask");

        try {
            Collection<RemoteCluster> clusters = this.clusterManager.getRemoteCluster();
            for(RemoteCluster cluster : clusters) {
                try {
                    ATransportClient tclient = this.transportManager.getTransportClient(this.clusterManager, cluster, this.policyManager);
                    if(tclient != null) {
                        RemoteCluster remoteCluster = tclient.getCluster();
                        this.clusterManager.updateRemoteCluster(remoteCluster);
                    }
                } catch (IOException ex) {
                    LOG.error(ex);
                }
            }
        } catch (IOException ex) {
            LOG.error(ex);
        }
            
        LOG.info("Done - RemoteClusterSyncTask");
    }
    
    @Override
    public String getName() {
        return "RemoteClusterSyncTask";
    }

    @Override
    public boolean isRepeatedTask() {
        return true;
    }

    @Override
    public long getDelay() {
        return 0;
    }

    @Override
    public long getInterval() {
        return this.syncInterval;
    }
}
