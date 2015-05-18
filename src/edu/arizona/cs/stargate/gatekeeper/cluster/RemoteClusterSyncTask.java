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

import edu.arizona.cs.stargate.common.DateTimeUtils;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.intercluster.RemoteGateKeeperClientManager;
import edu.arizona.cs.stargate.gatekeeper.schedule.ALeaderScheduledTask;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RemoteClusterSyncTask extends ALeaderScheduledTask {

    private static final Log LOG = LogFactory.getLog(RemoteClusterSyncTask.class);
    
    private static final int REMOTE_CLUSTER_NODE_SYNC_PERIOD_SEC = 60;
    
    private RemoteClusterManager remoteClusterManager;
    private RemoteGateKeeperClientManager gatekeeperClientManager;
    
    public RemoteClusterSyncTask(RemoteClusterManager remoteClusterManager, RemoteGateKeeperClientManager gatekeeperClientManager) {
        this.remoteClusterManager = remoteClusterManager;
        this.gatekeeperClientManager = gatekeeperClientManager;
    }
    
    @Override
    public void process() {
        LOG.info("Start - RemoteClusterNodeSyncTask");
        
        long currentTime = DateTimeUtils.getCurrentTime();
        
        Collection<Cluster> pendingClusters = this.remoteClusterManager.getAllPendingClusters();
        
        for(Cluster cluster : pendingClusters) {
            Cluster remoteCluster = retrieveClusterInfo(cluster);
            if(remoteCluster != null) {
                // update time
                remoteCluster.setLastContact(currentTime);
                this.remoteClusterManager.completeClusterSync(cluster, remoteCluster);
            }
        }
        
        Collection<Cluster> remoteClusters = this.remoteClusterManager.getAllClusters();
        for(Cluster cluster : remoteClusters) {
            if(DateTimeUtils.timeElapsedSecond(cluster.getLastContact(), currentTime, REMOTE_CLUSTER_NODE_SYNC_PERIOD_SEC)) {
                Cluster remoteCluster = retrieveClusterInfo(cluster);
                if (remoteCluster != null) {
                    // update time
                    remoteCluster.setLastContact(currentTime);
                    this.remoteClusterManager.updateCluster(remoteCluster);
                }
            }
        }
        
        LOG.info("Done - RemoteClusterNodeSyncTask");
    }
    
    private Cluster retrieveClusterInfo(Cluster cluster) {
        Cluster remoteCluster = null;
        
        Collection<ClusterNode> nodes = cluster.getAllNodes();
        for(ClusterNode node : nodes) {
            try {
                GateKeeperClient client = this.gatekeeperClientManager.getTempGateKeeperClient(node.getServiceURL());
                remoteCluster = client.getRestfulClient().getClusterManagerClient().getLocalCluster();
                client.stop();

                if(remoteCluster != null && remoteCluster.getName() != null && !remoteCluster.getName().isEmpty()) {
                    break;
                }
            } catch (Exception ex) {
                LOG.info("remote cluster service is unreachable - " + node.getServiceURL());
            }
        }
        return remoteCluster;
    }
    
    @Override
    public String getName() {
        return "RemoteClusterNodeSyncTask";
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
    public long getPeriod() {
        return REMOTE_CLUSTER_NODE_SYNC_PERIOD_SEC;
    }
    
}
