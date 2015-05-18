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

package edu.arizona.cs.stargate.gatekeeper.intercluster;

import edu.arizona.cs.stargate.common.NodeSelectionUtils;
import edu.arizona.cs.stargate.common.DateTimeUtils;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.cluster.ClusterNode;
import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.gatekeeper.cluster.RemoteClusterManager;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperClientConfiguration;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RemoteGateKeeperClientManager {
    private static final Log LOG = LogFactory.getLog(RemoteGateKeeperClientManager.class);

    private static final long REMOTECLUSTER_CONNECTION_EXPIRATION_PERIOD = 600;
    private static final long REMOTECLUSTER_UNREACHABLE_RECORD_EXPIRATION_PERIOD = 600;
    
    private static RemoteGateKeeperClientManager instance;
    
    private Map<String, GateKeeperClient> gatekeeperClients = new HashMap<String, GateKeeperClient>();
    private Map<String, Long> lastUpdates = new HashMap<String, Long>();
    private Map<URI, Long> unreachableURIs = new HashMap<URI, Long>();
    
    public static RemoteGateKeeperClientManager getInstance() {
        synchronized (RemoteGateKeeperClientManager.class) {
            if(instance == null) {
                instance = new RemoteGateKeeperClientManager();
            }
            return instance;
        }
    }
    
    RemoteGateKeeperClientManager() {
    }
    
    public synchronized GateKeeperClient getTempGateKeeperClient(URI serviceURL) {
        GateKeeperClientConfiguration gateKeeperClientConfiguration = new GateKeeperClientConfiguration(serviceURL);
        GateKeeperClient gateKeeperClient = new GateKeeperClient(gateKeeperClientConfiguration);
        gateKeeperClient.start();

        return gateKeeperClient;
    }
    
    public synchronized GateKeeperClient getGateKeeperClient(String cluster) {
        boolean needUpdate = false;
        long currentTime = DateTimeUtils.getCurrentTime();
        
        Long lastupdated = this.lastUpdates.get(cluster);
        if(lastupdated == null || DateTimeUtils.timeElapsedSecond(lastupdated, currentTime, REMOTECLUSTER_CONNECTION_EXPIRATION_PERIOD)) {
            needUpdate = true;
        }
        
        if(!needUpdate) {
            GateKeeperClient client = this.gatekeeperClients.get(cluster);
            if(client.getRestfulClient().checkLive()) {
                this.lastUpdates.remove(cluster);
                this.lastUpdates.put(cluster, currentTime);
                return client;
            } else {
                needUpdate = true;
                client.stop();
                this.lastUpdates.remove(cluster);
                this.gatekeeperClients.remove(cluster);
                this.unreachableURIs.put(client.getConfiguration().getServiceURI(), currentTime);
            }
        }
        
        LocalClusterManager lcm = LocalClusterManager.getInstance();
        RemoteClusterManager rcm = RemoteClusterManager.getInstance();
        Cluster rcluster = rcm.getCluster(cluster);
        if(rcluster == null) {
            return null;
        }

        ArrayList<ClusterNode> contactList = new ArrayList<ClusterNode>();
        for(ClusterNode rnode : rcluster.getAllNodes()) {
            Long lastUnreachableTime = this.unreachableURIs.get(rnode.getServiceURL());
            if(lastUnreachableTime == null || DateTimeUtils.timeElapsedSecond(lastUnreachableTime, currentTime, REMOTECLUSTER_UNREACHABLE_RECORD_EXPIRATION_PERIOD)) {
                contactList.add(rnode);
                this.unreachableURIs.remove(rnode.getServiceURL());
            }
        }
        
        while(contactList.size() > 0) {
            ClusterNode bestRNode = NodeSelectionUtils.selectBestNode(lcm.getLocalNode(), contactList);
            
            GateKeeperClientConfiguration gateKeeperClientConfiguration = new GateKeeperClientConfiguration(bestRNode.getServiceURL());
            GateKeeperClient gateKeeperClient = new GateKeeperClient(gateKeeperClientConfiguration);
            gateKeeperClient.start();
            
            if(gateKeeperClient.getRestfulClient().checkLive()) {
                this.lastUpdates.put(cluster, currentTime);
                this.gatekeeperClients.put(cluster, gateKeeperClient);

                return gateKeeperClient;
            } else {
                gateKeeperClient.stop();
                
                this.unreachableURIs.put(bestRNode.getServiceURL(), currentTime);
            }
        }
        
        return null;
    }
    
    @Override
    public synchronized String toString() {
        return "RemoteGateKeeperClientManager";
    }
}
