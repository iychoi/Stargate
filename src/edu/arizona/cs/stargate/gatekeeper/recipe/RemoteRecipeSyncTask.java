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
package edu.arizona.cs.stargate.gatekeeper.recipe;

import edu.arizona.cs.stargate.gatekeeper.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.cluster.ClusterNode;
import edu.arizona.cs.stargate.gatekeeper.cluster.RemoteClusterManager;
import edu.arizona.cs.stargate.gatekeeper.intercluster.RemoteGateKeeperClientManager;
import edu.arizona.cs.stargate.gatekeeper.schedule.ALeaderScheduledTask;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class RemoteRecipeSyncTask extends ALeaderScheduledTask {
    private static final Log LOG = LogFactory.getLog(RemoteRecipeSyncTask.class);
    
    private static final int REMOTE_RECIPE_SYNC_PERIOD_SEC = 600;
    
    private RemoteClusterManager remoteClusterManager;
    private RemoteRecipeManager remoteRecipeManager;
    private RemoteGateKeeperClientManager gatekeeperClientManager;
    
    public RemoteRecipeSyncTask(RemoteClusterManager remoteClusterManager, RemoteRecipeManager remoteRecipeManager, RemoteGateKeeperClientManager gatekeeperClientManager) {
        this.remoteClusterManager = remoteClusterManager;
        this.remoteRecipeManager = remoteRecipeManager;
        this.gatekeeperClientManager = gatekeeperClientManager;
    }
    
    @Override
    public void process() {
        LOG.info("Start - RemoteRecipeSyncTask");
        
        Collection<Cluster> remoteClusters = this.remoteClusterManager.getAllClusters();
        for(Cluster cluster : remoteClusters) {
            Collection<RemoteRecipe> recipes = retrieveRecipes(cluster);
            if(recipes != null) {
                for(RemoteRecipe recipe : recipes) {
                    this.remoteRecipeManager.updateRecipe(recipe);
                }
            }
        }
        
        LOG.info("Done - RemoteRecipeSyncTask");
    }

    private Collection<RemoteRecipe> retrieveRecipes(Cluster cluster) {
        Collection<ClusterNode> nodes = cluster.getAllNodes();
        for(ClusterNode node : nodes) {
            if(!node.isUnreachable()) {
                try {
                    GateKeeperClient client = this.gatekeeperClientManager.getTempGateKeeperClient(node.getServiceURL());
                    Collection<RemoteRecipe> recipes = client.getRestfulClient().getInterClusterDataTransferClient().getAllRecipes();
                    client.stop();

                    if(recipes != null) {
                        return recipes;
                    }
                } catch (Exception ex) {
                    LOG.info("remote cluster service is unreachable - " + node.getServiceURL());
                    node.setUnrechable(true);
                    this.remoteClusterManager.updateCluster(cluster);
                }
            }
        }
        return null;
    }
    
    @Override
    public String getName() {
        return "RemoteRecipeSyncTask";
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
        return REMOTE_RECIPE_SYNC_PERIOD_SEC;
    }
}
