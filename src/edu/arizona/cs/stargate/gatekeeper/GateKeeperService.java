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

package edu.arizona.cs.stargate.gatekeeper;

import edu.arizona.cs.stargate.gatekeeper.cluster.RemoteClusterManager;
import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.gatekeeper.dataexport.IDataExportConfigurationChangeEventHandler;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExportManager;
import edu.arizona.cs.stargate.common.NameUtils;
import edu.arizona.cs.stargate.common.TemporaryFileUtils;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.cluster.ClusterNode;
import edu.arizona.cs.stargate.common.LocalNodeInfoUtils;
import edu.arizona.cs.stargate.gatekeeper.cluster.NodeAlreadyAddedException;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.gatekeeper.distributed.DistributedService;
import edu.arizona.cs.stargate.gatekeeper.runtime.GateKeeperRuntimeInfo;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.intercluster.InterclusterRecipeSyncTask;
import edu.arizona.cs.stargate.gatekeeper.intercluster.RemoteGateKeeperClientManager;
import edu.arizona.cs.stargate.gatekeeper.restful.GateKeeperRestfulInterface;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeManager;
import edu.arizona.cs.stargate.gatekeeper.schedule.ScheduleManager;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class GateKeeperService {
    
    private static final Log LOG = LogFactory.getLog(GateKeeperService.class);
    
    private static GateKeeperService instance;
    
    private GateKeeperServiceConfiguration config;
    
    private GateKeeperRestfulInterface restfulInterface;
    
    private DistributedService distributedService;
    
    private LocalClusterManager localClusterManager;
    private RemoteClusterManager remoteClusterManager;
    private DataExportManager dataExportManager;
    private RecipeManager recipeManager;
    
    private ScheduleManager scheduleManager;
    private RemoteGateKeeperClientManager gatekeeperClientManager;
    
    private GateKeeperRuntimeInfo runtimeInfo;
    
    public static GateKeeperService getInstance(GateKeeperServiceConfiguration config) throws Exception {
        synchronized (GateKeeperService.class) {
            if(instance == null) {
                instance = new GateKeeperService(config);
            }
            return instance;
        }
    }
    
    public static GateKeeperService getInstance() throws ServiceNotStartedException {
        synchronized (GateKeeperService.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("GateKeeper service is not started");
            }
            return instance;
        }
    }
    
    GateKeeperService(GateKeeperServiceConfiguration config) throws Exception {
        if(config == null) {
            throw new Exception("GateKeeperServiceConfiguration is null. Failed to start GateKeeperService.");
        } else {
            this.config = config;
            this.config.setImmutable();
            
            this.distributedService = DistributedService.getInstance(config.getDistributedCacheServiceConfiguration());
            
            // start distributed cache service
            this.distributedService.start();
            
            this.localClusterManager = LocalClusterManager.getInstance();
            registerLocalClusterNodes(this.config.getLocalCluster());

            this.remoteClusterManager = RemoteClusterManager.getInstance();

            this.dataExportManager = DataExportManager.getInstance();
            this.recipeManager = RecipeManager.getInstance(this.config.getRecipeManagerConfiguration());

            addDataExportListener();

            this.dataExportManager.addDataExports(this.config.getDataExports());
            
            this.restfulInterface = GateKeeperRestfulInterface.getInstance();
            
            this.gatekeeperClientManager = RemoteGateKeeperClientManager.getInstance();
            this.scheduleManager = ScheduleManager.getInstance();
            
            registerRemoteClusters(this.config.getRemoteClusters());
        }
    }
    
    public synchronized void start() throws Exception {
        // start restful interface
        this.restfulInterface.start(this.config.getServicePort());
        
        // start schedules
        registerSchedules();
        
        // write runtime info
        writeRuntimeConfiguration();
    }
    
    public synchronized void stop() throws InterruptedException {
        // cleanup runtime info
        try {
            cleanupRuntimeConfiguration();
        } catch(Exception ex) {
            LOG.error(ex);
        }
        
        // stop restful interface
        try {
            this.restfulInterface.stop();
        } catch(Exception ex) {
            LOG.error(ex);
        }
        
        // stop distributed cache service
        try {
            this.distributedService.stop();
        } catch(Exception ex) {
            LOG.error(ex);
        }
    }
    
    private void writeRuntimeConfiguration() throws IOException {
        TemporaryFileUtils.prepareTempRoot();
        
        this.runtimeInfo = new GateKeeperRuntimeInfo();
        this.runtimeInfo.setServicePort(this.config.getServicePort());
        this.runtimeInfo.synchronize();
    }
    
    private void cleanupRuntimeConfiguration() throws IOException {
        TemporaryFileUtils.clearTempRoot();
    }
    
    private void registerLocalClusterNodes(Cluster cluster) {
        // name
        if(this.localClusterManager.getName() == null || this.localClusterManager.getName().isEmpty()) {
            if(cluster != null && cluster.getName() != null && !cluster.getName().isEmpty()) {
                this.localClusterManager.setName(this.config.getLocalCluster().getName());
            } else {
                this.localClusterManager.setName("Stargate_" + NameUtils.generateRandomString(10));
            }
        }
        
        // node
        if(cluster != null) {
            try {
                Collection<ClusterNode> nodes = this.config.getLocalCluster().getAllNodes();
                if(nodes != null) {
                    Iterator<ClusterNode> iterator = nodes.iterator();
                    int cnt = 0;
                    while(iterator.hasNext()) {
                        ClusterNode node = iterator.next();
                        
                        if(cnt < 1) {
                            this.localClusterManager.addNode(node, true);
                        } else {
                            LOG.warn("Ignoring adding a local node : " + node.toString());
                        }
                        cnt++;
                    }
                }
            } catch (NodeAlreadyAddedException ex) {
                LOG.error(ex);
            }
        } else {
            try {
                // autogenerate?
                ClusterNode node = LocalNodeInfoUtils.getClusterNodeFromPublicIP(this.config);
                this.localClusterManager.addNode(node, true);
            } catch (NodeAlreadyAddedException ex) {
                LOG.error(ex);
            } catch (URISyntaxException ex) {
                LOG.error(ex);
            }
        }
    }
    
    private void registerRemoteClusters(Collection<Cluster> clusters) {
        for(Cluster cluster : clusters) {
            Cluster remoteCluster = null;
            Collection<ClusterNode> nodes = cluster.getAllNodes();
            for(ClusterNode node : nodes) {
                try {
                    GateKeeperClient client = this.gatekeeperClientManager.getTempGateKeeperClient(node.getServiceURL());
                    remoteCluster = client.getRestfulClient().getClusterManagerClient().getLocalCluster();
                    client.stop();
                    
                    if(remoteCluster != null && remoteCluster.getName() != null && !remoteCluster.getName().isEmpty()) {
                        // register
                        this.remoteClusterManager.addCluster(remoteCluster);
                        break;
                    }
                } catch (Exception ex) {
                    LOG.warn("remote cluster service is unreachable - " + node.getServiceURL());
                }
            }
        }
    }
    
    private void addDataExportListener() {
        this.dataExportManager.addConfigChangeEventHandler(new IDataExportConfigurationChangeEventHandler(){

            @Override
            public String getName() {
                return "GateKeeperService";
            }

            @Override
            public void addDataExport(DataExportManager manager, DataExport info) {
                // pass to RecipeManager
                recipeManager.generateRecipe(info);
            }

            @Override
            public void removeDataExport(DataExportManager manager, DataExport info) {
                // pass to RecipeManager
                recipeManager.removeRecipe(info);
            }
        });
    }
    
    public GateKeeperServiceConfiguration getConfiguration() {
        return this.config;
    }
    
    public synchronized DistributedService getDistributedService() {
        return this.distributedService;
    }
    
    public synchronized LocalClusterManager getLocalClusterManager() {
        return this.localClusterManager;
    }
    
    public synchronized RemoteClusterManager getRemoteClusterManager() {
        return this.remoteClusterManager;
    }
    
    public synchronized DataExportManager getDataExportManager() {
        return this.dataExportManager;
    }
    
    public synchronized RecipeManager getRecipeManager() {
        return this.recipeManager;
    }
    
    public synchronized ScheduleManager getScheduleManager() {
        return this.scheduleManager;
    }
    
    public synchronized RemoteGateKeeperClientManager getRemoteGateKeeperClientManager() {
        return this.gatekeeperClientManager;
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperService";
    }

    private void registerSchedules() {
        this.scheduleManager.scheduleTask(new InterclusterRecipeSyncTask());
    }
}
