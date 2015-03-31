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

import com.google.inject.Guice;
import com.google.inject.Stage;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.common.cluster.ClusterNodeInfo;
import edu.arizona.cs.stargate.common.cluster.LocalNodeInfoUtils;
import edu.arizona.cs.stargate.common.cluster.NodeAlreadyAddedException;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import edu.arizona.cs.stargate.service.StargateService;
import edu.arizona.cs.stargate.service.StargateServiceConfiguration;
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
    private LocalClusterManager localClusterManager;
    private RemoteClusterManager remoteClusterManager;
    private DataExportManager dataExportManager;
    private RecipeManager recipeManager;
    private TransportManager transportManager;
    
    public static GateKeeperService getInstance(GateKeeperServiceConfiguration config) {
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
    
    GateKeeperService(GateKeeperServiceConfiguration config) {
        if(config == null) {
            this.config = new GateKeeperServiceConfiguration();
        } else {
            this.config = config;
            this.config.setImmutable();
        }
    }
    
    public synchronized void start() throws Exception {
        this.localClusterManager = LocalClusterManager.getInstance();
        registerLocalClusterInfo(this.config.getClusterInfo());
        
        this.remoteClusterManager = RemoteClusterManager.getInstance();
        
        this.dataExportManager = DataExportManager.getInstance();
        this.recipeManager = RecipeManager.getInstance(this.config.getRecipeManagerConfiguration());
        
        addDataExportListener();
        
        this.dataExportManager.addDataExport(this.config.getDataExport());
        
        this.transportManager = TransportManager.getInstance();
        
        Guice.createInjector(Stage.PRODUCTION, new GatekeeperServletModule());
    }
    
    private void registerLocalClusterInfo(ClusterInfo clusterInfo) {
        // name
        if(this.localClusterManager.getName() == null || this.localClusterManager.getName().isEmpty()) {
            if(clusterInfo != null && clusterInfo.getName() != null) {
                this.localClusterManager.setName(this.config.getClusterInfo().getName());
            } else {
                try {
                    // name
                    StargateServiceConfiguration stargateConfig = StargateService.getInstance().getConfiguration();
                    this.localClusterManager.setName(stargateConfig.getServiceName());
                } catch (ServiceNotStartedException ex) {
                    LOG.error(ex);
                }
            }
        }
        
        // node
        if(clusterInfo != null) {
            try {
                Collection<ClusterNodeInfo> nodes = this.config.getClusterInfo().getAllNode();
                if(nodes != null) {
                    this.localClusterManager.addNode(nodes);
                    Iterator<ClusterNodeInfo> iterator = nodes.iterator();
                    while(iterator.hasNext()) {
                        ClusterNodeInfo node = iterator.next();
                        this.localClusterManager.setLocalNode(node);
                        break;
                    }
                }
            } catch (NodeAlreadyAddedException ex) {
                LOG.error(ex);
            }
        } else {
            try {
                // autogenerate?
                StargateServiceConfiguration stargateConfig = StargateService.getInstance().getConfiguration();
                ClusterNodeInfo node = LocalNodeInfoUtils.getNodeInfo(stargateConfig);
                this.localClusterManager.addNode(node);
                this.localClusterManager.setLocalNode(node);
            } catch (NodeAlreadyAddedException ex) {
                LOG.error(ex);
            } catch (ServiceNotStartedException ex) {
                LOG.error(ex);
            } catch (URISyntaxException ex) {
                LOG.error(ex);
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
            public void addDataExport(DataExportManager manager, DataExportInfo info) {
                // pass to RecipeManager
                recipeManager.prepareRecipe(info);
            }

            @Override
            public void removeDataExport(DataExportManager manager, DataExportInfo info) {
                // pass to RecipeManager
                recipeManager.removeRecipe(info);
            }
        });
    }
    
    public GateKeeperServiceConfiguration getConfiguration() {
        return this.config;
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
    
    public synchronized TransportManager getTransportManager() {
        return this.transportManager;
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperService";
    }
}
