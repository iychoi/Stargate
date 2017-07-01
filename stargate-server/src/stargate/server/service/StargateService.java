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

package stargate.server.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.NodeAlreadyAddedException;
import stargate.commons.cluster.NodeStatus;
import stargate.commons.temporalstorage.APersistentTemporalStorageDriver;
import stargate.commons.datastore.ADataStoreDriver;
import stargate.commons.drivers.ADriver;
import stargate.commons.drivers.DriverFactory;
import stargate.commons.drivers.DriverSetting;
import stargate.commons.recipe.ARecipeGeneratorDriver;
import stargate.commons.schedule.AScheduleDriver;
import stargate.commons.service.AService;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.sourcefs.ASourceFileSystemDriver;
import stargate.commons.transport.ATransportDriver;
import stargate.commons.userinterface.AUserInterfaceDriver;
import stargate.commons.utils.NodeUtils;
import stargate.server.blockcache.BlockCacheManager;
import stargate.server.cluster.ClusterManager;
import stargate.server.cluster.LocalClusterManager;
import stargate.server.temporalstorage.TemporalStorageManager;
import stargate.server.dataexport.DataExportManager;
import stargate.server.datastore.DataStoreManager;
import stargate.server.policy.PolicyManager;
import stargate.server.recipe.RecipeGeneratorManager;
import stargate.server.recipe.RecipeManager;
import stargate.server.schedule.ScheduleManager;
import stargate.server.sourcefs.SourceFileSystemManager;
import stargate.server.tasks.RecipeSyncTask;
import stargate.server.tasks.RemoteClusterSyncTask;
import stargate.server.transport.TransportManager;
import stargate.server.userinterface.UserInterfaceManager;
import stargate.server.volume.VolumeManager;

/**
 *
 * @author iychoi
 */
public class StargateService extends AService {
    
    private static final Log LOG = LogFactory.getLog(StargateService.class);
    
    private static StargateService instance;
    
    private StargateServiceConfiguration config;
    private boolean serviceStarted;
    
    private List<ADriver> daemonDriver = new ArrayList<ADriver>();
    
    private TemporalStorageManager temporalStorageManager;
    private BlockCacheManager blockCacheManager;
    private DataStoreManager dataStoreManager;
    private SourceFileSystemManager sourceFileSystemManager;
    private RecipeGeneratorManager recipeGeneratorManager;
    private ScheduleManager scheduleManager;
    private PolicyManager policyManager;
    private ClusterManager clusterManager;
    private DataExportManager dataExportManager;
    private RecipeManager recipeManager;
    private TransportManager transportManager;
    private VolumeManager volumeManager;
    private UserInterfaceManager userInterfaceManager;
    
    public static StargateService getInstance(StargateServiceConfiguration config) throws Exception {
        synchronized (StargateService.class) {
            if(instance == null) {
                instance = new StargateService(config);
            }
            return instance;
        }
    }
    
    public static StargateService getInstance() throws ServiceNotStartedException {
        synchronized (StargateService.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("Stargate service is not started");
            }
            return instance;
        }
    }
    
    public StargateService(StargateServiceConfiguration config) throws Exception {
        if(config == null) {
            throw new Exception("config is null. Failed to start StargateService.");
        }
        
        this.config = config;
        this.config.setImmutable();
        this.serviceStarted = false;
    }
    
    public synchronized void start() throws Exception {
        verifyConfiguration();
        
        // init daemon drivers
        Collection<DriverSetting> daemonDriverSettings = this.config.getDaemonDriverSetting();
        for(DriverSetting driverSetting : daemonDriverSettings) {
            ADriver driverInstance = DriverFactory.createDriver(driverSetting);
            driverInstance.setService(this);
            driverInstance.startDriver();
            
            this.daemonDriver.add(driverInstance);
        }
        
        // init temporal storage manager
        // init temporal storage driver
        APersistentTemporalStorageDriver temporalStorageDriver = (APersistentTemporalStorageDriver)DriverFactory.createDriver(this.config.getTemporalStorageConfiguration().getDriverSetting());
        temporalStorageDriver.setService(this);
        this.temporalStorageManager = TemporalStorageManager.getInstance(temporalStorageDriver);
        this.temporalStorageManager.start();
        
        // init data store manager
        // init data store driver
        ADataStoreDriver dataStoreDriver = (ADataStoreDriver)DriverFactory.createDriver(this.config.getDataStoreConfiguration().getDriverSetting());
        dataStoreDriver.setService(this);
        this.dataStoreManager = DataStoreManager.getInstance(dataStoreDriver);
        this.dataStoreManager.start();

        // init block cache manager
        this.blockCacheManager = BlockCacheManager.getInstance(this.temporalStorageManager, this.dataStoreManager);
        
        // init source file system manager
        // init source file system driver
        ASourceFileSystemDriver sourceFileSystemDriver = (ASourceFileSystemDriver)DriverFactory.createDriver(this.config.getSourceFileSystemConfiguration().getDriverSetting());
        sourceFileSystemDriver.setService(this);
        this.sourceFileSystemManager = SourceFileSystemManager.getInstance(sourceFileSystemDriver);
        this.sourceFileSystemManager.start();

        // init recipe generator manager
        // init recipe generator driver
        ARecipeGeneratorDriver recipeGeneratorDriver = (ARecipeGeneratorDriver)DriverFactory.createDriver(this.config.getRecipeGeneratorConfiguration().getDriverSetting());
        recipeGeneratorDriver.setService(this);
        this.recipeGeneratorManager = RecipeGeneratorManager.getInstance(recipeGeneratorDriver);
        this.recipeGeneratorManager.start();

        // init schedule manager
        // init schedule driver
        AScheduleDriver scheduleDriver = (AScheduleDriver)DriverFactory.createDriver(this.config.getScheduleConfiguration().getDriverSetting());
        scheduleDriver.setService(this);
        this.scheduleManager = ScheduleManager.getInstance(scheduleDriver);
        this.scheduleManager.start();
        
        // init policy manager
        this.policyManager = PolicyManager.getInstance(this.dataStoreManager);
        
        // init cluster manager
        this.clusterManager = ClusterManager.getInstance(this.dataStoreManager);
        
        // init data export manager
        this.dataExportManager = DataExportManager.getInstance(this.dataStoreManager);
        
        // init recipe manager
        this.recipeManager = RecipeManager.getInstance(this.dataStoreManager, this.recipeGeneratorManager, this.sourceFileSystemManager, this.clusterManager, this.dataExportManager);
        
        // init transport manager
        // init transport driver
        ATransportDriver transportDriver = (ATransportDriver)DriverFactory.createDriver(this.config.getTransportConfiguration().getDriverSetting());
        transportDriver.setService(this);
        this.transportManager = TransportManager.getInstance(transportDriver, this.dataStoreManager, this.recipeManager, this.recipeGeneratorManager, this.sourceFileSystemManager, this.dataExportManager, this.blockCacheManager);
        this.transportManager.start();
        
        // setup cluster
        setupCluster(this.clusterManager);
        
        // init user-interface manager
        // init user-interface driver
        List<AUserInterfaceDriver> userInterfaceDrivers = new ArrayList<AUserInterfaceDriver>();
        Collection<DriverSetting> uiDriverSettings = this.config.getUserInterfaceConfiguration().getDriverSetting();
        for(DriverSetting driverSetting : uiDriverSettings) {
            AUserInterfaceDriver userInterfaceDriver = (AUserInterfaceDriver)DriverFactory.createDriver(driverSetting);
            userInterfaceDriver.setService(this);
            userInterfaceDrivers.add(userInterfaceDriver);
        }
        this.userInterfaceManager = UserInterfaceManager.getInstance(userInterfaceDrivers);
        this.userInterfaceManager.start();
        
        // init volume manager
        this.volumeManager = VolumeManager.getInstance(this.policyManager, this.dataStoreManager, this.sourceFileSystemManager, this.clusterManager, this.dataExportManager, this.recipeManager, this.transportManager);
        
        this.policyManager.addPolicy(this.config.getPolicy());
        this.clusterManager.addRemoteCluster(this.config.getRemoteCluster());
        this.dataExportManager.addDataExport(this.config.getDataExport());

        // register schedules
        this.scheduleManager.setScheduledTask(new RemoteClusterSyncTask(this.policyManager, this.clusterManager, this.transportManager));
        this.scheduleManager.setScheduledTask(new RecipeSyncTask(this.policyManager, this.sourceFileSystemManager, this.clusterManager, this.dataExportManager, this.recipeManager, this.recipeGeneratorManager));
        
        this.serviceStarted = true;
        LOG.info("Stargate service started");
    }
    
    public synchronized void stop() throws Exception {
        this.userInterfaceManager.stop();
        this.userInterfaceManager = null;
        
        this.transportManager.stop();
        this.transportManager = null;
        
        this.scheduleManager.stop();
        this.scheduleManager = null;
        
        this.recipeGeneratorManager.stop();
        this.recipeGeneratorManager = null;
        
        this.sourceFileSystemManager.stop();
        this.sourceFileSystemManager = null;
        
        this.dataStoreManager.stop();
        this.dataStoreManager = null;
        
        this.temporalStorageManager.stop();
        this.temporalStorageManager = null;
        
        this.policyManager = null;
        this.clusterManager = null;
        this.dataExportManager = null;
        this.recipeManager = null;
        this.volumeManager = null;
        this.blockCacheManager = null;
        
        for(ADriver driver : this.daemonDriver) {
            driver.stopDriver();
        }
        
        this.daemonDriver.clear();
        
        this.serviceStarted = false;
    }
    
    private void verifyConfiguration() throws Exception {
        if(this.config.getServiceName() == null || this.config.getServiceName().isEmpty()) {
            throw new Exception("service name is not given");
        }
    }
    
    private void setupCluster(ClusterManager clusterManager) throws IOException {
        LocalClusterManager localClusterManager = clusterManager.getLocalClusterManager();
        
        // name
        localClusterManager.setName(this.config.getServiceName());
        
        // node
        Collection<Node> node = this.config.getNode();
        if(!node.isEmpty()) {
            try {
                boolean localNodeFound = false;
                for(Node n : node) {
                    if(NodeUtils.isLocalNode(n)) {
                        localClusterManager.addNode(n);
                        
                        NodeStatus status = new NodeStatus(n);
                        localClusterManager.addNodeStatus(status);

                        localClusterManager.setLocalNode(n);
                        localNodeFound = true;
                        break;
                    }
                }
                
                if(!localNodeFound) {
                    throw new IOException("local node is not found in configuration");
                }
            } catch (NodeAlreadyAddedException ex) {
                throw new IOException(ex);
            }
        } else {
            try {
                // auto-generate
                URI serviceUri = this.transportManager.getDriver().getServiceURI();
                
                Node n = NodeUtils.createNode(this.config.getTransportConfiguration().getDriverSetting().getDriverClass(), serviceUri);
                localClusterManager.addNode(n);
                
                NodeStatus status = new NodeStatus(n);
                localClusterManager.addNodeStatus(status);
                
                localClusterManager.setLocalNode(n);
            } catch (NodeAlreadyAddedException ex) {
                throw new IOException(ex);
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized boolean isStarted() {
        return this.serviceStarted;
    }
    
    public synchronized PolicyManager getPolicyManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.policyManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized PolicyManager getPolicyManagerNoException() {
        return this.policyManager;
    }
    
    public synchronized TemporalStorageManager getTemporalStorageManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.temporalStorageManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized TemporalStorageManager getTemporalStorageManagerNoException() {
        return this.temporalStorageManager;
    }
    
    public synchronized DataStoreManager getDataStoreManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.dataStoreManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized DataStoreManager getDataStoreManagerNoException() {
        return this.dataStoreManager;
    }
    
    public synchronized BlockCacheManager getBlockCacheManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.blockCacheManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized BlockCacheManager getBlockCacheManagerNoException() {
        return this.blockCacheManager;
    }
    
    public synchronized SourceFileSystemManager getSourceFileSystemManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.sourceFileSystemManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized SourceFileSystemManager getSourceFileSystemManagerNoException() {
        return this.sourceFileSystemManager;
    }
    
    public synchronized RecipeGeneratorManager getRecipeGeneratorManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.recipeGeneratorManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized RecipeGeneratorManager getRecipeGeneratorManagerNoException() {
        return this.recipeGeneratorManager;
    }
    
    public synchronized ScheduleManager getScheduleManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.scheduleManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized ScheduleManager getScheduleManagerNoException() {
        return this.scheduleManager;
    }
    
    public synchronized ClusterManager getClusterManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.clusterManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized ClusterManager getClusterManagerNoException() {
        return this.clusterManager;
    }
    
    public synchronized DataExportManager getDataExportManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.dataExportManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized DataExportManager getDataExportManagerNoException() {
        return this.dataExportManager;
    }
    
    public synchronized RecipeManager getRecipeManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.recipeManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized RecipeManager getRecipeManagerNoException() {
        return this.recipeManager;
    }
    
    public synchronized TransportManager getTransportManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.transportManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized TransportManager getTransportManagerNoException() {
        return this.transportManager;
    }
    
    public synchronized VolumeManager getVolumeManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.volumeManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized VolumeManager getVolumeManagerNoException() {
        return this.volumeManager;
    }
    
    public synchronized UserInterfaceManager getUserInterfaceManager() throws ServiceNotStartedException {
        if(this.serviceStarted) {
            return this.userInterfaceManager;
        } else {
            throw new ServiceNotStartedException("Stargate service is not started");
        }
    }
    
    public synchronized UserInterfaceManager getUserInterfaceManagerNoException() {
        return this.userInterfaceManager;
    }
    
    public synchronized StargateServiceConfiguration getConfiguration() {
        return this.config;
    }
    
    @Override
    public synchronized String toString() {
        return "StargateService";
    }
}
