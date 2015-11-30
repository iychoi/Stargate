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
package stargate.drivers.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReplicatedMap;
import stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriver;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.drivers.ADriver;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.drivers.DriverNotInstantiatedException;
import stargate.server.recipe.RecipeManager;
import stargate.server.service.StargateService;
import stargate.server.service.StargateServiceConfiguration;
import stargate.server.volume.VolumeManager;

/**
 *
 * @author iychoi
 */
public class HazelcastCoreDriver extends ADriver {
    
    private static final Log LOG = LogFactory.getLog(HazelcastDataStoreDriver.class);
    
    private static final String HAZELCAST_GROUP_NAME_PREFIX = "Stargate_";
    
    private static HazelcastCoreDriver instance;
    
    private HazelcastCoreDriverConfiguration config;
    private HazelcastInstance hazelcastInstance;
    
    public static HazelcastCoreDriver getInstance() throws DriverNotInstantiatedException {
        synchronized (HazelcastCoreDriver.class) {
            if(instance == null) {
                throw new DriverNotInstantiatedException("HazelcastDriverGroup is not instantiated");
            }
            return instance;
        }
    }
    
    public HazelcastCoreDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HazelcastCoreDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HazelcastDriverGroupConfiguration");
        }
        
        if(instance != null) {
            throw new InstantiationError("cannot instantiate HazelcastDriverGroup class. Instance already exists");
        }
        
        this.config = (HazelcastCoreDriverConfiguration) config;
        
        instance = this;
    }
    
    public HazelcastCoreDriver(HazelcastCoreDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(instance != null) {
            throw new InstantiationError("cannot instantiate HazelcastDriverGroup class. Instance already exists");
        }
        
        this.config = config;
        
        instance = this;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        Config hazelcastConfig = makeHazelcastTCPConfig();
        this.hazelcastInstance = Hazelcast.newHazelcastInstance(hazelcastConfig);
    }

    @Override
    public synchronized void stopDriver() throws IOException {
        this.hazelcastInstance.shutdown();
    }
    
    public StargateService getStargateService() throws Exception {
        if(this.service instanceof StargateService) {
            return (StargateService)this.service;
        } else {
            throw new Exception("service object is not instance of StargateService");
        }
    }
    
    private Config makeDefaultHazelcastConfig() throws Exception {
        Config config = new Config();
        if(this.config.getServiceName() == null || this.config.getServiceName().isEmpty()) {
            StargateServiceConfiguration serviceConf = getStargateService().getConfiguration();
            config.getGroupConfig().setName(HAZELCAST_GROUP_NAME_PREFIX + serviceConf.getServiceName());
        } else {
            config.getGroupConfig().setName(HAZELCAST_GROUP_NAME_PREFIX + this.config.getServiceName());
        }
        
        NetworkConfig network = config.getNetworkConfig();
        network.setPort(this.config.getPort());
        network.setPortAutoIncrement(true);
        network.setPortCount(100);
        
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("dht");
        mapConfig.setBackupCount(2);
        mapConfig.getMaxSizeConfig().setSize(0);
        mapConfig.setTimeToLiveSeconds(0);
        
        config.addMapConfig(mapConfig);
        
        MapConfig mapHashMapConfig = new MapConfig();
        mapHashMapConfig.setName(RecipeManager.RECIPEMANAGER_HASH_MAP_ID);
        mapHashMapConfig.setBackupCount(2);
        mapHashMapConfig.getMaxSizeConfig().setSize(0);
        mapHashMapConfig.setTimeToLiveSeconds(0);
        mapHashMapConfig.setReadBackupData(true);

        config.addMapConfig(mapHashMapConfig);
        
        MapConfig hierarchyMapConfig = new MapConfig();
        hierarchyMapConfig.setName(VolumeManager.VOLUMEMANAGER_DIRECTORY_HIERARCHY_MAP_ID);
        hierarchyMapConfig.setBackupCount(2);
        hierarchyMapConfig.getMaxSizeConfig().setSize(0);
        hierarchyMapConfig.setTimeToLiveSeconds(0);
        hierarchyMapConfig.setReadBackupData(true);

        config.addMapConfig(hierarchyMapConfig);
        
        return config;
    }
    
    private Config makeHazelcastTCPConfig() throws IOException {
        try {
            Config config = makeDefaultHazelcastConfig();
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            if(!this.config.isLeaderHost()) {
                for(String knownHostAddr: this.config.getKnownHostAddr()) {
                    config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(knownHostAddr + ":" + this.config.getPort());
                }
            }
            
            config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(this.config.getMyHostAddr()+ ":" + this.config.getPort());
            
            return config;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public String getDriverName() {
        return "HazelcastDriverGroup";
    }

    public synchronized IMap<String, Object> getMap(String name) {
        return this.hazelcastInstance.getMap(name);
    }

    public synchronized ReplicatedMap<String, Object> getReplicatedMap(String name) {
        return this.hazelcastInstance.getReplicatedMap(name);
    }

    public synchronized boolean isLeader() {
        Member member = this.hazelcastInstance.getCluster().getMembers().iterator().next();
        return member.localMember();
    }
}
