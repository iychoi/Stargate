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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.common.AImmutableConfiguration;
import stargate.commons.common.JsonSerializer;
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.drivers.DriverSetting;
import stargate.drivers.hazelcast.HazelcastCoreDriver;
import stargate.drivers.hazelcast.HazelcastCoreDriverConfiguration;
import stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriver;
import stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriverConfiguration;
import stargate.drivers.hazelcast.schedule.HazelcastScheduleDriver;
import stargate.drivers.hazelcast.schedule.HazelcastScheduleDriverConfiguration;
import stargate.drivers.recipe.sha1fixed.SHA1FixedChunkRecipeGeneratorDriver;
import stargate.drivers.recipe.sha1fixed.SHA1FixedChunkRecipeGeneratorDriverConfiguration;
import stargate.drivers.sourcefs.hdfs.HDFSSourceFileSystemDriver;
import stargate.drivers.sourcefs.hdfs.HDFSSourceFileSystemDriverConfiguration;
import stargate.drivers.temporalstorage.hdfs.HDFSTemporalStorageDriver;
import stargate.drivers.temporalstorage.hdfs.HDFSTemporalStorageDriverConfiguration;
import stargate.drivers.transport.http.HTTPTransportDriver;
import stargate.drivers.transport.http.HTTPTransportDriverConfiguration;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriver;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfiguration;
import stargate.server.temporalstorage.TemporalStorageConfiguration;
import stargate.server.datastore.DataStoreConfiguration;
import stargate.server.recipe.RecipeGeneratorConfiguration;
import stargate.server.schedule.ScheduleConfiguration;
import stargate.server.sourcefs.SourceFileSystemConfiguration;
import stargate.server.transport.TransportConfiguration;
import stargate.server.userinterface.UserInterfaceConfiguration;

/**
 *
 * @author iychoi
 */
public class StargateServiceConfiguration extends AImmutableConfiguration {
    
    private static final Log LOG = LogFactory.getLog(StargateServiceConfiguration.class);
    
    private List<DriverSetting> daemonDriverSetting = new ArrayList<DriverSetting>();
    
    private String serviceName = "StargateDefault";
    private List<Node> node = new ArrayList<Node>();
    private DataStoreConfiguration datastoreConfiguration;
    private SourceFileSystemConfiguration sourceFileSystemConfiguration;
    private RecipeGeneratorConfiguration recipeGeneratorConfiguration;
    private ScheduleConfiguration scheduleConfiguration;
    private TransportConfiguration transportConfiguration;
    private TemporalStorageConfiguration temporalStorageConfiguration;
    private UserInterfaceConfiguration userInterfaceConfiguration;
    private List<RemoteCluster> remoteCluster = new ArrayList<RemoteCluster>();
    private List<DataExportEntry> dataExport = new ArrayList<DataExportEntry>();
    private Map<String, Object> policy = new HashMap<String, Object>();
    
    public static StargateServiceConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (StargateServiceConfiguration) serializer.fromJsonFile(file, StargateServiceConfiguration.class);
    }
    
    public static StargateServiceConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (StargateServiceConfiguration) serializer.fromJson(json, StargateServiceConfiguration.class);
    }
    
    public StargateServiceConfiguration() {
        initDefaults();
    }
    
    protected void initDefaults() {
        initDefaultDaemonDriverConfiguration();
        initDefaultTemporalStorageConfiguration();
        initDefaultDataStoreConfiguration();
        initDefaultSourceFileSystemConfiguration();
        initDefaultRecipeGeneratorConfiguration();
        initDefaultScheduleConfiguration();
        initDefaultTransportConfiguration();
        initDefaultUserInterfaceConfiguration();
    }
    
    @JsonProperty("daemon_driver_setting")
    public Collection<DriverSetting> getDaemonDriverSetting() {
        return Collections.unmodifiableCollection(this.daemonDriverSetting);
    }
    
    @JsonProperty("daemon_driver_setting")
    public void setDaemonDriverSetting(Collection<DriverSetting> driverSetting) {
        if(driverSetting == null) {
            throw new IllegalArgumentException("driverSetting is null");
        }
        
        super.verifyMutable();
    
        this.daemonDriverSetting.clear();
        this.daemonDriverSetting.addAll(driverSetting);
    }
    
    @JsonIgnore
    public void addDaemonDriverSetting(Collection<DriverSetting> driverSetting) {
        if(driverSetting == null) {
            throw new IllegalArgumentException("driverSetting is null");
        }
        
        super.verifyMutable();
    
        this.daemonDriverSetting.addAll(driverSetting);
    }
    
    @JsonIgnore
    public void addDaemonDriverSetting(DriverSetting driverSetting) {
        if(driverSetting == null) {
            throw new IllegalArgumentException("driverSetting is null");
        }
        
        super.verifyMutable();
        
        this.daemonDriverSetting.add(driverSetting);
    }
    
    @JsonProperty("name")
    public void setServiceName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        super.verifyMutable();
        
        this.serviceName = name;
    }
    
    @JsonProperty("name")
    public String getServiceName() {
        return this.serviceName;
    }
    
    @JsonProperty("node")
    public Collection<Node> getNode() {
        return Collections.unmodifiableCollection(this.node);
    }
    
    @JsonProperty("node")
    public void setNode(Collection<Node> node) {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        super.verifyMutable();
        
        this.node.clear();
        this.node.addAll(node);
    }
    
    @JsonIgnore
    public void addNode(Collection<Node> node) {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        super.verifyMutable();
        
        this.node.addAll(node);
    }
    
    @JsonIgnore
    public void addNode(Node node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        super.verifyMutable();
        
        this.node.add(node);
    }
    
    @JsonProperty("datastore")
    public void setDataStoreConfiguration(DataStoreConfiguration dataStoreConfiguration) {
        if(dataStoreConfiguration == null) {
            throw new IllegalArgumentException("dataStoreConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.datastoreConfiguration = dataStoreConfiguration;
    }
    
    @JsonProperty("datastore")
    public DataStoreConfiguration getDataStoreConfiguration() {
        return this.datastoreConfiguration;
    }
    
    @JsonProperty("source_filesystem")
    public void setSourceFileSystemConfiguration(SourceFileSystemConfiguration sourceFileSystemConfiguration) {
        if(sourceFileSystemConfiguration == null) {
            throw new IllegalArgumentException("sourceFileSystemConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.sourceFileSystemConfiguration = sourceFileSystemConfiguration;
    }
    
    @JsonProperty("source_filesystem")
    public SourceFileSystemConfiguration getSourceFileSystemConfiguration() {
        return this.sourceFileSystemConfiguration;
    }
    
    @JsonProperty("recipe_generator")
    public void setRecipeGeneratorConfiguration(RecipeGeneratorConfiguration recipeGeneratorConfiguration) {
        if(recipeGeneratorConfiguration == null) {
            throw new IllegalArgumentException("recipeGeneratorConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.recipeGeneratorConfiguration = recipeGeneratorConfiguration;
    }
    
    @JsonProperty("recipe_generator")
    public RecipeGeneratorConfiguration getRecipeGeneratorConfiguration() {
        return this.recipeGeneratorConfiguration;
    }
    
    @JsonProperty("schedule")
    public void setScheduleConfiguration(ScheduleConfiguration scheduleConfiguration) {
        if(scheduleConfiguration == null) {
            throw new IllegalArgumentException("scheduleConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.scheduleConfiguration = scheduleConfiguration;
    }
    
    @JsonProperty("schedule")
    public ScheduleConfiguration getScheduleConfiguration() {
        return this.scheduleConfiguration;
    }
    
    @JsonProperty("transport")
    public void setTransportConfiguration(TransportConfiguration transportConfiguration) {
        if(transportConfiguration == null) {
            throw new IllegalArgumentException("transportConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.transportConfiguration = transportConfiguration;
    }
    
    @JsonProperty("transport")
    public TransportConfiguration getTransportConfiguration() {
        return this.transportConfiguration;
    }
    
    @JsonProperty("temporal_storage")
    public void setTemporalStorageConfiguration(TemporalStorageConfiguration temporalStorageConfiguration) {
        if(temporalStorageConfiguration == null) {
            throw new IllegalArgumentException("temporalStorageConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.temporalStorageConfiguration = temporalStorageConfiguration;
    }
    
    @JsonProperty("temporal_storage")
    public TemporalStorageConfiguration getTemporalStorageConfiguration() {
        return this.temporalStorageConfiguration;
    }
    
    @JsonProperty("user_interface")
    public void setUserInterfaceConfiguration(UserInterfaceConfiguration userInterfaceConfiguration) {
        if(userInterfaceConfiguration == null) {
            throw new IllegalArgumentException("userInterfaceConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.userInterfaceConfiguration = userInterfaceConfiguration;
    }
    
    @JsonProperty("user_interface")
    public UserInterfaceConfiguration getUserInterfaceConfiguration() {
        return this.userInterfaceConfiguration;
    }
    
    @JsonProperty("remote_cluster")
    public Collection<RemoteCluster> getRemoteCluster() {
        return Collections.unmodifiableCollection(this.remoteCluster);
    }
    
    @JsonProperty("remote_cluster")
    public void setRemoteCluster(Collection<RemoteCluster> cluster) {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        super.verifyMutable();
        
        this.remoteCluster.clear();
        this.remoteCluster.addAll(cluster);
    }
    
    @JsonIgnore
    public void addRemoteCluster(Collection<RemoteCluster> cluster) {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        super.verifyMutable();
        
        this.remoteCluster.addAll(cluster);
    }
    
    @JsonIgnore
    public void addRemoteCluster(RemoteCluster cluster) {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is null or empty");
        }
        
        super.verifyMutable();
        
        this.remoteCluster.add(cluster);
    }
    
    @JsonProperty("data_export")
    public Collection<DataExportEntry> getDataExport() {
        return Collections.unmodifiableCollection(this.dataExport);
    }
    
    @JsonProperty("data_export")
    public void setDataExport(Collection<DataExportEntry> entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        super.verifyMutable();
        
        this.dataExport.clear();
        this.dataExport.addAll(entry);
    }
    
    @JsonIgnore
    public void addDataExport(Collection<DataExportEntry> entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        super.verifyMutable();
        
        this.dataExport.addAll(entry);
    }
    
    @JsonIgnore
    public void addDataExport(DataExportEntry entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        super.verifyMutable();
        
        this.dataExport.add(entry);
    }
    
    @JsonProperty("policy")
    public Map<String, Object> getPolicy() {
        return Collections.unmodifiableMap(this.policy);
    }
    
    @JsonProperty("policy")
    public void setPolicy(Map<String, Object> policy) {
        this.policy.putAll(policy);
    }
    
    @Override
    @JsonIgnore
    public void setImmutable() {
        super.setImmutable();
        
        for(DriverSetting driverSetting : this.daemonDriverSetting) {
            driverSetting.setImmutable();
        }
        
        this.datastoreConfiguration.setImmutable();
        this.sourceFileSystemConfiguration.setImmutable();
        this.recipeGeneratorConfiguration.setImmutable();
        this.scheduleConfiguration.setImmutable();
        this.transportConfiguration.setImmutable();
        this.temporalStorageConfiguration.setImmutable();
        this.userInterfaceConfiguration.setImmutable();
    }
    
    private void initDefaultDaemonDriverConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HazelcastCoreDriver.class);
        
        HazelcastCoreDriverConfiguration driverConfiguration = new HazelcastCoreDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        this.daemonDriverSetting.add(driverSetting);
    }
    
    private void initDefaultTemporalStorageConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HDFSTemporalStorageDriver.class);
        
        HDFSTemporalStorageDriverConfiguration driverConfiguration = new HDFSTemporalStorageDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        TemporalStorageConfiguration temporalStorageConfiguration = new TemporalStorageConfiguration();
        temporalStorageConfiguration.setDriverSetting(driverSetting);
        
        this.temporalStorageConfiguration = temporalStorageConfiguration;
    }
    
    private void initDefaultDataStoreConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HazelcastDataStoreDriver.class);
        
        HazelcastDataStoreDriverConfiguration driverConfiguration = new HazelcastDataStoreDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        DataStoreConfiguration datastoreConfiguration = new DataStoreConfiguration();
        datastoreConfiguration.setDriverSetting(driverSetting);
        
        this.datastoreConfiguration = datastoreConfiguration;
    }

    private void initDefaultSourceFileSystemConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HDFSSourceFileSystemDriver.class);
        
        HDFSSourceFileSystemDriverConfiguration driverConfiguration = new HDFSSourceFileSystemDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        SourceFileSystemConfiguration sourceFileSystemConfiguration = new SourceFileSystemConfiguration();
        sourceFileSystemConfiguration.setDriverSetting(driverSetting);
        
        this.sourceFileSystemConfiguration = sourceFileSystemConfiguration;
    }

    private void initDefaultRecipeGeneratorConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(SHA1FixedChunkRecipeGeneratorDriver.class);
        
        SHA1FixedChunkRecipeGeneratorDriverConfiguration driverConfiguration = new SHA1FixedChunkRecipeGeneratorDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        RecipeGeneratorConfiguration recipeGeneratorConfiguration = new RecipeGeneratorConfiguration();
        recipeGeneratorConfiguration.setDriverSetting(driverSetting);
        
        this.recipeGeneratorConfiguration = recipeGeneratorConfiguration;
    }

    private void initDefaultScheduleConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HazelcastScheduleDriver.class);
        
        HazelcastScheduleDriverConfiguration driverConfiguration = new HazelcastScheduleDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        ScheduleConfiguration scheduleConfiguration = new ScheduleConfiguration();
        scheduleConfiguration.setDriverSetting(driverSetting);
        
        this.scheduleConfiguration = scheduleConfiguration;
    }

    private void initDefaultTransportConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HTTPTransportDriver.class);
        
        HTTPTransportDriverConfiguration driverConfiguration = new HTTPTransportDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        TransportConfiguration transportConfiguration = new TransportConfiguration();
        transportConfiguration.setDriverSetting(driverSetting);
        
        this.transportConfiguration = transportConfiguration;
    }

    private void initDefaultUserInterfaceConfiguration() {
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HTTPUserInterfaceDriver.class);
        
        HTTPUserInterfaceDriverConfiguration driverConfiguration = new HTTPUserInterfaceDriverConfiguration();
        driverSetting.setDriverConfiguration(driverConfiguration);
        
        UserInterfaceConfiguration userInterfaceConfiguration = new UserInterfaceConfiguration();
        userInterfaceConfiguration.addDriverSetting(driverSetting);
        
        this.userInterfaceConfiguration = userInterfaceConfiguration;
    }
}
