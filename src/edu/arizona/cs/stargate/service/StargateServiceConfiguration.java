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

package edu.arizona.cs.stargate.service;

import edu.arizona.cs.stargate.cluster.Node;
import edu.arizona.cs.stargate.cluster.RemoteCluster;
import edu.arizona.cs.stargate.common.AImmutableConfiguration;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.datastore.DataStoreConfiguration;
import edu.arizona.cs.stargate.drivers.DriverSetting;
import edu.arizona.cs.stargate.policy.Policy;
import edu.arizona.cs.stargate.recipe.RecipeGeneratorConfiguration;
import edu.arizona.cs.stargate.schedule.ScheduleConfiguration;
import edu.arizona.cs.stargate.sourcefs.SourceFileSystemConfiguration;
import edu.arizona.cs.stargate.transport.TransportConfiguration;
import edu.arizona.cs.stargate.userinterface.UserInterfaceConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class StargateServiceConfiguration extends AImmutableConfiguration {
    
    private static final Log LOG = LogFactory.getLog(StargateServiceConfiguration.class);
    
    private static final String HADOOP_CONFIG_KEY = StargateServiceConfiguration.class.getCanonicalName();
    
    private List<DriverSetting> daemonDriverSetting = new ArrayList<DriverSetting>();
    
    private String serviceName;
    private List<Node> node = new ArrayList<Node>();
    private DataStoreConfiguration datastoreConfiguration;
    private SourceFileSystemConfiguration sourceFileSystemConfiguration;
    private RecipeGeneratorConfiguration recipeGeneratorConfiguration;
    private ScheduleConfiguration scheduleConfiguration;
    private TransportConfiguration transportConfiguration;
    private UserInterfaceConfiguration userInterfaceConfiguration;
    private List<RemoteCluster> remoteCluster = new ArrayList<RemoteCluster>();
    private List<DataExportEntry> dataExport = new ArrayList<DataExportEntry>();
    private Policy policy;
    
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
    
    public static StargateServiceConfiguration createInstance(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (StargateServiceConfiguration) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, StargateServiceConfiguration.class);
    }
    
    public static StargateServiceConfiguration createInstance(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (StargateServiceConfiguration) serializer.fromJsonFile(fs, file, StargateServiceConfiguration.class);
    }
    
    public StargateServiceConfiguration() {
    }
    
    @JsonProperty("daemon_driver_setting")
    public Collection<DriverSetting> getDaemonDriverSetting() {
        return Collections.unmodifiableCollection(this.daemonDriverSetting);
    }
    
    @JsonProperty("daemon_driver_setting")
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
    public Policy getPolicy() {
        return this.policy;
    }
    
    @JsonProperty("policy")
    public void setPolicy(Policy policy) {
        this.policy = policy;
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
        this.userInterfaceConfiguration.setImmutable();
    }
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(Configuration conf) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(FileSystem fs, Path file) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
