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

import edu.arizona.cs.stargate.common.ImmutableConfiguration;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.gatekeeper.distributedcache.DistributedCacheServiceConfiguration;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeManagerConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class GateKeeperServiceConfiguration extends ImmutableConfiguration {
    
    public static final int DEFAULT_SERVICE_PORT = 11010;
    
    private int servicePort = DEFAULT_SERVICE_PORT;
    
    private DistributedCacheServiceConfiguration distributedCacheServiceConfig;
    
    private Cluster localCluster;
    private ArrayList<Cluster> remoteClusters = new ArrayList<Cluster>();
    private ArrayList<DataExport> dataExports = new ArrayList<DataExport>();
    
    private RecipeManagerConfiguration recipeManagerConfiguration;
    
    public static GateKeeperServiceConfiguration createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GateKeeperServiceConfiguration) serializer.fromJsonFile(file, GateKeeperServiceConfiguration.class);
    }
    
    public static GateKeeperServiceConfiguration createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GateKeeperServiceConfiguration) serializer.fromJson(json, GateKeeperServiceConfiguration.class);
    }
    
    public GateKeeperServiceConfiguration() {
        this.recipeManagerConfiguration = new RecipeManagerConfiguration();
    }
    
    @JsonProperty("port")
    public void setServicePort(int port) {
        super.verifyMutable();
        
        this.servicePort = port;
    }
    
    @JsonProperty("port")
    public int getServicePort() {
        return this.servicePort;
    }
    
    @JsonProperty("dhtService")
    public void setDistributedCacheServiceConfiguration(DistributedCacheServiceConfiguration conf) {
        super.verifyMutable();
        
        this.distributedCacheServiceConfig = conf;
    }
    
    @JsonProperty("dhtService")
    public DistributedCacheServiceConfiguration getDistributedCacheServiceConfiguration() {
        return this.distributedCacheServiceConfig;
    }
    
    @JsonProperty("localCluster")
    public Cluster getLocalCluster() {
        return this.localCluster;
    }
    
    @JsonProperty("localCluster")
    public void setLocalCluster(Cluster cluster) {
        super.verifyMutable();
        
        this.localCluster = cluster;
    }
    
    @JsonProperty("remoteClusters")
    public Collection<Cluster> getRemoteClusters() {
        return this.remoteClusters;
    }
    
    @JsonProperty("remoteClusters")
    public void addRemoteClusters(Collection<Cluster> clusters) {
        super.verifyMutable();
        
        this.remoteClusters.addAll(clusters);
    }
    
    @JsonIgnore
    public void addRemoteCluster(Cluster cluster) {
        super.verifyMutable();
        
        this.remoteClusters.add(cluster);
    }
    
    @JsonProperty("export")
    public Collection<DataExport> getDataExports() {
        return Collections.unmodifiableCollection(this.dataExports);
    }
    
    @JsonProperty("export")
    public void addDataExports(Collection<DataExport> exports) {
        super.verifyMutable();
        
        this.dataExports.addAll(exports);
    }
    
    @JsonIgnore
    public void addDataExport(DataExport export) {
        super.verifyMutable();
        
        this.dataExports.add(export);
    }
    
    @JsonProperty("recipe")
    public RecipeManagerConfiguration getRecipeManagerConfiguration() {
        return this.recipeManagerConfiguration;
    }
    
    @JsonProperty("recipe")
    public void setRecipeManagerConfiguration(RecipeManagerConfiguration config) {
        super.verifyMutable();
        
        this.recipeManagerConfiguration = config;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();

        this.distributedCacheServiceConfig.setImmutable();
        this.recipeManagerConfiguration.setImmutable();
    }
}
