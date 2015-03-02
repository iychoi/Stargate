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

import edu.arizona.cs.stargate.common.ImmutableConfiguration;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class GateKeeperServiceConfiguration extends ImmutableConfiguration {
    
    private ClusterInfo clusterInfo;
    private ArrayList<DataExportInfo> dataExports = new ArrayList<DataExportInfo>();
    
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
    }
    
    @JsonProperty("cluster")
    public ClusterInfo getClusterInfo() {
        return this.clusterInfo;
    }
    
    @JsonProperty("cluster")
    public void setClusterInfo(ClusterInfo clusterInfo) {
        super.verifyMutable();
        
        this.clusterInfo = clusterInfo;
    }
    
    @JsonProperty("export")
    public Collection<DataExportInfo> getDataExport() {
        return Collections.unmodifiableCollection(this.dataExports);
    }
    
    @JsonProperty("export")
    public void addDataExport(Collection<DataExportInfo> info) {
        super.verifyMutable();
        
        this.dataExports.addAll(info);
    }
    
    public void addDataExport(DataExportInfo info) {
        super.verifyMutable();
        
        this.dataExports.add(info);
    }
    
    @JsonProperty("recipeManager")
    public RecipeManagerConfiguration getRecipeManagerConfiguration() {
        return this.recipeManagerConfiguration;
    }
    
    @JsonProperty("recipeManager")
    public void setRecipeManagerConfiguration(RecipeManagerConfiguration config) {
        super.verifyMutable();
        
        this.recipeManagerConfiguration = config;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
        
        this.recipeManagerConfiguration.setImmutable();
    }
}
