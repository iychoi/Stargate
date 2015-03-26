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

import edu.arizona.cs.stargate.cache.service.DistributedCacheServiceConfiguration;
import edu.arizona.cs.stargate.common.ImmutableConfiguration;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClientConfiguration;
import edu.arizona.cs.stargate.gatekeeper.service.GateKeeperServiceConfiguration;
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
public class StargateServiceConfiguration extends ImmutableConfiguration {
    
    public static final String DEFAULT_CONFIG_FILEPATH = "service.json";
    
    public static final int DEFAULT_SERVICE_PORT = 11010;
    
    private String serviceName;
    private int servicePort = DEFAULT_SERVICE_PORT;
    private DistributedCacheServiceConfiguration distributedCacheServiceConfig;
    private GateKeeperServiceConfiguration gatekeeperServiceConfig;
    private ArrayList<GateKeeperClientConfiguration> gatekeeperClientConfigs = new ArrayList<GateKeeperClientConfiguration>();
    
    public static StargateServiceConfiguration createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (StargateServiceConfiguration) serializer.fromJsonFile(file, StargateServiceConfiguration.class);
    }
    
    public static StargateServiceConfiguration createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (StargateServiceConfiguration) serializer.fromJson(json, StargateServiceConfiguration.class);
    }
    
    public StargateServiceConfiguration() {
    }
    
    @JsonProperty("name")
    public void setServiceName(String name) {
        super.verifyMutable();
        
        this.serviceName = name;
    }
    
    @JsonProperty("name")
    public String getServiceName() {
        return this.serviceName;
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
    
    public void addGatekeeperClientConfiguration(GateKeeperClientConfiguration conf) {
        super.verifyMutable();
        
        this.gatekeeperClientConfigs.add(conf);
    }
    
    @JsonProperty("gatekeeperClient")
    public Collection<GateKeeperClientConfiguration> getGatekeeperClientConfigurations() {
        return Collections.unmodifiableCollection(this.gatekeeperClientConfigs);
    }
    
    @JsonProperty("gatekeeperClient")
    public void setGatekeeperClientConfigurations(Collection<GateKeeperClientConfiguration> configurations) {
        super.verifyMutable();
        
        for(GateKeeperClientConfiguration conf : configurations) {
            addGatekeeperClientConfiguration(conf);
        }
    }
    
    @JsonProperty("gatekeeperService")
    public void setGatekeeperServiceConfiguration(GateKeeperServiceConfiguration conf) {
        super.verifyMutable();
        
        this.gatekeeperServiceConfig = conf;
    }
    
    @JsonProperty("gatekeeperService")
    public GateKeeperServiceConfiguration getGatekeeperServiceConfiguration() {
        return this.gatekeeperServiceConfig;
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
    
    @Override
    @JsonIgnore
    public void setImmutable() {
        super.setImmutable();
        
        this.distributedCacheServiceConfig.setImmutable();
        this.gatekeeperServiceConfig.setImmutable();
        for(GateKeeperClientConfiguration conf : this.gatekeeperClientConfigs) {
            conf.setImmutable();
        }
    }
}
