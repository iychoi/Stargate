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

import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClientConfiguration;
import edu.arizona.cs.stargate.gatekeeper.service.GateKeeperServiceConfiguration;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author iychoi
 */
public class StargateServiceConfiguration {
    public static final String DEFAULT_CONFIG_FILEPATH = "./config.json";
    public static final int DEFAULT_SERVICE_PORT = 11010;
    
    private int servicePort = DEFAULT_SERVICE_PORT;
    private GateKeeperServiceConfiguration gatekeeperServiceConfig;
    private GateKeeperClientConfiguration gatekeeperClientConfig;
    
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
    
    public void setServicePort(int port) {
        this.servicePort = port;
    }
    
    public int getServicePort() {
        return this.servicePort;
    }
    
    public void setGatekeeperClientConfiguration(GateKeeperClientConfiguration conf) {
        this.gatekeeperClientConfig = conf;
    }
    
    public GateKeeperClientConfiguration getGatekeeperClientConfiguration() {
        return this.gatekeeperClientConfig;
    }
    
    public void setGatekeeperServiceConfiguration(GateKeeperServiceConfiguration conf) {
        this.gatekeeperServiceConfig = conf;
    }
    
    public GateKeeperServiceConfiguration getGatekeeperServiceConfiguration() {
        return this.gatekeeperServiceConfig;
    }
}
