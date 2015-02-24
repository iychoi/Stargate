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
    private ClusterManager clusterManager;
    
    public static GateKeeperService getInstance(GateKeeperServiceConfiguration config) {
        synchronized (GateKeeperService.class) {
            if(instance == null) {
                instance = new GateKeeperService(config);
            }
            return instance;
        }
    }
    
    GateKeeperService(GateKeeperServiceConfiguration config) {
        if(config == null) {
            this.config = new GateKeeperServiceConfiguration();
        } else {
            this.config = config;
        }
    }
    
    public synchronized void start() throws Exception {
        this.clusterManager = ClusterManager.getInstance();
        if(this.config.getClusterInfo() != null) {
            this.clusterManager.setLocalCluster(this.config.getClusterInfo());
        }
        
        Guice.createInjector(Stage.PRODUCTION, new GatekeeperServletModule());
    }
    
    public synchronized ClusterManager getClusterManager() {
        return this.clusterManager;
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperService";
    }
}
