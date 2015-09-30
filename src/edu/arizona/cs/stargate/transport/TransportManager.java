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
package edu.arizona.cs.stargate.transport;

import edu.arizona.cs.stargate.cluster.ClusterManager;
import edu.arizona.cs.stargate.cluster.RemoteCluster;
import edu.arizona.cs.stargate.policy.PolicyManager;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class TransportManager {

    private static final Log LOG = LogFactory.getLog(TransportManager.class);
    
    private static TransportManager instance;

    private ATransportDriver driver;
    
    public static TransportManager getInstance(ATransportDriver driver) {
        synchronized (TransportManager.class) {
            if(instance == null) {
                instance = new TransportManager(driver);
            }
            return instance;
        }
    }
    
    public static TransportManager getInstance() throws ServiceNotStartedException {
        synchronized (TransportManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("TransportManager is not started");
            }
            return instance;
        }
    }
    
    TransportManager(ATransportDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        this.driver = driver;
    }
    
    public ATransportDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        this.driver.startDriver();
    }

    public synchronized void stop() throws IOException {
        this.driver.stopDriver();
    }
    
    public synchronized ATransportClient getTransportClient(RemoteCluster remoteCluster, PolicyManager policyManager) throws IOException {
        return this.driver.getTransportClient(remoteCluster, policyManager);
    }
    
    public synchronized ATransportClient getTransportClient(ClusterManager clusterManager, RemoteCluster remoteCluster, PolicyManager policyManager) throws IOException {
        return this.driver.getTransportClient(clusterManager, remoteCluster, policyManager);
    }
    
    @Override
    public synchronized String toString() {
        return "TransportManager";
    }
}
