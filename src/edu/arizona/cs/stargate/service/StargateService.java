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

import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class StargateService {
    private static final Log LOG = LogFactory.getLog(StargateService.class);
    
    private static StargateService instance;
    
    private StargateServiceConfiguration config;
    private GateKeeperService gatekeeperService;
    
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
    
    StargateService(StargateServiceConfiguration config) throws Exception {
        if(config == null) {
            throw new Exception("StargateServiceConfiguration is null. Failed to start StargateService.");
        } else {
            this.config = config;
            this.gatekeeperService = GateKeeperService.getInstance(config.getGatekeeperServiceConfiguration());
        }
    }
    
    public synchronized StargateServiceConfiguration getConfiguration() {
        return this.config;
    }
    
    public synchronized GateKeeperService getGateKeeperService() {
        return this.gatekeeperService;
    }
    
    public synchronized void start() throws Exception {
        this.gatekeeperService.start();
        LOG.info("Stargate service started");
    }
    
    public synchronized void stop() throws InterruptedException {
        this.gatekeeperService.stop();
        LOG.info("Stargate service stopped");
    }
    
    @Override
    public synchronized String toString() {
        return "StargateService";
    }
}
