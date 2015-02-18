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

import com.google.inject.servlet.GuiceFilter;
import edu.arizona.cs.stargate.gatekeeper.service.GateKeeperService;
import java.util.EnumSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;

/**
 *
 * @author iychoi
 */
public class StargateService {
    private static final Log LOG = LogFactory.getLog(StargateService.class);
    
    private static StargateService instance;
    
    private StargateServiceConfiguration config;
    private Server jettyServer;
    
    private GateKeeperService gatekeeperService;
    
    public static StargateService getInstance(StargateServiceConfiguration config) {
        synchronized (StargateService.class) {
            if(instance == null) {
                instance = new StargateService(config);
            }
            return instance;
        }
    }
    
    StargateService(StargateServiceConfiguration config) {
        this.config = config;
        this.gatekeeperService = GateKeeperService.getInstance(config.getGatekeeperServiceConfiguration());
    }
    
    public synchronized void start() throws Exception {
        //Guice.createInjector(Stage.PRODUCTION, new GatekeeperServletModule()); 
        this.gatekeeperService.start();
        
        this.jettyServer = new Server(this.config.getServicePort());
        
        // setting servlets
        ServletContextHandler context = new ServletContextHandler(this.jettyServer, "/", ServletContextHandler.SESSIONS);
        context.addFilter(GuiceFilter.class, "/*", EnumSet.<javax.servlet.DispatcherType>of(javax.servlet.DispatcherType.REQUEST, javax.servlet.DispatcherType.ASYNC));

        context.addServlet(DefaultServlet.class, "/*");
        
        this.jettyServer.start();
        LOG.info("Stargate service started on port " + this.config.getServicePort());
    }
    
    public synchronized void join() throws InterruptedException {
        this.jettyServer.join();
        LOG.info("Stargate service stopped");
    }
    
    @Override
    public synchronized String toString() {
        return "StargateService";
    }
}
