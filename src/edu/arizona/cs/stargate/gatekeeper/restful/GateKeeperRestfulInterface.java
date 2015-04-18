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

package edu.arizona.cs.stargate.gatekeeper.restful;

import com.google.inject.Guice;
import com.google.inject.Stage;
import com.google.inject.servlet.GuiceFilter;
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
public class GateKeeperRestfulInterface {
    private static final Log LOG = LogFactory.getLog(GateKeeperRestfulInterface.class);
    
    private static GateKeeperRestfulInterface instance;
    private Server jettyWebServer;
    
    public static GateKeeperRestfulInterface getInstance() throws Exception {
        synchronized (GateKeeperRestfulInterface.class) {
            if(instance == null) {
                instance = new GateKeeperRestfulInterface();
            }
            return instance;
        }
    }
    
    GateKeeperRestfulInterface() throws Exception {
    }
    
    public synchronized void start(int port) throws Exception {
        // start web service
        this.jettyWebServer = new Server(port);
        
        // setting servlets
        ServletContextHandler context = new ServletContextHandler(this.jettyWebServer, "/", ServletContextHandler.SESSIONS);
        context.addFilter(GuiceFilter.class, "/*", EnumSet.<javax.servlet.DispatcherType>of(javax.servlet.DispatcherType.REQUEST, javax.servlet.DispatcherType.ASYNC));

        context.addServlet(DefaultServlet.class, "/*");
        
        this.jettyWebServer.start();
        
        // start servlets
        Guice.createInjector(Stage.PRODUCTION, new GateKeeperServletModule());
    }
    
    public synchronized void stop() throws InterruptedException {
        // stop web server
        try {
            this.jettyWebServer.join();
        } catch(Exception ex) {
            LOG.error(ex);
        }
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperRestfulInterface";
    }
}
