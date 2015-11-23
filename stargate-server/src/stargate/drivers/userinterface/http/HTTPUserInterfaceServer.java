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
package stargate.drivers.userinterface.http;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import stargate.server.CommonsLoggingLog;

/**
 *
 * @author iychoi
 */
public class HTTPUserInterfaceServer {
    
    private static final Log LOG = LogFactory.getLog(HTTPUserInterfaceServer.class);
    
    private int servicePort;
    private Server jettyWebServer;
    
    public HTTPUserInterfaceServer(int port) {
        if(port <= 0) {
            throw new IllegalArgumentException("port is invalid");
        }
        
        this.servicePort = port;
    }
    
    public synchronized void start() throws IOException {
        CommonsLoggingLog commonsLoggingLog = new CommonsLoggingLog();
        org.eclipse.jetty.util.log.Log.__logClass = CommonsLoggingLog.class.getCanonicalName();
        org.eclipse.jetty.util.log.Log.setLog(commonsLoggingLog);
        
        // configure servlets
        ServletHolder sh = new ServletHolder(ServletContainer.class);  
        
        sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig"); 
        sh.setInitParameter("com.sun.jersey.config.property.packages", HTTPUserInterfaceServlet.class.getPackage().getName());
        sh.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true"); 
        
        // start web service
        this.jettyWebServer = new Server(this.servicePort);
        
        // setting servlets
        ServletContextHandler context = new ServletContextHandler(this.jettyWebServer, "/", ServletContextHandler.SESSIONS);
        context.addServlet(sh, "/*");
        
        try {
            this.jettyWebServer.start();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public synchronized void stop() throws IOException {
        // stop web server
        try {
            this.jettyWebServer.join();
        } catch(Exception ex) {
            LOG.error("Exception occurred while stopping web server", ex);
        }
    }
}
