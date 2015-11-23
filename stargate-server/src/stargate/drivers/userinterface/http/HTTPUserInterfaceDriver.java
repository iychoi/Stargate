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

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.userinterface.AUserInterfaceDriver;
import stargate.commons.userinterface.AUserInterfaceDriverConfiguration;
import stargate.commons.userinterface.AUserInterfaceServer;

/**
 *
 * @author iychoi
 */
public class HTTPUserInterfaceDriver extends AUserInterfaceDriver {

    private static final Log LOG = LogFactory.getLog(HTTPUserInterfaceDriver.class);
    
    private HTTPUserInterfaceDriverConfiguration config;
    private HTTPUserInterfaceServer server;
    
    public HTTPUserInterfaceDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HTTPUserInterfaceDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HTTPUserInterfaceDriverConfiguration");
        }
        
        this.config = (HTTPUserInterfaceDriverConfiguration) config;
    }
    
    public HTTPUserInterfaceDriver(AUserInterfaceDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HTTPUserInterfaceDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HTTPUserInterfaceDriverConfiguration");
        }
        
        this.config = (HTTPUserInterfaceDriverConfiguration) config;
    }
    
    public HTTPUserInterfaceDriver(HTTPUserInterfaceDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        // start server
        this.server = new HTTPUserInterfaceServer(this.config.getServicePort());
        this.server.start();
    }

    @Override
    public synchronized void stopDriver() throws IOException {
        // stop server
        this.server.stop();
    }
    
    @Override
    public String getDriverName() {
        return "HTTPUserInterfaceDriver";
    }

    @Override
    public AUserInterfaceServer getUserInterfaceServer() throws IOException {
        return new HTTPUserInterfaceServlet();
    }
}
