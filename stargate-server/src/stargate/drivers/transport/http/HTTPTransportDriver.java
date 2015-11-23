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
package stargate.drivers.transport.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.policy.ClusterPolicy;
import stargate.commons.transport.ATransportClient;
import stargate.commons.transport.ATransportDriver;
import stargate.commons.transport.ATransportDriverConfiguration;
import stargate.commons.transport.ATransportServer;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.utils.IPUtils;
import stargate.commons.utils.NodeUtils;
import stargate.server.cluster.LocalClusterManager;
import stargate.server.service.StargateService;

/**
 *
 * @author iychoi
 */
public class HTTPTransportDriver extends ATransportDriver {

    private static final Log LOG = LogFactory.getLog(HTTPTransportDriver.class);
    
    private HTTPTransportDriverConfiguration config;
    private HTTPTransportServer server;
    
    public HTTPTransportDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HTTPTransportDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HTTPTransportDriverConfiguration");
        }
        
        this.config = (HTTPTransportDriverConfiguration) config;
    }
    
    public HTTPTransportDriver(ATransportDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HTTPTransportDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HTTPTransportDriverConfiguration");
        }
        
        this.config = (HTTPTransportDriverConfiguration) config;
    }
    
    public HTTPTransportDriver(HTTPTransportDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        // start server
        this.server = new HTTPTransportServer(this.config.getServicePort());
        this.server.start();
    }

    @Override
    public synchronized void stopDriver() throws IOException {
        // stop server
        this.server.stop();
    }
    
    @Override
    public String getDriverName() {
        return "HTTPTransportDriver";
    }
    
    public StargateService getStargateService() throws Exception {
        if(this.service instanceof StargateService) {
            return (StargateService)this.service;
        } else {
            throw new Exception("service object is not instance of StargateService");
        }
    }
    
    private LocalClusterManager getLocalClusterManager() throws Exception {
        return getStargateService().getClusterManager().getLocalClusterManager();
    }
    
    private ClusterPolicy getClusterPolicy() throws Exception {
        return getStargateService().getPolicyManager().getClusterPolicy();
    }
    
    @Override
    public ATransportClient getTransportClient(RemoteCluster remoteCluster) throws IOException {
        if(remoteCluster == null || remoteCluster.isEmpty()) {
            throw new IllegalArgumentException("remoteCluster is null or empty");
        }
        
        // RANDOM based contact
        /*
        Collection<Node> contactOrder = NodeUtils.getRandomContactNodeList(remoteCluster);
        for(Node n : contactOrder) {
            TransportServiceInfo transportServiceInfo = n.getTransportServiceInfo();
            if(transportServiceInfo.getDriverClass().equals(HTTPTransportDriver.class)) {
                try {
                    HTTPTransportClient client = new HTTPTransportClient(transportServiceInfo.getConnectionURI(), this.config.getThreadPoolSize());
                    if(client.isLive()) {
                        return client;
                    } else {
                        remoteCluster.reportNodeUnreachable(cp, n.getName());
                    }
                } catch (Exception ex) {
                    remoteCluster.reportNodeUnreachable(cp, n.getName());
                }
            }
        }
        */
        
        // heuristic - 1:1 mapping
        try {
            ClusterPolicy cp = getClusterPolicy();
            LocalClusterManager localClusterManager = getLocalClusterManager();
            Node localNode = localClusterManager.getLocalNode();
            Collection<Node> node = localClusterManager.getNode();

            Collection<Node> contactOrder = NodeUtils.getLocalClusterAwareContactNodeList(node, localNode, remoteCluster);
            for(Node n : contactOrder) {
                TransportServiceInfo transportServiceInfo = n.getTransportServiceInfo();
                if(transportServiceInfo.getDriverClass().equals(HTTPTransportDriver.class)) {
                    try {
                        HTTPTransportClient client = new HTTPTransportClient(transportServiceInfo.getConnectionURI(), this.config.getThreadPoolSize());
                        if(client.isLive()) {
                            return client;
                        } else {
                            remoteCluster.reportNodeUnreachable(cp, n.getName());
                        }
                    } catch (Exception ex) {
                        remoteCluster.reportNodeUnreachable(cp, n.getName());
                    }
                }
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        
        throw new IOException("unable to connect to a remote cluster " + remoteCluster.getName());
    }

    @Override
    public ATransportServer getTransportServer() {
        return new HTTPTransportServlet();
    }

    @Override
    public URI getServiceURI() throws IOException {
        try {
            Collection<String> hostAddress = IPUtils.getHostAddress();
            List<String> acceptedHostAddr = new ArrayList<String>();
            
            for(String addr : hostAddress) {
                Pattern pattern = Pattern.compile(this.config.getServiceHostNamePattern());
                Matcher matcher = pattern.matcher(addr);
                if(matcher.matches()) {
                    acceptedHostAddr.add(addr);
                }
            }
            
            if(acceptedHostAddr.isEmpty()) {
                return new URI("http://localhost:" + this.config.getServicePort());
            } else {
                for(String addr : acceptedHostAddr) {
                    // preferred - domainname
                    if(IPUtils.isDomainName(addr)) {
                        return new URI("http://" + addr + ":" + this.config.getServicePort());
                    }
                }
                
                for(String addr : acceptedHostAddr) {
                    // preferred - public address
                    if(IPUtils.isPublicIPAddress(addr)) {
                        return new URI("http://" + addr + ":" + this.config.getServicePort());
                    }
                }
                
                return new URI("http://" + acceptedHostAddr.get(0) + ":" + this.config.getServicePort());
            }
            
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
}
