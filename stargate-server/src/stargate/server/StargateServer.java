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
package stargate.server;

import java.io.File;
import java.util.Collection;
import stargate.commons.cluster.Node;
import stargate.commons.common.LocalResourceLocator;
import stargate.server.cluster.LocalClusterManager;
import stargate.server.service.StargateService;
import stargate.server.service.StargateServiceConfiguration;

/**
 *
 * @author iychoi
 */
public class StargateServer {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            LocalResourceLocator rl = new LocalResourceLocator();
            
            File configFile = null;
            if(args.length != 0) {
                if(args[0] != null && !args[0].isEmpty()) {
                    configFile = rl.getResourceLocation(args[0]);
                    if (!configFile.exists()) {
                        System.err.println("configuration file (" + configFile.getAbsolutePath() + ") not found");
                        return;
                    }
                }
            }
            
            StargateServiceConfiguration serviceConfiguration;
            if(configFile != null) {
                serviceConfiguration = StargateServiceConfiguration.createInstance(configFile);
            } else {
                serviceConfiguration = new StargateServiceConfiguration();
            }
            
            StargateService instance = StargateService.getInstance(serviceConfiguration);
            instance.start();
            
            LocalClusterManager localClusterManager = instance.getClusterManager().getLocalClusterManager();
            Collection<Node> allNode = localClusterManager.getNode();
            System.out.println("Cluster Name : " + localClusterManager.getName());
            for(Node node : allNode) {
                System.out.println("Node : " + node.getName());
                System.out.println("Obj : " + node.toJson());
            }
            
            System.out.println("Stargate service is running");
            System.out.println("press ctrl + c for stopping the service");
            
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    // service loop
                    Thread.sleep(1000);
                } catch(InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            
            System.out.println("Stargate service is stopping");
            
            instance.stop();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
