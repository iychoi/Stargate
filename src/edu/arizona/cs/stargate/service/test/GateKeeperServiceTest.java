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

package edu.arizona.cs.stargate.service.test;

import edu.arizona.cs.stargate.cache.service.DistributedCacheServiceConfiguration;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.common.ResourceLocator;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.common.cluster.ClusterNodeInfo;
import edu.arizona.cs.stargate.common.cluster.NodeAlreadyAddedException;
import edu.arizona.cs.stargate.gatekeeper.service.GateKeeperServiceConfiguration;
import edu.arizona.cs.stargate.gatekeeper.service.LocalClusterManager;
import edu.arizona.cs.stargate.gatekeeper.service.RecipeManagerConfiguration;
import edu.arizona.cs.stargate.service.StargateService;
import edu.arizona.cs.stargate.service.StargateServiceConfiguration;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public class GateKeeperServiceTest {
    public static void makeDummyServiceConf(File f) throws IOException {
        try {
            StargateServiceConfiguration serviceConf = new StargateServiceConfiguration();
            serviceConf.setServiceName("Stargate1");
            
            DistributedCacheServiceConfiguration dhtConf = new DistributedCacheServiceConfiguration();
            dhtConf.setMyHostAddr("localhost:10111");
            
            serviceConf.setDistributedCacheServiceConfiguration(dhtConf);
            
            GateKeeperServiceConfiguration gatekeeperConf = new GateKeeperServiceConfiguration();
            
            ClusterInfo clusterInfo = new ClusterInfo("local");
            clusterInfo.addNode(new ClusterNodeInfo("node1", "http://111.111.111.1"));
            clusterInfo.addNode(new ClusterNodeInfo("node2", "http://111.111.111.2"));
            clusterInfo.addNode(new ClusterNodeInfo("node3", "http://111.111.111.3"));
            clusterInfo.addNode(new ClusterNodeInfo("node4", "http://111.111.111.4"));
            
            gatekeeperConf.setClusterInfo(clusterInfo);
            
            RecipeManagerConfiguration recipeConfiguration = new RecipeManagerConfiguration();
            recipeConfiguration.setChunkSize(1024*1024);
            recipeConfiguration.setHashAlgorithm("SHA-1");
            
            gatekeeperConf.setRecipeManagerConfiguration(recipeConfiguration);
            
            serviceConf.setGatekeeperServiceConfiguration(gatekeeperConf);
            
            JsonSerializer serializer = new JsonSerializer(true);
            serializer.toJsonFile(f, serviceConf);
        } catch (NodeAlreadyAddedException ex) {
        } catch (URISyntaxException ex) {
        }
    }
    
    public static StargateServiceConfiguration getServiceConfiguration(File f) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        StargateServiceConfiguration conf = (StargateServiceConfiguration) serializer.fromJsonFile(f, StargateServiceConfiguration.class);
        return conf;
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            ResourceLocator rl = new ResourceLocator();
            
            File configFile;
            if(args.length != 0) {
                configFile = rl.getResourceLocation(args[0]);
            } else {
                configFile = rl.getResourceLocation(StargateServiceConfiguration.DEFAULT_CONFIG_FILEPATH);
            }
            
            if (!configFile.exists()) {
                makeDummyServiceConf(configFile);
            }
            
            StargateServiceConfiguration conf = getServiceConfiguration(configFile);
            
            StargateService instance = StargateService.getInstance(conf);
            instance.start();
            
            LocalClusterManager localClusterManager = instance.getGateKeeperService().getLocalClusterManager();
            Collection<ClusterNodeInfo> allNode = localClusterManager.getAllNode();
            System.out.println("Cluster Name : " + localClusterManager.getName());
            for(ClusterNodeInfo node : allNode) {
                System.out.println("Node : " + node.getName() + " / " + node.getAddr().toASCIIString());
            }
            
            instance.join();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
