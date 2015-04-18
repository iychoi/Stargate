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

package edu.arizona.cs.stargate.gatekeeper.restful.client.test;

import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import edu.arizona.cs.stargate.gatekeeper.restful.client.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.restful.client.GateKeeperRestfulClientConfiguration;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperServiceConfiguration;
import edu.arizona.cs.stargate.service.StargateServiceConfiguration;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public class GateKeeperClientTest {
    
    private GateKeeperRestfulClientConfiguration clientConfig;
    private GateKeeperClient client;
    
    public void prepareClient(URI gatekeeperServiceURL) {
        try {
            this.clientConfig = new GateKeeperRestfulClientConfiguration(gatekeeperServiceURL);
            this.client = new GateKeeperClient(this.clientConfig);
            
            testCheckLive();
            testClusterInfo();
            testDataExport();
            
            client.stop();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String gatekeeperServiceURL = "http://localhost:" + GateKeeperServiceConfiguration.DEFAULT_SERVICE_PORT;
        if(args.length != 0) {
            gatekeeperServiceURL = args[0];
        }
        
        GateKeeperClientTest test = new GateKeeperClientTest();
        try {
            test.prepareClient(new URI(gatekeeperServiceURL));
        } catch (URISyntaxException ex) {
            ex.printStackTrace();
        }
    }

    private void testCheckLive() {
        boolean live = this.client.checkLive();
        System.out.println("live : " + live);
    }

    private void testClusterInfo() {
        try {
            Cluster localClusterInfo = this.client.getClusterManagerClient().getLocalCluster();
            System.out.println("local cluster info : " + DataFormatUtils.toJSONFormat(localClusterInfo));
            
            this.client.getClusterManagerClient().removeAllRemoteClusters();
            
            Collection<Cluster> remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusters();
            System.out.println("remote cluster info : " + DataFormatUtils.toJSONFormat(remoteClusterInfo));
            
            System.out.println("Adding remote cluster info");
            
            Cluster remoteCluster1 = new Cluster("remote1");
            this.client.getClusterManagerClient().addRemoteCluster(remoteCluster1);
            
            remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusters();
            System.out.println("remote cluster info : " + DataFormatUtils.toJSONFormat(remoteClusterInfo));
            
            Cluster remoteCluster2 = new Cluster("remote2");
            this.client.getClusterManagerClient().addRemoteCluster(remoteCluster2);
            
            remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusters();
            System.out.println("remote cluster info : " + DataFormatUtils.toJSONFormat(remoteClusterInfo));
            
            this.client.getClusterManagerClient().removeAllRemoteClusters();
            
            remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusters();
            System.out.println("remote cluster info : " + DataFormatUtils.toJSONFormat(remoteClusterInfo));
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void testDataExport() {
        try {
            this.client.getDataExportManagerClient().removeAllDataExports();
            
            Collection<DataExport> dataExportInfo = this.client.getDataExportManagerClient().getAllDataExports();
            System.out.println("data export info : " + DataFormatUtils.toJSONFormat(dataExportInfo));
            
            DataExport export = new DataExport("/aaa/bbb", "file:///home/iychoi/NetBeansProjects/Stargate/libs/hadoop-core-0.20.2-cdh3u5.jar");
            this.client.getDataExportManagerClient().addDataExport(export);
            
            dataExportInfo = this.client.getDataExportManagerClient().getAllDataExports();
            System.out.println("data export info : " + DataFormatUtils.toJSONFormat(dataExportInfo));
            
            for(DataExport dei : dataExportInfo) {
                // check recipe
                LocalRecipe recipe = this.client.getRecipeManagerClient().getRecipe(dei.getResourcePath());
                System.out.println("recipe of " + recipe.getResourcePath().toASCIIString());
                System.out.println(DataFormatUtils.toJSONFormat(recipe));
            }
            
            
            // retest
            Thread.sleep(3000);
            for(DataExport dei : dataExportInfo) {
                // check recipe
                LocalRecipe recipe = this.client.getRecipeManagerClient().getRecipe(dei.getResourcePath());
                System.out.println("recipe of " + recipe.getResourcePath().toASCIIString());
                System.out.println(DataFormatUtils.toJSONFormat(recipe));
            }
            
            // downloadtest
            Thread.sleep(3000);
            byte[] buffer = new byte[100*1024];
            for(DataExport dei : dataExportInfo) {
                // check recipe
                //InputStream is = this.client.getDataExportManagerClient().getDataChunk(dei.getVirtualPath(), 0, 1024);
                //System.out.println("reading first 1024KB from " + dei.getVirtualPath());
                //is.read(buffer);

                //System.out.println(new String(buffer));
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
