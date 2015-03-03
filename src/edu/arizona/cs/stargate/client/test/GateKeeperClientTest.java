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

package edu.arizona.cs.stargate.client.test;

import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.common.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.common.recipe.Recipe;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClientConfiguration;
import edu.arizona.cs.stargate.service.StargateServiceConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public class GateKeeperClientTest {
    
    private GateKeeperClientConfiguration clientConfig;
    private GateKeeperClient client;
    
    public void prepareClient(URI gatekeeperServiceURL) {
        try {
            this.clientConfig = new GateKeeperClientConfiguration(gatekeeperServiceURL);
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
        String gatekeeperServiceURL = "http://localhost:" + StargateServiceConfiguration.DEFAULT_SERVICE_PORT;
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
            ClusterInfo localClusterInfo = this.client.getClusterManagerClient().getLocalClusterInfo();
            System.out.println("local cluster info : " + DataFormatter.toJSONFormat(localClusterInfo));
            
            this.client.getClusterManagerClient().removeAllRemoteCluster();
            
            Collection<ClusterInfo> remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            System.out.println("Adding remote cluster info");
            
            ClusterInfo remoteCluster1 = new ClusterInfo("remote1");
            this.client.getClusterManagerClient().addRemoteCluster(remoteCluster1);
            
            remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            ClusterInfo remoteCluster2 = new ClusterInfo("remote2");
            this.client.getClusterManagerClient().addRemoteCluster(remoteCluster2);
            
            remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            this.client.getClusterManagerClient().removeAllRemoteCluster();
            
            remoteClusterInfo = this.client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void testDataExport() {
        try {
            this.client.getDataExportManagerClient().removeAllDataExport();
            
            Collection<DataExportInfo> dataExportInfo = this.client.getDataExportManagerClient().getAllDataExportInfo();
            System.out.println("data export info : " + DataFormatter.toJSONFormat(dataExportInfo));
            
            DataExportInfo info = new DataExportInfo("test");
            DataExportEntry entry = new DataExportEntry("/aaa/bbb", "file:///home/iychoi/NetBeansProjects/Stargate/libs/hadoop-core-0.20.2-cdh3u5.jar");
            info.addExportEntry(entry);
            this.client.getDataExportManagerClient().addDataExport(info);
            
            dataExportInfo = this.client.getDataExportManagerClient().getAllDataExportInfo();
            System.out.println("data export info : " + DataFormatter.toJSONFormat(dataExportInfo));
            
            for(DataExportInfo dei : dataExportInfo) {
                for(DataExportEntry dee : dei.getAllExportEntry()) {
                    // check recipe
                    Recipe recipe = this.client.getRecipeManagerClient().getRecipe(dee.getResourcePath());
                    System.out.println("recipe of " + recipe.getResourcePath().toASCIIString());
                    System.out.println(DataFormatter.toJSONFormat(recipe));
                }
            }
            
            
            // retest
            Thread.sleep(3000);
            for(DataExportInfo dei : dataExportInfo) {
                for(DataExportEntry dee : dei.getAllExportEntry()) {
                    // check recipe
                    Recipe recipe = this.client.getRecipeManagerClient().getRecipe(dee.getResourcePath());
                    System.out.println("recipe of " + recipe.getResourcePath().toASCIIString());
                    System.out.println(DataFormatter.toJSONFormat(recipe));
                }
            }
            
            // downloadtest
            Thread.sleep(3000);
            byte[] buffer = new byte[100*1024];
            for(DataExportInfo dei : dataExportInfo) {
                for(DataExportEntry dee : dei.getAllExportEntry()) {
                    // check recipe
                    InputStream is = this.client.getDataExportManagerClient().getDataChunk("test", dee.getVirtualPath(), 0, 1024);
                    System.out.println("reading first 1024KB from " + dee.getVirtualPath());
                    is.read(buffer);
                    
                    System.out.println(new String(buffer));
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
