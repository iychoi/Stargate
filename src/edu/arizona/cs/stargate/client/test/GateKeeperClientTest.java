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
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClientConfiguration;
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
    
    private GateKeeperClientConfiguration clientConfig;
    private GateKeeperClient client;
    
    public void prepareClient(URI gatekeeperServiceURL) {
        try {
            this.clientConfig = new GateKeeperClientConfiguration(gatekeeperServiceURL);
            this.client = new GateKeeperClient(this.clientConfig);
            
            testCheckLive();
            testClusterInfo();
            //testDataExport();
            
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
            
            client.getClusterManagerClient().removeAllRemoteCluster();
            
            Collection<ClusterInfo> remoteClusterInfo = client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            System.out.println("Adding remote cluster info");
            
            ClusterInfo remoteCluster1 = new ClusterInfo("remote1");
            client.getClusterManagerClient().addRemoteCluster(remoteCluster1);
            
            remoteClusterInfo = client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            ClusterInfo remoteCluster2 = new ClusterInfo("remote2");
            client.getClusterManagerClient().addRemoteCluster(remoteCluster2);
            
            remoteClusterInfo = client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            client.getClusterManagerClient().removeAllRemoteCluster();
            
            remoteClusterInfo = client.getClusterManagerClient().getAllRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void testDataExport() {
        try {
            Collection<DataExportInfo> dataExportInfo = this.client.getDataExportManagerClient().getAllDataExportInfo();
            System.out.println("data export info : " + DataFormatter.toJSONFormat(dataExportInfo));
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
