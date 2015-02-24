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
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClient;
import edu.arizona.cs.stargate.gatekeeper.client.GateKeeperClientConfiguration;
import edu.arizona.cs.stargate.service.StargateServiceConfiguration;
import java.net.URI;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public class GateKeeperClientTest {
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            String gatekeeperService = "http://localhost:" + StargateServiceConfiguration.DEFAULT_SERVICE_PORT;
            if(args.length != 0) {
                gatekeeperService = args[0];
            }
            
            GateKeeperClientConfiguration conf = new GateKeeperClientConfiguration(new URI(gatekeeperService));
            
            GateKeeperClient client = new GateKeeperClient(conf);
            boolean live = client.checkLive();
            System.out.println("live : " + live);
            
            ClusterInfo localClusterInfo = client.getClusterManagerClient().getLocalClusterInfo();
            System.out.println("local cluster info : " + DataFormatter.toJSONFormat(localClusterInfo));
            
            client.getClusterManagerClient().removeAllRemoteCluster();
            
            Collection<ClusterInfo> remoteClusterInfo = client.getClusterManagerClient().getRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            System.out.println("Adding remote cluster info");
            
            ClusterInfo remoteCluster1 = new ClusterInfo("remote1");
            client.getClusterManagerClient().addRemoteCluster(remoteCluster1);
            
            remoteClusterInfo = client.getClusterManagerClient().getRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            ClusterInfo remoteCluster2 = new ClusterInfo("remote2");
            client.getClusterManagerClient().addRemoteCluster(remoteCluster2);
            
            remoteClusterInfo = client.getClusterManagerClient().getRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            client.getClusterManagerClient().removeAllRemoteCluster();
            
            remoteClusterInfo = client.getClusterManagerClient().getRemoteClusterInfo();
            System.out.println("remote cluster info : " + DataFormatter.toJSONFormat(remoteClusterInfo));
            
            
            client.stop();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
