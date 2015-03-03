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

package edu.arizona.cs.stargate.gatekeeper.client;

import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.gatekeeper.AGateKeeperAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class GateKeeperClient extends AGateKeeperAPI {
    
    private static final Log LOG = LogFactory.getLog(GateKeeperClient.class);
    
    private GateKeeperClientConfiguration config;
    private GateKeeperRPCClient rpcClient;
    private ClusterManagerClient clusterManagerClient;
    private DataExportManagerClient dataExportManagerClient;
    private RecipeManagerClient recipeManagerClient;
    
    public GateKeeperClient(GateKeeperClientConfiguration conf) {
        this.config = conf;
        this.rpcClient = new GateKeeperRPCClient(conf);
        this.clusterManagerClient = new ClusterManagerClient(this);
        this.dataExportManagerClient = new DataExportManagerClient(this);
        this.recipeManagerClient = new RecipeManagerClient(this);
    }
    
    public ClusterManagerClient getClusterManagerClient() {
        return this.clusterManagerClient;
    }
    
    public DataExportManagerClient getDataExportManagerClient() {
        return this.dataExportManagerClient;
    }
    
    public RecipeManagerClient getRecipeManagerClient() {
        return this.recipeManagerClient;
    }
    
    public GateKeeperRPCClient getRPCClient() {
        return this.rpcClient;
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperClient";
    }
    
    public void stop() {
        this.rpcClient.stop();
    }
    
    public String getResourcePath(String path) {
        if(AGateKeeperAPI.PATH.endsWith("/") &&
                path.startsWith("/")) {
            return AGateKeeperAPI.PATH + path.substring(1);
        }
        return AGateKeeperAPI.PATH + path;
    }

    @Override
    public boolean checkLive() {
        RestfulResponse<Boolean> response;
        try {
            response = (RestfulResponse<Boolean>) this.rpcClient.get(getResourcePath(AGateKeeperAPI.CHECK_LIVE_PATH), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
        
        if(response.getException() != null) {
            return false;
        } else {
            return response.getResponse().booleanValue();
        }
    }
}
