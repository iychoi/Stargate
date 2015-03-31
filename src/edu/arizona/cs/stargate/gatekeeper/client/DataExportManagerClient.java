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
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.gatekeeper.ADataExportManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class DataExportManagerClient extends ADataExportManagerAPI {
    
    private static final Log LOG = LogFactory.getLog(DataExportManagerClient.class);
    
    private GateKeeperClient gatekeeperClient;
    private GateKeeperRPCClient gatekeeperRPCClient;

    public DataExportManagerClient(GateKeeperClient gatekeeperClient) {
        this.gatekeeperClient = gatekeeperClient;
        this.gatekeeperRPCClient = gatekeeperClient.getRPCClient();
    }
    
    public String getResourcePath(String path) {
        return ADataExportManagerAPI.PATH + path;
    }
    
    public String getResourcePath(String path, Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, String>> entrySet = params.entrySet();
        for(Map.Entry<String, String> entry : entrySet) {
            sb.append(entry.getKey() + "=" + entry.getValue() + "&");
        }
        return ADataExportManagerAPI.PATH + path + "?" + sb.toString();
    }
    
    @Override
    public Collection<DataExportInfo> getAllDataExportInfo() throws Exception {
        RestfulResponse<Collection<DataExportInfo>> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("vpath", "*");
            response = (RestfulResponse<Collection<DataExportInfo>>) this.gatekeeperRPCClient.get(getResourcePath(ADataExportManagerAPI.GET_DATA_EXPORT_INFO_PATH, params), new GenericType<RestfulResponse<Collection<DataExportInfo>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            return response.getResponse();
        }
    }
    
    @Override
    public DataExportInfo getDataExportInfo(String vpath) throws Exception {
        RestfulResponse<Collection<DataExportInfo>> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("vpath", vpath);
            response = (RestfulResponse<Collection<DataExportInfo>>) this.gatekeeperRPCClient.get(getResourcePath(ADataExportManagerAPI.GET_DATA_EXPORT_INFO_PATH, params), new GenericType<RestfulResponse<Collection<DataExportInfo>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            Collection<DataExportInfo> dataExportInfo = response.getResponse();
            Iterator<DataExportInfo> iterator = dataExportInfo.iterator();
            if(iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }
    }
    
    @Override
    public void addDataExport(DataExportInfo info) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.post(getResourcePath(ADataExportManagerAPI.ADD_DATA_EXPORT_PATH), info, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }

    @Override
    public void removeDataExport(String vpath) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("vpath", vpath);
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(getResourcePath(ADataExportManagerAPI.DELETE_DATA_EXPORT_PATH, params), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }

    @Override
    public void removeAllDataExport() throws Exception {
        RestfulResponse<Boolean> response;
        try {
            Map<String, String> params = new HashMap<String, String>();
            params.put("vpath", "*");
            response = (RestfulResponse<Boolean>) this.gatekeeperRPCClient.delete(getResourcePath(ADataExportManagerAPI.DELETE_DATA_EXPORT_PATH, params), new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
}
