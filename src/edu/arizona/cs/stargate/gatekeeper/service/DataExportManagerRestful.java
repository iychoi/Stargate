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

package edu.arizona.cs.stargate.gatekeeper.service;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.DataFormatter;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.gatekeeper.AClusterManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.ADataExportManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import java.io.IOException;
import java.util.Collection;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author iychoi
 */
@Path(AClusterManagerAPI.PATH)
@Singleton
public class DataExportManagerRestful extends ADataExportManagerAPI {
    @GET
    @Path(ADataExportManagerAPI.GET_DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetDataExportInfoText() {
        try {
            return DataFormatter.toJSONFormat(responseGetDataExportInfoJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ADataExportManagerAPI.GET_DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<DataExportInfo>> responseGetDataExportInfoJSON() {
        try {
            return new RestfulResponse<Collection<DataExportInfo>>(getDataExportInfo());
        } catch(Exception ex) {
            return new RestfulResponse<Collection<DataExportInfo>>(ex);
        }
    }
    
    @Override
    public Collection<DataExportInfo> getDataExportInfo() throws Exception {
        DataExportManager dem = DataExportManager.getInstance();
        return dem.getAllDataExportInfo();
    }

    
    
    @POST
    @Path(ADataExportManagerAPI.ADD_DATA_EXPORT_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseAddDataExportText(DataExportInfo dataExportInfo) {
        try {
            return DataFormatter.toJSONFormat(responseAddDataExportJSON(dataExportInfo));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @POST
    @Path(ADataExportManagerAPI.ADD_DATA_EXPORT_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseAddDataExportJSON(DataExportInfo dataExportInfo) {
        try {
            if(dataExportInfo != null) {
                addDataExport(dataExportInfo);
                return new RestfulResponse<Boolean>(true);
            } else {
                return new RestfulResponse<Boolean>(false);
            }
        } catch(Exception ex) {
            return new RestfulResponse<Boolean>(ex);
        }
    }
    
    @Override
    public void addDataExport(DataExportInfo info) throws Exception {
        DataExportManager dem = DataExportManager.getInstance();
        dem.addDataExport(info);
    }
    
    @DELETE
    @Path(ADataExportManagerAPI.DELETE_DATA_EXPORT_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteDataExportText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatter.toJSONFormat(responseDeleteDataExportJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(ADataExportManagerAPI.DELETE_DATA_EXPORT_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseDeleteDataExportJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            if(name != null) {
                if(name.equals("*")) {
                    removeAllDataExport();
                } else {
                    removeDataExport(name);
                }
                return new RestfulResponse<Boolean>(true);
            } else {
                return new RestfulResponse<Boolean>(false);
            }
        } catch(Exception ex) {
            return new RestfulResponse<Boolean>(ex);
        }
    }

    @Override
    public void removeDataExport(String name) throws Exception {
        DataExportManager dem = DataExportManager.getInstance();
        dem.removeDataExport(name);
    }

    @Override
    public void removeAllDataExport() throws Exception {
        DataExportManager dem = DataExportManager.getInstance();
        dem.removeAllDataExport();
    }
}
