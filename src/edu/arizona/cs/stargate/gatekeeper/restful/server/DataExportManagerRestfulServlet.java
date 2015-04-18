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

package edu.arizona.cs.stargate.gatekeeper.restful.server;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExport;
import edu.arizona.cs.stargate.gatekeeper.dataexport.DataExportManager;
import edu.arizona.cs.stargate.gatekeeper.GateKeeperService;
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.ADataExportManagerRestfulAPI;
import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
@Path(ADataExportManagerRestfulAPI.BASE_PATH)
@Singleton
public class DataExportManagerRestfulServlet extends ADataExportManagerRestfulAPI {
    
    private static final Log LOG = LogFactory.getLog(DataExportManagerRestfulServlet.class);
    
    @GET
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetDataExportInfoText(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseGetDataExportInfoJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<DataExport>> responseGetDataExportInfoJSON(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            if(vpath != null) {
                if(vpath.equals("*")) {
                    return new RestfulResponse<Collection<DataExport>>(getAllDataExportInfo());
                } else {
                    DataExport info = getDataExportInfo(vpath);
                    List<DataExport> dataExportInfo = new ArrayList<DataExport>();
                    dataExportInfo.add(info);
                    
                    return new RestfulResponse<Collection<DataExport>>(Collections.unmodifiableCollection(dataExportInfo));
                }
            } else {
                return new RestfulResponse<Collection<DataExport>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<DataExport>>(ex);
        }
    }
    
    @Override
    public DataExport getDataExportInfo(String vpath) throws Exception {
        DataExportManager dem = getDataExportManager();
        return dem.getDataExport(vpath);
    }
    
    @Override
    public Collection<DataExport> getAllDataExportInfo() throws Exception {
        DataExportManager dem = getDataExportManager();
        return dem.getAllDataExports();
    }
    
    @POST
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseAddDataExportText(DataExport dataExportInfo) {
        try {
            return DataFormatUtils.toJSONFormat(responseAddDataExportJSON(dataExportInfo));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @POST
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseAddDataExportJSON(DataExport dataExportInfo) {
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
    public void addDataExport(DataExport info) throws Exception {
        DataExportManager dem = getDataExportManager();
        dem.addDataExport(info);
    }
    
    @DELETE
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteDataExportText(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            return DataFormatUtils.toJSONFormat(responseDeleteDataExportJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseDeleteDataExportJSON(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            if(vpath != null) {
                if(vpath.equals("*")) {
                    removeAllDataExport();
                } else {
                    removeDataExport(vpath);
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
    public void removeDataExport(String vpath) throws Exception {
        DataExportManager dem = getDataExportManager();
        dem.removeDataExport(vpath);
    }

    @Override
    public void removeAllDataExport() throws Exception {
        DataExportManager dem = getDataExportManager();
        dem.removeAllDataExports();
    }
    
    private DataExportManager getDataExportManager() {
        try {
            GateKeeperService gatekeeperService = GateKeeperService.getInstance();
            return gatekeeperService.getDataExportManager();
        } catch (ServiceNotStartedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
