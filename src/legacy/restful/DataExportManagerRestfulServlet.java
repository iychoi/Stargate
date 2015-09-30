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

package legacy.restful;
/*
import com.google.inject.Singleton;
import edu.arizona.cs.stargate.common.utils.HexaUtils;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.dataexport.DataExportManager;
import edu.arizona.cs.stargate.service.GateKeeperService;
import edu.arizona.cs.stargate.transport.http.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.ADataExportManagerRestfulAPI;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
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
*/
/**
 *
 * @author iychoi
 */
//@Path(ADataExportManagerRestfulAPI.BASE_PATH)
//@Singleton
public class DataExportManagerRestfulServlet {
    /*
    private static final Log LOG = LogFactory.getLog(DataExportManagerRestfulServlet.class);
    
    @GET
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetDataExportText(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            return HexaUtils.toJSONFormat(responseGetDataExportJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<DataExportEntry>> responseGetDataExportJSON(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            if(vpath != null) {
                if(vpath.equals("*")) {
                    return new RestfulResponse<Collection<DataExportEntry>>(getAllDataExports());
                } else {
                    DataExportEntry export = getDataExport(vpath);
                    List<DataExportEntry> exports = new ArrayList<DataExportEntry>();
                    exports.add(export);
                    
                    return new RestfulResponse<Collection<DataExportEntry>>(Collections.unmodifiableCollection(exports));
                }
            } else {
                return new RestfulResponse<Collection<DataExportEntry>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<DataExportEntry>>(ex);
        }
    }
    
    @Override
    public DataExportEntry getDataExport(String vpath) throws Exception {
        DataExportManager dem = getDataExportManager();
        return dem.getDataExport(vpath);
    }
    
    @Override
    public Collection<DataExportEntry> getAllDataExports() throws Exception {
        DataExportManager dem = getDataExportManager();
        return dem.getDataExport();
    }
    
    @POST
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseAddDataExportText(DataExportEntry export) {
        try {
            return HexaUtils.toJSONFormat(responseAddDataExportJSON(export));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @POST
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseAddDataExportJSON(DataExportEntry export) {
        try {
            if(export != null) {
                addDataExport(export);
                return new RestfulResponse<Boolean>(true);
            } else {
                return new RestfulResponse<Boolean>(false);
            }
        } catch(Exception ex) {
            return new RestfulResponse<Boolean>(ex);
        }
    }
    
    @Override
    public void addDataExport(DataExportEntry export) throws Exception {
        DataExportManager dem = getDataExportManager();
        dem.addDataExport(export);
    }
    
    @DELETE
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseDeleteDataExportText(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            return HexaUtils.toJSONFormat(responseDeleteDataExportJSON(vpath));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @DELETE
    @Path(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseDeleteDataExportJSON(
            @DefaultValue("null") @QueryParam("vpath") String vpath
    ) {
        try {
            if(vpath != null) {
                if(vpath.equals("*")) {
                    removeAllDataExports();
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
    public void removeAllDataExports() throws Exception {
        DataExportManager dem = getDataExportManager();
        dem.clearDataExport();
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
    */
}
