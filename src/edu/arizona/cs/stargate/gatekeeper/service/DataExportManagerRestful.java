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
import edu.arizona.cs.stargate.common.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.common.dataexport.DataExportInfo;
import edu.arizona.cs.stargate.common.recipe.ChunkReaderFactory;
import edu.arizona.cs.stargate.gatekeeper.ADataExportManagerAPI;
import edu.arizona.cs.stargate.gatekeeper.response.RestfulResponse;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
@Path(ADataExportManagerAPI.PATH)
@Singleton
public class DataExportManagerRestful extends ADataExportManagerAPI {
    
    private static final Log LOG = LogFactory.getLog(DataExportManagerRestful.class);
    
    @GET
    @Path(ADataExportManagerAPI.GET_DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseGetDataExportInfoText(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            return DataFormatter.toJSONFormat(responseGetDataExportInfoJSON(name));
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(ADataExportManagerAPI.GET_DATA_EXPORT_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Collection<DataExportInfo>> responseGetDataExportInfoJSON(
            @DefaultValue("null") @QueryParam("name") String name
    ) {
        try {
            if(name != null) {
                if(name.equals("*")) {
                    return new RestfulResponse<Collection<DataExportInfo>>(getAllDataExportInfo());
                } else {
                    DataExportInfo info = getDataExportInfo(name);
                    List<DataExportInfo> dataExportInfo = new ArrayList<DataExportInfo>();
                    dataExportInfo.add(info);
                    
                    return new RestfulResponse<Collection<DataExportInfo>>(Collections.unmodifiableCollection(dataExportInfo));
                }
            } else {
                return new RestfulResponse<Collection<DataExportInfo>>(new Exception("invalid parameter"));
            }
        } catch(Exception ex) {
            return new RestfulResponse<Collection<DataExportInfo>>(ex);
        }
    }
    
    @Override
    public DataExportInfo getDataExportInfo(String name) throws Exception {
        DataExportManager dem = getDataExportManager();
        return dem.getDataExportInfo(name);
    }
    
    @Override
    public Collection<DataExportInfo> getAllDataExportInfo() throws Exception {
        DataExportManager dem = getDataExportManager();
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
        DataExportManager dem = getDataExportManager();
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
        DataExportManager dem = getDataExportManager();
        dem.removeDataExport(name);
    }

    @Override
    public void removeAllDataExport() throws Exception {
        DataExportManager dem = getDataExportManager();
        dem.removeAllDataExport();
    }
    
    @GET
    @Path(ADataExportManagerAPI.GET_DATA_CHUNK_PATH)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response responseGetDataChunk(
            @DefaultValue("null") @QueryParam("name") String name,
            @DefaultValue("null") @QueryParam("path") String path,
            @DefaultValue("0") @QueryParam("offset") long offset,
            @DefaultValue("0") @QueryParam("len") int len
    ) throws Exception {
        if(name != null && path != null && len > 0) {
            
            final InputStream is = getDataChunk(name, path, offset, len);
            
            StreamingOutput stream = new StreamingOutput() {

                @Override
                public void write(OutputStream out) throws IOException, WebApplicationException {
                    try {
                        int buffersize = 100 * 1024;
                        byte[] buffer = new byte[buffersize];

                        int read = 0;
                        while ((read = is.read(buffer)) > 0) {
                            out.write(buffer, 0, read);
                        }
                        is.close();

                    } catch (Exception ex) {
                        throw new WebApplicationException(ex);
                    }
                }

            };
            
            return Response.ok(stream).header("content-disposition", "attachment; filename = " + path).build();
        } else {
            throw new Exception("invalid parameter");
        }
    }
    
    @Override
    public InputStream getDataChunk(String name, String path, long offset, int len) throws Exception {
        DataExportManager dem = getDataExportManager();
        DataExportInfo info = dem.getDataExportInfo(name);
        DataExportEntry entry = info.getExportEntry(path);

        return ChunkReaderFactory.getChunkReader(entry.getResourcePath(), offset, len);
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
