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
import edu.arizona.cs.stargate.transport.http.GateKeeperRestfulClient;
import edu.arizona.cs.stargate.common.WebParamBuilder;
import com.sun.jersey.api.client.GenericType;
import edu.arizona.cs.stargate.common.utils.PathUtils;
import edu.arizona.cs.stargate.dataexport.DataExportEntry;
import edu.arizona.cs.stargate.gatekeeper.restful.api.ADataExportManagerRestfulAPI;
import edu.arizona.cs.stargate.transport.http.RestfulResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
*/
/**
 *
 * @author iychoi
 */
public class DataExportManagerClient {
    /*    
    private static final Log LOG = LogFactory.getLog(DataExportManagerClient.class);
    
    private GateKeeperRestfulClient gatekeeperRestfulClient;

    public DataExportManagerClient(GateKeeperRestfulClient gatekeeperRestfulClient) {
        this.gatekeeperRestfulClient = gatekeeperRestfulClient;
    }
    
    public String getResourcePath(String path) {
        return PathUtils.concatPath(ADataExportManagerRestfulAPI.BASE_PATH, path);
    }
    
    @Override
    public Collection<DataExportEntry> getAllDataExports() throws Exception {
        RestfulResponse<Collection<DataExportEntry>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH));
            builder.addParam("vpath", "*");
            String url = builder.build();
            response = (RestfulResponse<Collection<DataExportEntry>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<DataExportEntry>>>(){});
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
    public DataExportEntry getDataExport(String vpath) throws Exception {
        RestfulResponse<Collection<DataExportEntry>> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH));
            builder.addParam("vpath", vpath);
            String url = builder.build();
            response = (RestfulResponse<Collection<DataExportEntry>>) this.gatekeeperRestfulClient.get(url, new GenericType<RestfulResponse<Collection<DataExportEntry>>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        } else {
            Collection<DataExportEntry> exports = response.getResponse();
            Iterator<DataExportEntry> iterator = exports.iterator();
            if(iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }
    }
    
    @Override
    public void addDataExport(DataExportEntry export) throws Exception {
        RestfulResponse<Boolean> response;
        try {
            String url = getResourcePath(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH);
            response = (RestfulResponse<Boolean>) this.gatekeeperRestfulClient.post(url, export, new GenericType<RestfulResponse<Boolean>>(){});
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
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH));
            builder.addParam("vpath", vpath);
            String url = builder.build();
            response = (RestfulResponse<Boolean>) this.gatekeeperRestfulClient.delete(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }

    @Override
    public void removeAllDataExports() throws Exception {
        RestfulResponse<Boolean> response;
        try {
            WebParamBuilder builder = new WebParamBuilder(getResourcePath(ADataExportManagerRestfulAPI.DATA_EXPORT_PATH));
            builder.addParam("vpath", "*");
            String url = builder.build();
            response = (RestfulResponse<Boolean>) this.gatekeeperRestfulClient.delete(url, new GenericType<RestfulResponse<Boolean>>(){});
        } catch (IOException ex) {
            LOG.error(ex);
            throw ex;
        }
        
        if(response.getException() != null) {
            throw response.getException();
        }
    }
    */
}
