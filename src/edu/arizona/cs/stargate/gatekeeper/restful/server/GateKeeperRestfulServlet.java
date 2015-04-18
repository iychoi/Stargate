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
import edu.arizona.cs.stargate.gatekeeper.restful.RestfulResponse;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AGateKeeperRestfulAPI;
import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 *
 * @author iychoi
 */
@Path(AGateKeeperRestfulAPI.BASE_PATH)
@Singleton
public class GateKeeperRestfulServlet extends AGateKeeperRestfulAPI {

    @GET
    @Path(AGateKeeperRestfulAPI.LIVENESS_PATH)
    @Produces(MediaType.TEXT_PLAIN)
    public String responseCheckLiveText() {
        try {
            return DataFormatUtils.toJSONFormat(responseCheckLiveJSON());
        } catch (IOException ex) {
            return "DataFormatter formatting error";
        }
    }
    
    @GET
    @Path(AGateKeeperRestfulAPI.LIVENESS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public RestfulResponse<Boolean> responseCheckLiveJSON() {
        return new RestfulResponse<Boolean>(checkLive());
    }
    
    @Override
    public boolean checkLive() {
        return true;
    }
}
