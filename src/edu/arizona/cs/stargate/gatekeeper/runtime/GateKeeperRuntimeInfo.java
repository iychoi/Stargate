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

package edu.arizona.cs.stargate.gatekeeper.runtime;

import edu.arizona.cs.stargate.common.DataFormatUtils;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.common.TemporaryFileUtils;
import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class GateKeeperRuntimeInfo {
    
    public static final String RUNTIME_INFO_FILENAME = "gatekeeper_runtime.json";
    
    private int servicePort;
    
    public GateKeeperRuntimeInfo() {
    }
    
    @JsonProperty("service_port")
    public synchronized void setServicePort(int port) {
        this.servicePort = port;
    }
    
    @JsonProperty("service_port")
    public synchronized int getServicePort() {
        return this.servicePort;
    }
    
    public synchronized void load() throws IOException {
        File f = TemporaryFileUtils.getTempFile(RUNTIME_INFO_FILENAME);
        if(f.exists()) {
            JsonSerializer serializer = new JsonSerializer();
            GateKeeperRuntimeInfo other = (GateKeeperRuntimeInfo) serializer.fromJsonFile(f, GateKeeperRuntimeInfo.class);
            
            this.servicePort = other.servicePort;
        } else {
            throw new IOException("GateKeeperRuntimeInfo file not exists : " + RUNTIME_INFO_FILENAME);
        }
    }
    
    public synchronized void removeFile() throws IOException {
         File f = TemporaryFileUtils.getTempFile(RUNTIME_INFO_FILENAME);
         if(f.exists()) {
             if(!f.delete()) {
                 throw new IOException("Removing a runtime info file is failed : " + f.toString());
             }
         }
    }
    
    public synchronized void synchronize() throws IOException {
        File f = TemporaryFileUtils.getTempFile(RUNTIME_INFO_FILENAME);
        DataFormatUtils.toJSONFile(f, this);
    }
}
