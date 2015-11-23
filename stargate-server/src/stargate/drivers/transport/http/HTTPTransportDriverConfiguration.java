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

package stargate.drivers.transport.http;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.transport.ATransportDriverConfiguration;

/**
 *
 * @author iychoi
 */
public class HTTPTransportDriverConfiguration extends ATransportDriverConfiguration {
    
    private static final Log LOG = LogFactory.getLog(HTTPTransportDriverConfiguration.class);
    
    private static final String HADOOP_CONFIG_KEY = HTTPTransportDriverConfiguration.class.getCanonicalName();
    
    public static final String DEFAULT_SERVICE_HOSTNAME_PATTERN = ".*";
    public static final int DEFAULT_SERVICE_PORT = 31010;
    public static final int DEFAULT_THREAD_POOL_SIZE = 10;
    
    private String serviceHostNamePattern = DEFAULT_SERVICE_HOSTNAME_PATTERN;
    private int servicePort = DEFAULT_SERVICE_PORT;
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    
    public static HTTPTransportDriverConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (HTTPTransportDriverConfiguration) serializer.fromJsonFile(file, HTTPTransportDriverConfiguration.class);
    }
    
    public static HTTPTransportDriverConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (HTTPTransportDriverConfiguration) serializer.fromJson(json, HTTPTransportDriverConfiguration.class);
    }
    
    public HTTPTransportDriverConfiguration() {
        this.servicePort = DEFAULT_SERVICE_PORT;
        this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    }
    
    @JsonProperty("service_host_name_pattern")
    public void setServiceHostNamePattern(String serviceHostNamePattern) {
        if(serviceHostNamePattern == null || serviceHostNamePattern.isEmpty()) {
            throw new IllegalArgumentException("serviceHostNamePattern is null or empty");
        }
        
        super.verifyMutable();
        
        this.serviceHostNamePattern = serviceHostNamePattern;
    }
    
    @JsonProperty("service_host_name_pattern")
    public String getServiceHostNamePattern() {
        return this.serviceHostNamePattern;
    }
    
    @JsonProperty("service_port")
    public void setServicePort(int port) {
        if(port <= 0) {
            throw new IllegalArgumentException("port is invalid");
        }
        
        super.verifyMutable();
        
        this.servicePort = port;
    }
    
    @JsonProperty("service_port")
    public int getServicePort() {
        return this.servicePort;
    }
    
    @JsonProperty("thread_pool_size")
    public void setThreadPoolSize(int size) {
        if(size <= 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        super.verifyMutable();
        
        this.threadPoolSize = size;
    }
    
    @JsonProperty("thread_pool_size")
    public int getThreadPoolSize() {
        return this.threadPoolSize;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
    }
    
    @JsonIgnore
    public synchronized String toJson() throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(this);
    }
    
    @JsonIgnore
    public synchronized void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(file, this);
    }
}
