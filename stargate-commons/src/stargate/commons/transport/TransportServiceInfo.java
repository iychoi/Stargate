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
package stargate.commons.transport;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.utils.ClassUtils;

/**
 *
 * @author iychoi
 */
public class TransportServiceInfo {
    private static final Log LOG = LogFactory.getLog(TransportServiceInfo.class);
    
    private String driverClass;
    private URI connectionUri;
    
    public static TransportServiceInfo createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (TransportServiceInfo) serializer.fromJsonFile(file, TransportServiceInfo.class);
    }
    
    public static TransportServiceInfo createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (TransportServiceInfo) serializer.fromJson(json, TransportServiceInfo.class);
    }
    
    public TransportServiceInfo() {
    }
    
    public TransportServiceInfo(TransportServiceInfo that) {
        this.driverClass = that.driverClass;
        this.connectionUri = that.connectionUri;
    }
    
    public TransportServiceInfo(String driverClass, URI connectionUri) {
        if(driverClass == null || driverClass.isEmpty()) {
            throw new IllegalArgumentException("driverClass is null or empty");
        }
        
        if(connectionUri == null) {
            throw new IllegalArgumentException("connectionUri is null");
        }
        
        initialize(driverClass, connectionUri);
    }
    
    private void initialize(String driverClass, URI connectionUri) {
        if(driverClass == null) {
            throw new IllegalArgumentException("driverClass is null");
        }
        
        if(connectionUri == null) {
            throw new IllegalArgumentException("connectionUri is null or empty");
        }
        
        this.driverClass = driverClass;
        this.connectionUri = connectionUri;
    }
    
    @JsonProperty("driver_class")
    public void setDriverClass(String clazz) {
        if(clazz == null || clazz.isEmpty()) {
            throw new IllegalArgumentException("clazz is null or empty");
        }
        
        this.driverClass = clazz;
    }
    
    @JsonIgnore
    public void setDriverClass(Class clazz) {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }
        
        this.driverClass = clazz.getCanonicalName();
    }
    
    @JsonProperty("driver_class")
    public String getDriverClassString() {
        return this.driverClass;
    }
    
    @JsonIgnore
    public Class getDriverClass() throws ClassNotFoundException {
        return ClassUtils.findClass(this.driverClass);
    }
    
    @JsonProperty("connection_uri")
    public URI getConnectionURI() {
        return this.connectionUri;
    }
    
    @JsonProperty("connection_uri")
    public void setConnectionURI(URI connectionUri) {
        this.connectionUri = connectionUri;
    }
    
    @JsonIgnore
    @Override
    public synchronized String toString() {
        return this.driverClass + "\t" + this.connectionUri.toASCIIString();
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
