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
package stargate.commons.drivers;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.AImmutableConfiguration;
import stargate.commons.common.JsonSerializer;
import stargate.commons.utils.ClassUtils;

/**
 *
 * @author iychoi
 */
public class DriverSetting extends AImmutableConfiguration {
    
    private static final Log LOG = LogFactory.getLog(DriverSetting.class);
    
    private Class driverClass;
    private ADriverConfiguration driverConfiguration;
    
    public static DriverSetting createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (DriverSetting) serializer.fromJsonFile(file, DriverSetting.class);
    }
    
    public static DriverSetting createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (DriverSetting) serializer.fromJson(json, DriverSetting.class);
    }
    
    public DriverSetting() {
    }
    
    @JsonProperty("driver_class")
    public void setDriverClass(String clazz) throws ClassNotFoundException {
        if(clazz == null || clazz.isEmpty()) {
            throw new IllegalArgumentException("clazz is null or empty");
        }
        
        super.verifyMutable();
        
        this.driverClass = ClassUtils.findClass(clazz);
    }
    
    @JsonIgnore
    public void setDriverClass(Class clazz) {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is null");
        }
        
        super.verifyMutable();
        
        this.driverClass = clazz;
    }
    
    @JsonProperty("driver_class")
    public String getDriverClassString() {
        return this.driverClass.getCanonicalName();
    }
    
    @JsonIgnore
    public Class getDriverClass() {
        return this.driverClass;
    }
    
    @JsonProperty("driver_configuration")
    public ADriverConfiguration getDriverConfiguration() {
        return this.driverConfiguration;
    }
    
    @JsonProperty("driver_configuration")
    public void setDriverConfiguration(ADriverConfiguration driverConfiguration) {
        if(driverConfiguration == null) {
            throw new IllegalArgumentException("driverConfiguration is null");
        }
        
        super.verifyMutable();
        
        this.driverConfiguration = driverConfiguration;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
        
        this.driverConfiguration.setImmutable();
    }
}
