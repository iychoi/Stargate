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
package stargate.server.userinterface;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.AImmutableConfiguration;
import stargate.commons.common.JsonSerializer;
import stargate.commons.drivers.DriverSetting;

/**
 *
 * @author iychoi
 */
public class UserInterfaceConfiguration extends AImmutableConfiguration {
    
    private static final Log LOG = LogFactory.getLog(UserInterfaceConfiguration.class);
    
    private List<DriverSetting> driverSetting = new ArrayList<DriverSetting>();
    
    public static UserInterfaceConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (UserInterfaceConfiguration) serializer.fromJsonFile(file, UserInterfaceConfiguration.class);
    }
    
    public static UserInterfaceConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (UserInterfaceConfiguration) serializer.fromJson(json, UserInterfaceConfiguration.class);
    }
    
    public UserInterfaceConfiguration() {
    }
    
    @JsonProperty("driver_setting")
    public void addDriverSetting(Collection<DriverSetting> setting) {
        if(setting == null || setting.isEmpty()) {
            throw new IllegalArgumentException("setting is null or empty");
        }
        
        super.verifyMutable();
        
        this.driverSetting.addAll(setting);
    }
    
    @JsonIgnore
    public void addDriverSetting(DriverSetting setting) {
        if(setting == null) {
            throw new IllegalArgumentException("setting is null");
        }
        
        super.verifyMutable();
        
        this.driverSetting.add(setting);
    }
    
    @JsonProperty("driver_setting")
    public Collection<DriverSetting> getDriverSetting() {
        return Collections.unmodifiableCollection(this.driverSetting);
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
        
        for(DriverSetting setting : this.driverSetting) {
            setting.setImmutable();
        }
    }
}
