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
package stargate.server.recipe;

import java.io.File;
import java.io.IOException;
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
public class RecipeGeneratorConfiguration extends AImmutableConfiguration {
    
    private static final Log LOG = LogFactory.getLog(RecipeGeneratorConfiguration.class);
    
    private static final String HADOOP_CONFIG_KEY = RecipeGeneratorConfiguration.class.getCanonicalName();
    
    private DriverSetting driverSetting;
    
    public static RecipeGeneratorConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (RecipeGeneratorConfiguration) serializer.fromJsonFile(file, RecipeGeneratorConfiguration.class);
    }
    
    public static RecipeGeneratorConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (RecipeGeneratorConfiguration) serializer.fromJson(json, RecipeGeneratorConfiguration.class);
    }
    
    public RecipeGeneratorConfiguration() {
    }
    
    @JsonProperty("driver_setting")
    public void setDriverSetting(DriverSetting setting) {
        if(setting == null) {
            throw new IllegalArgumentException("setting is null");
        }
        
        super.verifyMutable();
        
        this.driverSetting = setting;
    }
    
    @JsonProperty("driver_setting")
    public DriverSetting getDriverSetting() {
        return this.driverSetting;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
        
        this.driverSetting.setImmutable();
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
