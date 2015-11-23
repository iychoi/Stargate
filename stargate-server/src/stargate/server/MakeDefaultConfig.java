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
package stargate.server;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import stargate.commons.common.JsonSerializer;
import stargate.commons.common.LocalResourceLocator;
import stargate.commons.dataexport.DataExportEntry;
import stargate.commons.drivers.DriverSetting;
import stargate.drivers.hazelcast.HazelcastCoreDriver;
import stargate.drivers.hazelcast.HazelcastCoreDriverConfiguration;
import stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriver;
import stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriverConfiguration;
import stargate.drivers.hazelcast.schedule.HazelcastScheduleDriver;
import stargate.drivers.hazelcast.schedule.HazelcastScheduleDriverConfiguration;
import stargate.drivers.recipe.sha1fixed.SHA1FixedChunkRecipeGeneratorDriver;
import stargate.drivers.recipe.sha1fixed.SHA1FixedChunkRecipeGeneratorDriverConfiguration;
import stargate.drivers.sourcefs.hdfs.HDFSSourceFileSystemDriver;
import stargate.drivers.sourcefs.hdfs.HDFSSourceFileSystemDriverConfiguration;
import stargate.drivers.transport.http.HTTPTransportDriver;
import stargate.drivers.transport.http.HTTPTransportDriverConfiguration;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriver;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfiguration;
import stargate.server.datastore.DataStoreConfiguration;
import stargate.server.recipe.RecipeGeneratorConfiguration;
import stargate.server.schedule.ScheduleConfiguration;
import stargate.server.service.StargateServiceConfiguration;
import stargate.server.sourcefs.SourceFileSystemConfiguration;
import stargate.server.transport.TransportConfiguration;
import stargate.server.userinterface.UserInterfaceConfiguration;

/**
 *
 * @author iychoi
 */
public class MakeDefaultConfig {

    public static DriverSetting makeHazelcastCoreDriverSetting() {
        HazelcastCoreDriverConfiguration hazelcastConfig = new HazelcastCoreDriverConfiguration();
        hazelcastConfig.setMyHostAddr("localhost");
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HazelcastCoreDriver.class);
        driverSetting.setDriverConfiguration(hazelcastConfig);
        return driverSetting;
    }
    
    public static DriverSetting makeHazelcastDataStoreDriverSetting() {
        HazelcastDataStoreDriverConfiguration hazelcastDataStoreConfig = new HazelcastDataStoreDriverConfiguration();
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HazelcastDataStoreDriver.class);
        driverSetting.setDriverConfiguration(hazelcastDataStoreConfig);
        return driverSetting;
    }
    
    public static DriverSetting makeHDFSSourceFSDriverSetting() {
        HDFSSourceFileSystemDriverConfiguration hdfsConfig = new HDFSSourceFileSystemDriverConfiguration();
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HDFSSourceFileSystemDriver.class);
        driverSetting.setDriverConfiguration(hdfsConfig);
        return driverSetting;
    }
    
    public static DriverSetting makeSHA1FixedRecipeGeneratorDriverSetting() {
        SHA1FixedChunkRecipeGeneratorDriverConfiguration sha1Config = new SHA1FixedChunkRecipeGeneratorDriverConfiguration();
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(SHA1FixedChunkRecipeGeneratorDriver.class);
        driverSetting.setDriverConfiguration(sha1Config);
        return driverSetting;
    }
    
    public static DriverSetting makeHazelcastScheduleDriverSetting() {
        HazelcastScheduleDriverConfiguration hazelcastScheduleConfig = new HazelcastScheduleDriverConfiguration();
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HazelcastScheduleDriver.class);
        driverSetting.setDriverConfiguration(hazelcastScheduleConfig);
        return driverSetting;
    }
    
    public static DriverSetting makeHTTPTransportDriverSetting() {
        HTTPTransportDriverConfiguration httpTransportConfig = new HTTPTransportDriverConfiguration();
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HTTPTransportDriver.class);
        driverSetting.setDriverConfiguration(httpTransportConfig);
        return driverSetting;
    }
    
    public static DriverSetting makeHTTPUserInterfaceDriverSetting() {
        HTTPUserInterfaceDriverConfiguration httpUserInterfaceConf = new HTTPUserInterfaceDriverConfiguration();
        
        DriverSetting driverSetting = new DriverSetting();
        driverSetting.setDriverClass(HTTPUserInterfaceDriver.class);
        driverSetting.setDriverConfiguration(httpUserInterfaceConf);
        return driverSetting;
    }
    
    public static void makeDummyServiceConf(File f) throws IOException, URISyntaxException {
        StargateServiceConfiguration serviceConf = new StargateServiceConfiguration();

        serviceConf.setServiceName("StargateTestService");
        
        serviceConf.addDaemonDriverSetting(makeHazelcastCoreDriverSetting());

        DataStoreConfiguration datastoreConf = new DataStoreConfiguration();
        datastoreConf.setDriverSetting(makeHazelcastDataStoreDriverSetting());
        serviceConf.setDataStoreConfiguration(datastoreConf);
        
        SourceFileSystemConfiguration sourcefsConf = new SourceFileSystemConfiguration();
        sourcefsConf.setDriverSetting(makeHDFSSourceFSDriverSetting());
        serviceConf.setSourceFileSystemConfiguration(sourcefsConf);
        
        RecipeGeneratorConfiguration recipeConf = new RecipeGeneratorConfiguration();
        recipeConf.setDriverSetting(makeSHA1FixedRecipeGeneratorDriverSetting());
        serviceConf.setRecipeGeneratorConfiguration(recipeConf);
        
        ScheduleConfiguration scheduleConf = new ScheduleConfiguration();
        scheduleConf.setDriverSetting(makeHazelcastScheduleDriverSetting());
        serviceConf.setScheduleConfiguration(scheduleConf);
        
        TransportConfiguration transportConf = new TransportConfiguration();
        transportConf.setDriverSetting(makeHTTPTransportDriverSetting());
        serviceConf.setTransportConfiguration(transportConf);
        
        UserInterfaceConfiguration userInterfaceConf = new UserInterfaceConfiguration();
        userInterfaceConf.addDriverSetting(makeHTTPUserInterfaceDriverSetting());
        serviceConf.setUserInterfaceConfiguration(userInterfaceConf);
        
        // data export
        DataExportEntry dee = new DataExportEntry("test/CP000828.ffn.gz", "/");
        serviceConf.addDataExport(dee);
        
        JsonSerializer serializer = new JsonSerializer(true);
        serializer.toJsonFile(f, serviceConf);
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            LocalResourceLocator rl = new LocalResourceLocator();
            
            File configFile = rl.getResourceLocation("stargate.json");
            
            makeDummyServiceConf(configFile);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
