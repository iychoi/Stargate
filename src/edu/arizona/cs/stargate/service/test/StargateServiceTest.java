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

package edu.arizona.cs.stargate.service.test;

import edu.arizona.cs.stargate.common.LocalResourceLocator;
import edu.arizona.cs.stargate.cluster.Node;
import edu.arizona.cs.stargate.cluster.LocalClusterManager;
import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.datastore.DataStoreConfiguration;
import edu.arizona.cs.stargate.drivers.hazelcast.HazelcastCoreDriver;
import edu.arizona.cs.stargate.drivers.hazelcast.HazelcastCoreDriverConfiguration;
import edu.arizona.cs.stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriver;
import edu.arizona.cs.stargate.drivers.hazelcast.datastore.HazelcastDataStoreDriverConfiguration;
import edu.arizona.cs.stargate.drivers.hazelcast.schedule.HazelcastScheduleDriver;
import edu.arizona.cs.stargate.drivers.hazelcast.schedule.HazelcastScheduleDriverConfiguration;
import edu.arizona.cs.stargate.policy.Policy;
import edu.arizona.cs.stargate.recipe.RecipeGeneratorConfiguration;
import edu.arizona.cs.stargate.drivers.recipe.sha1fixed.SHA1FixedChunkRecipeGeneratorDriver;
import edu.arizona.cs.stargate.drivers.recipe.sha1fixed.SHA1FixedChunkRecipeGeneratorDriverConfiguration;
import edu.arizona.cs.stargate.schedule.ScheduleConfiguration;
import edu.arizona.cs.stargate.service.StargateService;
import edu.arizona.cs.stargate.service.StargateServiceConfiguration;
import edu.arizona.cs.stargate.sourcefs.SourceFileSystemConfiguration;
import edu.arizona.cs.stargate.drivers.sourcefs.hdfs.HDFSSourceFileSystemDriver;
import edu.arizona.cs.stargate.drivers.sourcefs.hdfs.HDFSSourceFileSystemDriverConfiguration;
import edu.arizona.cs.stargate.transport.TransportConfiguration;
import edu.arizona.cs.stargate.drivers.transport.http.HTTPTransportDriver;
import edu.arizona.cs.stargate.drivers.transport.http.HTTPTransportDriverConfiguration;
import edu.arizona.cs.stargate.userinterface.UserInterfaceConfiguration;
import edu.arizona.cs.stargate.drivers.DriverSetting;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPUserInterfaceDriver;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfiguration;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public class StargateServiceTest {
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
        
        serviceConf.setPolicy(new Policy());
        
        TransportConfiguration transportConf = new TransportConfiguration();
        transportConf.setDriverSetting(makeHTTPTransportDriverSetting());
        serviceConf.setTransportConfiguration(transportConf);
        
        UserInterfaceConfiguration userInterfaceConf = new UserInterfaceConfiguration();
        userInterfaceConf.addDriverSetting(makeHTTPUserInterfaceDriverSetting());
        serviceConf.setUserInterfaceConfiguration(userInterfaceConf);
        
        JsonSerializer serializer = new JsonSerializer(true);
        serializer.toJsonFile(f, serviceConf);
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            LocalResourceLocator rl = new LocalResourceLocator();
            
            File configFile = null;
            if(args.length == 0) {
                configFile = rl.getResourceLocation("stargate.json");
            } else {
                configFile = rl.getResourceLocation(args[0]);
            }
            
            if (!configFile.exists()) {
                makeDummyServiceConf(configFile);
            }
            
            StargateServiceConfiguration conf = StargateServiceConfiguration.createInstance(configFile);
            
            StargateService instance = StargateService.getInstance(conf);
            instance.start();
            
            LocalClusterManager localClusterManager = instance.getClusterManager().getLocalClusterManager();
            Collection<Node> allNode = localClusterManager.getNode();
            System.out.println("Cluster Name : " + localClusterManager.getName());
            for(Node node : allNode) {
                System.out.println("Node : " + node.getName());
                System.out.println("Obj : " + node.toJson());
            }
            
            System.out.println("Stargate service is running");
            System.out.println("press ctrl + c for stopping the service");
            
            while(!Thread.currentThread().isInterrupted()) {
                try {
                    // service loop
                    Thread.sleep(1000);
                } catch(InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            
            System.out.println("Stargate service is stopping");
            
            instance.stop();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
