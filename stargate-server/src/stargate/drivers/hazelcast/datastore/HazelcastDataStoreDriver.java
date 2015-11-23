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
package stargate.drivers.hazelcast.datastore;

import stargate.drivers.hazelcast.HazelcastCoreDriver;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datastore.ADataStoreDriver;
import stargate.commons.datastore.ADataStoreDriverConfiguration;
import stargate.commons.datastore.ADistributedDataStore;
import stargate.commons.datastore.AReplicatedDataStore;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.drivers.DriverNotInstantiatedException;

/**
 *
 * @author iychoi
 */
public class HazelcastDataStoreDriver extends ADataStoreDriver {

    private static final Log LOG = LogFactory.getLog(HazelcastDataStoreDriver.class);
    
    private HazelcastDataStoreDriverConfiguration config;
    private HazelcastCoreDriver driverGroup;
    
    public HazelcastDataStoreDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HazelcastDataStoreDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HazelcastDataStoreDriverConfiguration");
        }
        
        this.config = (HazelcastDataStoreDriverConfiguration) config;
    }
    
    public HazelcastDataStoreDriver(ADataStoreDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HazelcastDataStoreDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HazelcastDataStoreDriverConfiguration");
        }
        
        this.config = (HazelcastDataStoreDriverConfiguration) config;
    }
    
    public HazelcastDataStoreDriver(HazelcastDataStoreDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        try {
            this.driverGroup = HazelcastCoreDriver.getInstance();
        } catch (DriverNotInstantiatedException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public synchronized void stopDriver() throws IOException {
        this.driverGroup = null;
    }
    
    @Override
    public String getDriverName() {
        return "HazelcastDataStoreDriver";
    }

    @Override
    public synchronized ADistributedDataStore getDistributedDataStore(String name, Class keyclass, Class valclass) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(keyclass == null) {
            throw new IllegalArgumentException("keyclass is null");
        }
        
        if(valclass == null) {
            throw new IllegalArgumentException("valclass is null");
        }
        
        return new HazelcastDistributedDataStore(this.driverGroup.getMap(name), keyclass, valclass);
    }

    @Override
    public synchronized AReplicatedDataStore getReplicatedDataStore(String name, Class keyclass, Class valclass) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(keyclass == null) {
            throw new IllegalArgumentException("keyclass is null");
        }
        
        if(valclass == null) {
            throw new IllegalArgumentException("valclass is null");
        }
        
        return new HazelcastReplicatedDataStore(this.driverGroup.getReplicatedMap(name), keyclass, valclass);
    }
}
