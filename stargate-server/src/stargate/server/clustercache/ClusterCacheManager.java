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
package stargate.server.clustercache;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.clustercache.AClusterCacheDriver;
import stargate.commons.service.ServiceNotStartedException;

/**
 *
 * @author iychoi
 */
public class ClusterCacheManager {
    
    private static final Log LOG = LogFactory.getLog(ClusterCacheManager.class);
    
    private static ClusterCacheManager instance;

    private AClusterCacheDriver driver;
    
    public static ClusterCacheManager getInstance(AClusterCacheDriver driver) {
        synchronized (ClusterCacheManager.class) {
            if(instance == null) {
                instance = new ClusterCacheManager(driver);
            }
            return instance;
        }
    }
    
    public static ClusterCacheManager getInstance() throws ServiceNotStartedException {
        synchronized (ClusterCacheManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("ClusterCacheManager is not started");
            }
            return instance;
        }
    }
    
    ClusterCacheManager(AClusterCacheDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        this.driver = driver;
    }
    
    public AClusterCacheDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        this.driver.startDriver();
    }

    public synchronized void stop() throws IOException {
        this.driver.stopDriver();
    }
    
    public boolean hasDataChunkCache(String hash) {
        return this.driver.hasDataChunkCache(hash);
    }
    
    public InputStream readDataChunkCache(String hash) throws IOException {
        return this.driver.readDataChunkCache(hash);
    }
    
    public void writeDataChunkCache(String hash, byte[] data) throws IOException {
        this.driver.writeDataChunkCache(hash, data);
    }
    
    public void expellDataChunkCache(String hash) throws IOException {
        this.driver.expellDataChunkCache(hash);
    }
    
    public void expellDataChunkCache() throws IOException {
        this.driver.expellDataChunkCache();
    }
    
    @Override
    public synchronized String toString() {
        return "ClusterCacheManager";
    }
}
