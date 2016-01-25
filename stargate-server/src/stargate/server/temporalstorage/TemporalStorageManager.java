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
package stargate.server.temporalstorage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.temporalstorage.APersistentTemporalStorageDriver;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.temporalstorage.TemporalFileMetadata;

/**
 *
 * @author iychoi
 */
public class TemporalStorageManager {
    
    private static final Log LOG = LogFactory.getLog(TemporalStorageManager.class);
    
    private static TemporalStorageManager instance;

    private APersistentTemporalStorageDriver driver;
    
    public static TemporalStorageManager getInstance(APersistentTemporalStorageDriver driver) {
        synchronized (TemporalStorageManager.class) {
            if(instance == null) {
                instance = new TemporalStorageManager(driver);
            }
            return instance;
        }
    }
    
    public static TemporalStorageManager getInstance() throws ServiceNotStartedException {
        synchronized (TemporalStorageManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("ClusterCacheManager is not started");
            }
            return instance;
        }
    }
    
    TemporalStorageManager(APersistentTemporalStorageDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        this.driver = driver;
    }
    
    public APersistentTemporalStorageDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        this.driver.startDriver();
    }

    public synchronized void stop() throws IOException {
        this.driver.stopDriver();
    }
    
    public synchronized TemporalFileMetadata getMetadata(URI path) throws IOException, FileNotFoundException {
        return this.driver.getMetadata(path);
    }
    
    public synchronized boolean exists(URI path) throws IOException {
        return this.driver.exists(path);
    }
    public synchronized boolean isDirectory(URI path) throws IOException, FileNotFoundException {
        return this.driver.isDirectory(path);
    }
    
    public synchronized boolean isFile(URI path) throws IOException, FileNotFoundException {
        return this.driver.isFile(path);
    }
    
    public synchronized Collection<URI> listDirectory(URI path) throws IOException, FileNotFoundException {
        return this.driver.listDirectory(path);
    }
    
    public synchronized Collection<TemporalFileMetadata> listDirectoryWithMetadata(URI path) throws IOException, FileNotFoundException {
        return this.driver.listDirectoryWithMetadata(path);
    }
    
    public synchronized boolean makeDirs(URI path) throws IOException {
        return this.driver.makeDirs(path);
    }
    
    public synchronized boolean remove(URI path) throws IOException, FileNotFoundException {
        return this.driver.remove(path);
    }
    
    public synchronized boolean removeDir(URI path, boolean recursive) throws IOException, FileNotFoundException {
        return this.driver.removeDir(path, recursive);
    }
    
    public synchronized InputStream getInputStream(URI path) throws IOException, FileNotFoundException {
        return this.driver.getInputStream(path);
    }
    
    public synchronized OutputStream getOutputStream(URI path) throws IOException {
        return this.driver.getOutputStream(path);
    }
    
    @Override
    public synchronized String toString() {
        return "ClusterCacheManager";
    }
}
