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
package edu.arizona.cs.stargate.userinterface;

import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class UserInterfaceManager {

    private static final Log LOG = LogFactory.getLog(UserInterfaceManager.class);
    
    private static UserInterfaceManager instance;

    private List<AUserInterfaceDriver> driver = new ArrayList<AUserInterfaceDriver>();
    
    public static UserInterfaceManager getInstance(Collection<AUserInterfaceDriver> driver) {
        synchronized (UserInterfaceManager.class) {
            if(instance == null) {
                instance = new UserInterfaceManager(driver);
            }
            return instance;
        }
    }
    
    public static UserInterfaceManager getInstance() throws ServiceNotStartedException {
        synchronized (UserInterfaceManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("UserInterfaceManager is not started");
            }
            return instance;
        }
    }
    
    UserInterfaceManager(Collection<AUserInterfaceDriver> driver) {
        if(driver == null || driver.isEmpty()) {
            throw new IllegalArgumentException("driver is null or empty");
        }
        
        this.driver.addAll(driver);
    }
    
    public Collection<AUserInterfaceDriver> getDriver() {
        return Collections.unmodifiableCollection(this.driver);
    }
    
    public AUserInterfaceDriver getDriver(String driverName) {
        if(driverName == null || driverName.isEmpty()) {
            throw new IllegalArgumentException("driverName is null or empty");
        }
        
        for(AUserInterfaceDriver driver : this.driver) {
            if(driverName.equals(driver.getDriverName())) {
                return driver;
            }
        }
        return null;
    }
    
    public synchronized void start() throws IOException {
        for(AUserInterfaceDriver driver : this.driver) {
            driver.startDriver();
        }
    }

    public synchronized void stop() throws IOException {
        for(AUserInterfaceDriver driver : this.driver) {
            driver.stopDriver();
        }
    }
    
    @Override
    public synchronized String toString() {
        return "UserInterfaceManager";
    }
}
