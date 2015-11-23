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

package stargate.server.schedule;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.schedule.AScheduleDriver;
import stargate.commons.schedule.AScheduledLeaderTask;
import stargate.commons.schedule.AScheduledTask;
import stargate.commons.service.ServiceNotStartedException;

/**
 *
 * @author iychoi
 */
public class ScheduleManager {

    private static final Log LOG = LogFactory.getLog(ScheduleManager.class);
    
    private static ScheduleManager instance;
    
    private AScheduleDriver driver;

    public static ScheduleManager getInstance(AScheduleDriver driver) {
        synchronized (ScheduleManager.class) {
            if(instance == null) {
                instance = new ScheduleManager(driver);
            }
            return instance;
        }
    }
    
    public static ScheduleManager getInstance() throws ServiceNotStartedException {
        synchronized (ScheduleManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("ScheduleManager is not started");
            }
            return instance;
        }
    }
    
    ScheduleManager(AScheduleDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        this.driver = driver;
    }

    public AScheduleDriver getDriver() {
        return this.driver;
    }
    
    public synchronized void start() throws IOException {
        this.driver.startDriver();
    }

    public synchronized void stop() throws IOException {
        this.driver.stopDriver();
    }
    
    public void setScheduledTask(AScheduledLeaderTask task) {
        this.driver.setScheduledLeaderTask(task);
    }

    public void setScheduledTask(AScheduledTask task) {
        this.driver.setScheduledTask(task);
    }
    
    @Override
    public synchronized String toString() {
        return "ScheduleManager";
    }
}
