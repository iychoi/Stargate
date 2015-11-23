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
package stargate.drivers.hazelcast.schedule;

import stargate.drivers.hazelcast.HazelcastCoreDriver;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.drivers.DriverNotInstantiatedException;
import stargate.commons.schedule.AScheduleDriver;
import stargate.commons.schedule.AScheduleDriverConfiguration;
import stargate.commons.schedule.AScheduledLeaderTask;
import stargate.commons.schedule.AScheduledTask;
import stargate.commons.schedule.ScheduledLeaderTaskWrapper;

/**
 *
 * @author iychoi
 */
public class HazelcastScheduleDriver extends AScheduleDriver {

    private static final Log LOG = LogFactory.getLog(HazelcastScheduleDriver.class);
    
    private HazelcastScheduleDriverConfiguration config;
    private HazelcastCoreDriver driverGroup;
    private int taskThreads;
    private ScheduledExecutorService threadPool;
    
    public HazelcastScheduleDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HazelcastScheduleDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HazelcastScheduleDriverConfiguration");
        }
        
        this.config = (HazelcastScheduleDriverConfiguration) config;
    }
    
    public HazelcastScheduleDriver(AScheduleDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof HazelcastScheduleDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of HazelcastScheduleDriverConfiguration");
        }
        
        this.config = (HazelcastScheduleDriverConfiguration) config;
    }
    
    public HazelcastScheduleDriver(HazelcastScheduleDriverConfiguration config) {
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
        
        this.taskThreads = this.config.getTaskThreads();
        this.threadPool = Executors.newScheduledThreadPool(this.taskThreads);
    }

    @Override
    public synchronized void stopDriver() throws IOException {
        this.threadPool.shutdown();
    }
    
    public synchronized int getTaskThreads() {
        return this.taskThreads;
    }

    @Override
    public String getDriverName() {
        return "HazelcastScheduleDriver";
    }

    @Override
    public synchronized void setScheduledLeaderTask(AScheduledLeaderTask task) {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        ScheduledLeaderTaskWrapper wrapper = new ScheduledLeaderTaskWrapper(task, this);
        
        if(wrapper.isRepeatedTask()) {
            this.threadPool.scheduleWithFixedDelay(wrapper, wrapper.getDelay(), wrapper.getInterval(), TimeUnit.SECONDS);
        } else {
            this.threadPool.schedule(wrapper, wrapper.getDelay(), TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void setScheduledTask(AScheduledTask task) {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        if(task.isRepeatedTask()) {
            this.threadPool.scheduleWithFixedDelay(task, task.getDelay(), task.getInterval(), TimeUnit.SECONDS);
        } else {
            this.threadPool.schedule(task, task.getDelay(), TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized boolean isLeader() {
        return this.driverGroup.isLeader();
    }
}
