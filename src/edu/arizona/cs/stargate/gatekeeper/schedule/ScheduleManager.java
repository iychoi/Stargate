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

package edu.arizona.cs.stargate.gatekeeper.schedule;

import edu.arizona.cs.stargate.common.ServiceNotStartedException;
import edu.arizona.cs.stargate.gatekeeper.distributed.DistributedService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ScheduleManager {

    private static final Log LOG = LogFactory.getLog(ScheduleManager.class);
    
    private static final int SCHEDULE_BACKGROUND_WORKER_THREADS = 4;
    
    private static ScheduleManager instance;

    private DistributedService distributedService;
    private ScheduledExecutorService taskWorker;
    
    public static ScheduleManager getInstance(DistributedService distributedService) {
        synchronized (ScheduleManager.class) {
            if(instance == null) {
                instance = new ScheduleManager(distributedService);
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
    
    ScheduleManager(DistributedService distributedService) {
        this.distributedService = distributedService;
        
        this.taskWorker = Executors.newScheduledThreadPool(SCHEDULE_BACKGROUND_WORKER_THREADS);
    }

    public synchronized void scheduleTask(AScheduledTask task) {
        if(task.isRepeatedTask()) {
            this.taskWorker.scheduleWithFixedDelay(task, task.getDelay(), task.getPeriod(), TimeUnit.SECONDS);
        } else {
            this.taskWorker.schedule(task, task.getDelay(), TimeUnit.SECONDS);
        }
    }
    
    public void stop() {
        this.taskWorker.shutdown();
    }
}
