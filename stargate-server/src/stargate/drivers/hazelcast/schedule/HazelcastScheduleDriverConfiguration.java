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

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.common.JsonSerializer;
import stargate.commons.schedule.AScheduleDriverConfiguration;

/**
 *
 * @author iychoi
 */
public class HazelcastScheduleDriverConfiguration extends AScheduleDriverConfiguration {
    
    private static final Log LOG = LogFactory.getLog(HazelcastScheduleDriverConfiguration.class);
    
    private static final int DEFAULT_SCHEDULE_TASK_THREADS = 4;
    
    private int taskThreads = DEFAULT_SCHEDULE_TASK_THREADS;
    
    public static HazelcastScheduleDriverConfiguration createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (HazelcastScheduleDriverConfiguration) serializer.fromJsonFile(file, HazelcastScheduleDriverConfiguration.class);
    }
    
    public static HazelcastScheduleDriverConfiguration createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is empty or null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (HazelcastScheduleDriverConfiguration) serializer.fromJson(json, HazelcastScheduleDriverConfiguration.class);
    }
    
    public HazelcastScheduleDriverConfiguration() {
        this.taskThreads = DEFAULT_SCHEDULE_TASK_THREADS;
    }
    
    @JsonProperty("task_threads")
    public void setTaskThreads(int threads) {
        if(threads <= 0) {
            throw new IllegalArgumentException("threads is invalid");
        }
        
        super.verifyMutable();
        
        this.taskThreads = threads;
    }
    
    @JsonProperty("task_threads")
    public int getTaskThreads() {
        return this.taskThreads;
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
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
