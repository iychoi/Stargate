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
package edu.arizona.cs.stargate.policy;

import edu.arizona.cs.stargate.common.utils.DateTimeUtils;
import edu.arizona.cs.stargate.datastore.AReplicatedDataStore;
import edu.arizona.cs.stargate.datastore.DataStoreManager;
import edu.arizona.cs.stargate.service.ServiceNotStartedException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class PolicyManager {
    
    private static final Log LOG = LogFactory.getLog(PolicyManager.class);
    
    private static final String POLICYMANAGER_MAP_ID = "PolicyManager_Policy";
    
    private static PolicyManager instance;
    
    private DataStoreManager datastoreManager;
    
    private AReplicatedDataStore policy;
    protected long lastUpdateTime;
    
    public static PolicyManager getInstance(DataStoreManager datastoreManager) {
        synchronized (PolicyManager.class) {
            if(instance == null) {
                instance = new PolicyManager(datastoreManager);
            }
            return instance;
        }
    }
    
    public static PolicyManager getInstance() throws ServiceNotStartedException {
        synchronized (PolicyManager.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("PolicyManager is not started");
            }
            return instance;
        }
    }
    
    PolicyManager(DataStoreManager datastoreManager) {
        if(datastoreManager == null) {
            throw new IllegalArgumentException("datastoreManager is null");
        }
        
        this.datastoreManager = datastoreManager;
        
        this.policy = this.datastoreManager.getReplicatedDataStore(POLICYMANAGER_MAP_ID, String.class, String.class);
    }
    
    public synchronized int getPolicyCount() {
        return this.policy.size();
    }
    
    public synchronized Map<String, String> getPolicy() throws IOException {
        Map<String, String> entries = new HashMap<String, String>();
        Set<Object> keySet = this.policy.keySet();
        for(Object key : keySet) {
            String value = (String) this.policy.get(key);
            entries.put((String) key, value);
        }

        return Collections.unmodifiableMap(entries);
    }
    
    public synchronized String getPolicy(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty or null");
        }
        
        return (String) this.policy.get(key);
    }
    
    public synchronized void addPolicy(Policy policy) throws IOException {
        if(policy == null) {
            throw new IllegalArgumentException("policy is null");
        }
        
        policy.saveTo(this);
    }
    
    public synchronized void addPolicy(ClusterPolicy policy) throws IOException {
        if(policy == null) {
            throw new IllegalArgumentException("policy is null");
        }
        
        policy.saveTo(this);
    }
    
    public synchronized void addPolicy(Map<String, String> entry) throws IOException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        for(Map.Entry<String, String> e : entry.entrySet()) {
            addPolicy(e.getKey(), e.getValue());
        }
    }
    
    public synchronized void addPolicy(String key, String value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty or null");
        }

        this.policy.put(key, value);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    public synchronized void clearPolicy() throws IOException {
        this.policy.clear();
    }
    
    public synchronized void removePolicy(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty or null");
        }
        
        if(this.policy.containsKey(key)) {
            this.policy.remove(key);
            
            this.lastUpdateTime = DateTimeUtils.getCurrentTime();
        }
    }
    
    public synchronized boolean hasPolicy(String key) {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty or null");
        }
        
        return this.policy.containsKey(key);
    }
    
    public synchronized boolean isEmpty() {
        if(this.policy == null || this.policy.isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
    
    @Override
    public synchronized String toString() {
        return "PolicyManager";
    }
}
