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
package stargate.server.policy;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datastore.ADistributedDataStore;
import stargate.commons.policy.APolicy;
import stargate.commons.policy.ClusterPolicy;
import stargate.commons.policy.VolumePolicy;
import stargate.commons.service.ServiceNotStartedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.server.datastore.DataStoreManager;

/**
 *
 * @author iychoi
 */
public class PolicyManager {
    
    private static final Log LOG = LogFactory.getLog(PolicyManager.class);
    
    private static final String POLICYMANAGER_MAP_ID = "PolicyManager_Policy";
    
    private static PolicyManager instance;
    
    private DataStoreManager datastoreManager;
    
    private ADistributedDataStore policy;
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
        
        this.policy = this.datastoreManager.getPersistentDistributedDataStore(POLICYMANAGER_MAP_ID, String.class);
    }
    
    public synchronized int getPolicyCount() {
        return this.policy.size();
    }
    
    public synchronized Object getPolicy(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is empty or null");
        }
        
        return this.policy.get(key);
    }
    
    public synchronized ClusterPolicy getClusterPolicy() throws IOException {
        ClusterPolicy cp = new ClusterPolicy();
        cp.readFrom(this.policy);
        return cp;
    }
    
    public synchronized VolumePolicy getVolumePolicy() throws IOException {
        VolumePolicy vp = new VolumePolicy();
        vp.readFrom(this.policy);
        return vp;
    }
    
    public synchronized void addPolicy(APolicy policy) throws IOException {
        if(policy == null) {
            throw new IllegalArgumentException("policy is null");
        }
        
        policy.addTo(this.policy);
        
        this.lastUpdateTime = DateTimeUtils.getCurrentTime();
    }
    
    public synchronized void addPolicy(Map<String, Object> map) throws IOException {
        if(map == null) {
            throw new IllegalArgumentException("map is null");
        }
        
        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            addPolicy(entry.getKey(), entry.getValue().toString());
        }
    }
    
    public synchronized void addPolicy(String key, Object value) throws IOException {
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
