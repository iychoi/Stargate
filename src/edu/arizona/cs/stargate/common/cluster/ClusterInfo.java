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

package edu.arizona.cs.stargate.common.cluster;

import edu.arizona.cs.stargate.common.AJsonSerializable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author iychoi
 */
public class ClusterInfo extends AJsonSerializable {
    
    private static final Log LOG = LogFactory.getLog(ClusterInfo.class);
    
    private Hashtable<String, ClusterNodeInfo> m_nodeTable = new Hashtable<String, ClusterNodeInfo>();
    private ArrayList<IClusterConfigChangeEventHandler> m_configChangeEventHandlers = new ArrayList<IClusterConfigChangeEventHandler>();
    
    private String m_name;
    
    ClusterInfo() {
        this.m_name = null;
    }
    
    public static ClusterInfo createInstance(File file) throws IOException {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.fromLocalFile(file);
        return clusterInfo;
    }
    
    public static ClusterInfo createInstance(String json) {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.fromJson(json);
        return clusterInfo;
    }
    
    public static ClusterInfo createInstance(JSONObject jsonobj) {
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.fromJsonObj(jsonobj);
        return clusterInfo;
    }
    
    public ClusterInfo(ClusterInfo that) {
        this.m_name = that.m_name;
        this.m_nodeTable.putAll(that.m_nodeTable);
        this.m_configChangeEventHandlers.addAll(that.m_configChangeEventHandlers);
    }
    
    public ClusterInfo(String name) {
        this.m_name = name;
    }
    
    public ClusterInfo(String name, ClusterNodeInfo[] node) throws NodeAlreadyAddedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        this.m_name = name;
        
        if(node != null) {
            for(ClusterNodeInfo nodeinfo : node) {
                addNode(nodeinfo);
            }
        }
    }
    
    public synchronized String getName() {
        return this.m_name;
    }
    
    synchronized void setName(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        this.m_name = name;
    }
    
    public synchronized int getNodeNumber() {
        return this.m_nodeTable.keySet().size();
    }
    
    public synchronized Collection<ClusterNodeInfo> getAllNodeInfo() {
        return Collections.unmodifiableCollection(this.m_nodeTable.values());
    }
    
    public synchronized ClusterNodeInfo getNodeInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.m_nodeTable.get(name);
    }
    
    public synchronized boolean hasNodeInfo(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        return this.m_nodeTable.containsKey(name);
    }
    
    public synchronized ClusterNodeInfo getGatekeeperNodeInfo() {
        Enumeration<String> keys = this.m_nodeTable.keys();
        while(keys.hasMoreElements()) {
            String key = keys.nextElement();
            ClusterNodeInfo node = this.m_nodeTable.get(key);
            if(node.getGatekeeper()) {
                return node;
            }
        }
        return null;
    }
    
    public synchronized void removeAllNode() {
        Enumeration<String> keys = m_nodeTable.keys();
        while(keys.hasMoreElements()) {
            String key = keys.nextElement();
            ClusterNodeInfo node = m_nodeTable.get(key);
            
            removeNode(node);
        }
    }
    
    public synchronized void addNode(ClusterNodeInfo node) throws NodeAlreadyAddedException {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        if(this.m_nodeTable.containsKey(node.getName())) {
            throw new NodeAlreadyAddedException("node " + node.getName() + "is already added");
        }
        
        ClusterNodeInfo put = this.m_nodeTable.put(node.getName(), node);
        if(put != null) {
            raiseEventForAddNode(put);
        }
    }
    
    public synchronized void addConfigChangeEventHandler(IClusterConfigChangeEventHandler eventHandler) {
        this.m_configChangeEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(IClusterConfigChangeEventHandler eventHandler) {
        this.m_configChangeEventHandlers.remove(eventHandler);
    }
    
    public synchronized void removeConfigChangeEventHandler(String handlerName) {
        ArrayList<IClusterConfigChangeEventHandler> toberemoved = new ArrayList<IClusterConfigChangeEventHandler>();
        
        for(IClusterConfigChangeEventHandler handler : this.m_configChangeEventHandlers) {
            if(handler.getName().equals(handlerName)) {
                toberemoved.add(handler);
            }
        }
        
        for(IClusterConfigChangeEventHandler handler : toberemoved) {
            this.m_configChangeEventHandlers.remove(handler);
        }
    }

    public synchronized void removeNode(ClusterNodeInfo node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is empty or null");
        }
        
        removeNode(node.getName());
    }
    
    public synchronized void removeNode(String name) {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is empty or null");
        }
        
        ClusterNodeInfo removed = this.m_nodeTable.remove(name);
        if(removed != null) {
            raiseEventForRemoveNode(removed);
        }
    }

    private synchronized void raiseEventForAddNode(ClusterNodeInfo node) {
        LOG.debug("node added : " + node.toString());
        
        for(IClusterConfigChangeEventHandler handler: this.m_configChangeEventHandlers) {
            handler.addNode(this, node);
        }
    }
    
    private synchronized void raiseEventForRemoveNode(ClusterNodeInfo node) {
        LOG.debug("node removed : " + node.toString());
        
        for(IClusterConfigChangeEventHandler handler: this.m_configChangeEventHandlers) {
            handler.removeNode(this, node);
        }
    }
    
    @Override
    public String toString() {
        return this.m_name;
    }

    @Override
    public synchronized void fromJsonObj(JSONObject jsonobj) {
        String name = jsonobj.getString("name");
        JSONArray node = jsonobj.getJSONArray("node");
        
        this.m_configChangeEventHandlers.clear();
        this.m_nodeTable.clear();
        this.m_name = name;
        
        int len = node.length();
        for(int i=0;i<len;i++) {
            ClusterNodeInfo nodeInfo = ClusterNodeInfo.createInstance(node.getJSONObject(i));
            try {
                addNode(nodeInfo);
            } catch (NodeAlreadyAddedException ex) {
                LOG.error("failed to add node : " + nodeInfo.toString() + " -- ignored");
            }
        }
    }

    @Override
    public synchronized JSONObject toJsonObj() {
        JSONObject jsonobj = new JSONObject();
        
        jsonobj.put("name", this.m_name);
        
        JSONArray node = new JSONArray();
        Enumeration<String> keys = this.m_nodeTable.keys();
        while(keys.hasMoreElements()) {
            String key = keys.nextElement();
            ClusterNodeInfo nodeInfo = this.m_nodeTable.get(key);
            JSONObject nodeInfoJsonObj = nodeInfo.toJsonObj();
            node.put(nodeInfoJsonObj);
        }
        
        jsonobj.put("node", node);
        return jsonobj;
    }

    public boolean isEmpty() {
        if(this.m_name == null || this.m_name.isEmpty()) {
            return true;
        }
        return false;
    }
}
