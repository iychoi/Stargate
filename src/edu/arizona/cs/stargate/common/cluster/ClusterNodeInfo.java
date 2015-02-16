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
import java.net.URI;
import java.net.URISyntaxException;
import org.json.JSONObject;

/**
 *
 * @author iychoi
 */
public class ClusterNodeInfo extends AJsonSerializable {
    
    private String m_name;
    private URI m_addr;
    private boolean m_gatekeeper;
    
    ClusterNodeInfo() {
        this.m_name = null;
        this.m_addr = null;
        this.m_gatekeeper = false;
    }
    
    public static ClusterNodeInfo createInstance(File file) throws IOException {
        ClusterNodeInfo nodeInfo = new ClusterNodeInfo();
        nodeInfo.fromLocalFile(file);
        return nodeInfo;
    }
    
    public static ClusterNodeInfo createInstance(String json) {
        ClusterNodeInfo nodeInfo = new ClusterNodeInfo();
        nodeInfo.fromJson(json);
        return nodeInfo;
    }
    
    public static ClusterNodeInfo createInstance(JSONObject jsonobj) {
        ClusterNodeInfo nodeInfo = new ClusterNodeInfo();
        nodeInfo.fromJsonObj(jsonobj);
        return nodeInfo;
    }
    
    public ClusterNodeInfo(ClusterNodeInfo that) {
        this.m_name = that.m_name;
        this.m_addr = that.m_addr;
        this.m_gatekeeper = that.m_gatekeeper;
    }
    
    public ClusterNodeInfo(String name, URI addr, boolean gatekeeper) {
        initializeClusterNodeInfo(name, addr, gatekeeper);
    }
    
    public ClusterNodeInfo(String name, URI addr) {
        initializeClusterNodeInfo(name, addr, false);
    }
    
    public ClusterNodeInfo(String name, String addr) throws URISyntaxException {
        initializeClusterNodeInfo(name, new URI(addr), false);
    }
    
    public ClusterNodeInfo(String name, String addr, boolean gatekeeper) throws URISyntaxException {
        initializeClusterNodeInfo(name, new URI(addr), gatekeeper);
    }
    
    private void initializeClusterNodeInfo(String name, URI addr, boolean gatekeeper) {
        setName(name);
        setAddr(addr);
        setGatekeeper(gatekeeper);
    }

    public String getName() {
        return this.m_name;
    }
    
    void setName(String name) {
        this.m_name = name;
    }

    public URI getAddr() {
        return m_addr;
    }

    void setAddr(URI m_addr) {
        this.m_addr = m_addr;
    }
    
    void setAddr(String m_addr) throws URISyntaxException {
        this.m_addr = new URI(m_addr);
    }
    
    public boolean getGatekeeper() {
        return this.m_gatekeeper;
    }
    
    void setGatekeeper(boolean gatekeeper) {
        this.m_gatekeeper = gatekeeper;
    }
    
    public boolean isEmpty() {
        if(this.m_name == null || this.m_name.isEmpty()) {
            return true;
        }
        
        if(this.m_addr == null || this.m_addr.getHost().isEmpty()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        String gkwk = this.m_gatekeeper ? "GK" : "WK";
        return this.m_name + "(" + gkwk + ", " + this.m_addr.toString() + ")";
    }
    
    @Override
    public void fromJsonObj(JSONObject jsonobj) {
        String name = jsonobj.getString("name");
        String addr = jsonobj.getString("addr");
        boolean gatekeeper = jsonobj.getBoolean("gatekeeper");
        
        try {
            setName(name);
            setAddr(addr);
            setGatekeeper(gatekeeper);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("address is not in URI format");
        }
    }
    
    @Override
    public JSONObject toJsonObj() {
        JSONObject jsonobj = new JSONObject();
        
        jsonobj.put("name", this.m_name);
        jsonobj.put("addr", this.m_addr.toString());
        jsonobj.put("gatekeeper", this.m_gatekeeper);
        
        return jsonobj;
    }
}