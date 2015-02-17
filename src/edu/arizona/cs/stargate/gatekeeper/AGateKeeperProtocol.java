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

package edu.arizona.cs.stargate.gatekeeper;

import edu.arizona.cs.stargate.common.cluster.ClusterAlreadyAddedException;
import edu.arizona.cs.stargate.common.cluster.ClusterInfo;
import edu.arizona.cs.stargate.common.cluster.IClusterConfigChangeEventHandler;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public abstract class AGateKeeperProtocol {
    public abstract ClusterInfo getLocalClusterInfo();
    
    public abstract int getRemoteClusterNumber();
    
    public abstract Collection<ClusterInfo> getAllRemoteClusterInfo();
    
    public abstract ClusterInfo getRemoteClusterInfo(String name);
    
    public abstract boolean hasRemoteClusterInfo(String name);
    
    public abstract void removeAllRemoteCluster();
    
    public abstract void addRemoteCluster(ClusterInfo cluster) throws ClusterAlreadyAddedException;
    
    public abstract void addConfigChangeEventHandler(IClusterConfigChangeEventHandler eventHandler);
    
    public abstract void removeConfigChangeEventHandler(IClusterConfigChangeEventHandler eventHandler);
    
    public abstract void removeConfigChangeEventHandler(String handlerName);

    public abstract void removeRemoteCluster(ClusterInfo cluster);
    
    public abstract void removeRemoteCluster(String name);
}
