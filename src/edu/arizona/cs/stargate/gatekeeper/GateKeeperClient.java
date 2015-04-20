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

import edu.arizona.cs.stargate.gatekeeper.restful.client.GateKeeperRestfulClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class GateKeeperClient {
    
    private static final Log LOG = LogFactory.getLog(GateKeeperClient.class);
    
    private GateKeeperClientConfiguration config;
    private GateKeeperRestfulClient restfulClient;
    
    public GateKeeperClient(GateKeeperClientConfiguration conf) {
        this.config = conf;
        this.restfulClient = new GateKeeperRestfulClient(conf);
    }
    
    public GateKeeperRestfulClient getRestfulClient() {
        return this.restfulClient;
    }
    
    public GateKeeperClientConfiguration getConfiguration() {
        return this.config;
    }
    
    @Override
    public synchronized String toString() {
        return "GateKeeperClient";
    }
    
    public void start() {
        this.restfulClient.start();
    }
    
    public void stop() {
        this.restfulClient.stop();
    }
}
