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

package edu.arizona.cs.stargate.gatekeeper.client;

import edu.arizona.cs.stargate.common.JsonSerializer;
import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 *
 * @author iychoi
 */
public class GateKeeperClientConfiguration {
    private URI serviceURI;
    
    public static GateKeeperClientConfiguration createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GateKeeperClientConfiguration) serializer.fromJsonFile(file, GateKeeperClientConfiguration.class);
    }
    
    public static GateKeeperClientConfiguration createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (GateKeeperClientConfiguration) serializer.fromJson(json, GateKeeperClientConfiguration.class);
    }
    
    GateKeeperClientConfiguration() {
    }
    
    public GateKeeperClientConfiguration(URI uri) {
        initialize(uri);
    }
    
    private void initialize(URI uri) {
        this.serviceURI = uri;
    }
    
    public void setServiceURI(URI uri) {
        this.serviceURI = uri;
    }
    
    public URI getServiceURI() {
        return this.serviceURI;
    }
}
