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

package edu.arizona.cs.stargate.hdfs;

/**
 *
 * @author iychoi
 */
public class StargateFileStatus {

    private long length;
    private String cluster;
    private String vpath;
    private boolean dir;
    private int blockReplication;
    private long blockSize;
    private long modificationTime;
    
    public long getModificationTime() {
        return this.modificationTime;
    }
 
    public long getBlockSize() {
        return this.blockSize;
    }
 
    public int getBlockReplication() {
        return blockReplication;
    }
    
    public boolean isDir() {
        return dir;
    }
    
    public long getLength() {
        return this.length;
    }
    
    public String getCluster() {
        return this.cluster;
    }
    
    public String getVPath() {
        return this.vpath;
    }
    
}
