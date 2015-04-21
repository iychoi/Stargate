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

package edu.arizona.cs.stargate.client.fs;

import edu.arizona.cs.stargate.gatekeeper.dataexport.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.restful.client.FileSystemRestfulClient;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ChunkInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(ChunkInputStream.class);
    
    private FileSystemRestfulClient filesystemRestfulClient;
    private String clusterName;
    private String virtualPath;
    private long size;
    private long blockSize;
    private long offset;
    private FileChunkData cachedChunkData;
    
    public ChunkInputStream(FileSystemRestfulClient filesystemClient, String clusterName, String virtualPath, long size, long blockSize) {
        initialize(filesystemClient, clusterName, virtualPath, size, blockSize);
    }

    public ChunkInputStream(FileSystemRestfulClient filesystemClient, VirtualFileStatus status) {
        initialize(filesystemClient, status.getClusterName(), status.getVirtualPath(), status.getLength(), status.getBlockSize());
    }
    
    private void initialize(FileSystemRestfulClient filesystemClient, String clusterName, String virtualPath, long size, long blockSize) {
        this.filesystemRestfulClient = filesystemClient;
        this.clusterName = clusterName;
        this.virtualPath = virtualPath;
        this.size = size;
        this.blockSize = blockSize;
        this.offset = 0;
    }
    
    public long getPos() throws IOException {
        return this.offset;
    }
    
    public void seek(long l) throws IOException {
        if(l < 0) {
            throw new IOException("cannot seek to negative offset : " + l);
        }
        
        if(l >= this.size) {
            this.offset = this.size;
        } else {
            this.offset = l;
        }
    }
    
    @Override
    public long skip(long l) throws IOException {
        if(l <= 0) {
            return 0;
        }
        
        if(this.offset >= this.size) {
            return 0;
        }
        
        long lavailable = this.size - this.offset;
        if(l >= lavailable) {
            this.offset = this.size;
            return lavailable;
        } else {
            this.offset += l;
            return l;
        }
    }
    
    @Override
    public synchronized int available() throws IOException {
        if(this.cachedChunkData == null) {
            return (int) Math.min(this.blockSize, this.size);
        } else {
            if(this.cachedChunkData.getOffset() <= this.offset &&
                    this.cachedChunkData.getOffset() + this.cachedChunkData.getSize() > this.offset) {
                return (int) (this.cachedChunkData.getOffset() + this.cachedChunkData.getSize() - this.offset);
            }
        }
        
        return (int) Math.min(this.blockSize, this.size);
    }
    
    private synchronized void loadChunkData(long offsetStart) throws IOException {
        long lavailable = this.size - offsetStart;
        long csize = Math.min(lavailable, this.blockSize);
        
        if(this.cachedChunkData != null) {
            if(this.cachedChunkData.getOffset() == offsetStart) {
                // safe to reuse
                return;
            }
        }
        
        byte[] data = this.filesystemRestfulClient.readChunkData(this.clusterName, this.virtualPath, offset, csize);
        if(data.length != csize) {
            throw new IOException("received chunk data does not match to requested size");
        }
        
        FileChunkData chunkdata = new FileChunkData(offset, csize, data);
        this.cachedChunkData = chunkdata;
    }
    
    private synchronized boolean isEOF() {
        if(this.offset >= this.size) {
            return true;
        }
        return false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(isEOF()) {
            return -1;
        }
        
        long blockStartOffset = (this.offset / this.blockSize) * this.blockSize;
        loadChunkData(blockStartOffset);
        
        if(this.cachedChunkData == null) {
            throw new IOException("could not access cached chunk data object");
        }
        
        int inoffset = (int) (this.offset - this.cachedChunkData.getOffset());
        byte ch = this.cachedChunkData.getData()[inoffset];
        
        this.offset++;
        return ch;
    }
    
    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(isEOF()) {
            return -1;
        }
        
        long lavailable = this.size - this.offset;
        int remain = len;
        if(len > lavailable) {
            remain = (int) lavailable;
        }
        
        int copied = remain;
        
        int doff = off;
        while(remain > 0) {
            long blockStartOffset = (this.offset / this.blockSize) * this.blockSize;
            loadChunkData(blockStartOffset);

            if(this.cachedChunkData == null) {
                throw new IOException("could not access cached chunk data object");
            }

            int inoffset = (int) (this.offset - this.cachedChunkData.getOffset());
            int inlength = (int) (this.cachedChunkData.getSize() - inoffset);
            
            System.arraycopy(this.cachedChunkData.getData(), inoffset, bytes, doff, inlength);
            this.offset += inlength;
            doff += inlength;
            remain -= inlength;
        }
        return copied;
    }
    
    @Override
    public void close() throws IOException {
        this.offset = 0;
        this.size = 0;
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
