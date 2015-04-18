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

package edu.arizona.cs.stargate.gatekeeper.recipe;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * @author iychoi
 */
public class ChunkReader extends InputStream {

    private InputStream is;
    private long offset;
    private int size;
    private long currentOffset;
    
    public ChunkReader(InputStream is, long offset, int size) throws IOException {
        this.is = is;
        this.offset = offset;
        this.size = size;
        this.currentOffset = offset;
        long skip = this.is.skip(this.offset);
        if(skip != this.offset) {
            throw new IOException("failed to move offset to " + skip);
        }
    }
    
    public ChunkReader(FSDataInputStream is, long offset, int size) throws IOException {
        this.is = is;
        this.offset = offset;
        this.size = size;
        this.currentOffset = offset;
        is.seek(offset);
        long skip = is.getPos();
        if(skip != this.offset) {
            throw new IOException("failed to move offset to " + skip);
        }
    }
    
    private int availableBytes(int toRead) {
        long offsetMove = (this.currentOffset - this.offset) + toRead;
        if(offsetMove > this.size) {
            return (int) (this.size - (this.currentOffset - this.offset));
        } else {
            return toRead;
        }
    }
    
    @Override
    public int read() throws IOException {
        if(availableBytes(1) >= 1) {
            int read = is.read();
            if(read >= 0) {
                this.currentOffset++;
            }
            return read;
        }
        return -1;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        int availableBytes = availableBytes(bytes.length);
        if(availableBytes <= 0) {
            return -1;
        }
        
        int read = this.is.read(bytes, 0, availableBytes);
        if(read >= 0) {
            this.currentOffset += read;
        }
        return read;
    }

    @Override
    public int read(byte[] bytes, int offset, int len) throws IOException {
        int availableBytes = availableBytes(len);
        if(availableBytes <= 0) {
            return -1;
        }
        
        int read = this.is.read(bytes, offset, availableBytes);
        if(read >= 0) {
            this.currentOffset += read;
        }
        return read;
    }

    @Override
    public long skip(long len) throws IOException {
        int availableBytes = availableBytes((int) len);
        if(availableBytes <= 0) {
            return -1;
        }
        
        long skipped = this.is.skip(availableBytes);
        if(skipped >= 0) {
            this.currentOffset += skipped;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        int availableBytes = availableBytes((int) this.is.available());
        if(availableBytes <= 0) {
            return -1;
        }
        
        return availableBytes;
    }

    @Override
    public void close() throws IOException {
        this.is.close();
    }

    @Override
    public synchronized void mark(int i) {
    }

    @Override
    public synchronized void reset() throws IOException {
        this.is.reset();
        long skip = this.is.skip(this.offset);
        if(skip != this.offset) {
            throw new IOException("failed to move offset to " + skip);
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
