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

package edu.arizona.cs.stargate.fs;

import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeChunk;
import edu.arizona.cs.stargate.gatekeeper.restful.client.FileSystemRestfulClient;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ChunkInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(ChunkInputStream.class);
    
    private FileSystemRestfulClient filesystemRestfulClient;
    private ArrayList<RecipeChunk> chunks = new ArrayList<RecipeChunk>();
    private long offset;
    private long size;
    private ChunkData chunkData;
    
    public ChunkInputStream(FileSystemRestfulClient filesystemClient, Collection<RecipeChunk> chunks) {
        this.filesystemRestfulClient = filesystemClient;
        this.chunks.addAll(chunks);
        this.size = 0;
        for(RecipeChunk chunk : this.chunks) {
            this.size += chunk.getLength();
        }
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
        RecipeChunk chunk = getChunk(this.offset);
        return (int) (chunk.getOffset() + chunk.getLength() - this.offset);
    }
    
    private RecipeChunk getChunk(long offset) {
        for(RecipeChunk chunk : this.chunks) {
            if(chunk.getOffset() <= offset) {
                if(chunk.getOffset() + chunk.getLength() > offset) {
                    return chunk;
                }
            }
        }
        return null;
    }
    
    private synchronized void readChunk(RecipeChunk chunk) {
        if(this.chunkData != null &&
                this.chunkData.getChunk().getOffset() == chunk.getOffset()) {
            return;
        }
        
        byte[] data = this.filesystemRestfulClient.getChunkData(chunk);
        ChunkData chunkdata = new ChunkData(chunk, data);
        this.chunkData = chunkdata;
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
        
        RecipeChunk chunk = getChunk(this.offset);
        readChunk(chunk);
        
        int inoffset = (int) (this.offset - chunk.getOffset());
        
        byte ch = this.chunkData.getData()[inoffset];
        
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
            RecipeChunk chunk = getChunk(this.offset);
            readChunk(chunk);

            int inoffset = (int) (this.offset - chunk.getOffset());
            int inlength = (int) (chunk.getLength() - inoffset);
            
            System.arraycopy(this.chunkData.getData(), inoffset, bytes, doff, inlength);
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
        this.chunks.clear();
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
