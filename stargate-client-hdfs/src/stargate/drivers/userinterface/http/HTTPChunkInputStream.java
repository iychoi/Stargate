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

package stargate.drivers.userinterface.http;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import stargate.commons.recipe.ChunkData;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;

/**
 *
 * @author iychoi
 */
public class HTTPChunkInputStream extends FSInputStream {

    private static final Log LOG = LogFactory.getLog(HTTPChunkInputStream.class);
    
    private HTTPUserInterfaceClient httpUserInterfaceClient;
    private Recipe recipe;
    private ChunkData cachedChunkData;
    private long offset;
    private long size;
    
    public HTTPChunkInputStream(HTTPUserInterfaceClient client, Recipe recipe) {
        if(client == null) {
            throw new IllegalArgumentException("client is null");
        }
        
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        initialize(client, recipe);
    }

    private void initialize(HTTPUserInterfaceClient client, Recipe recipe) {
        if(client == null) {
            throw new IllegalArgumentException("client is null");
        }
        
        if(recipe == null || recipe.isEmpty()) {
            throw new IllegalArgumentException("recipe is null or empty");
        }
        
        this.httpUserInterfaceClient = client;
        this.recipe = recipe;
        this.cachedChunkData = null;
        this.offset = 0;
        this.size = recipe.getMetadata().getObjectSize();
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.offset;
    }
    
    @Override
    public synchronized int available() throws IOException {
        if(this.cachedChunkData != null) {
            if(this.cachedChunkData.getOffset() <= this.offset &&
                    this.cachedChunkData.getOffset() + this.cachedChunkData.getLength() > this.offset) {
                return (int) (this.cachedChunkData.getOffset() + this.cachedChunkData.getLength() - this.offset);
            }
        }
        
        if(this.recipe.getChunkSize() == 0) {
            RecipeChunk chunk = this.recipe.getChunk(this.offset);
            return (int) Math.min(chunk.getOffset() + chunk.getLength() - this.offset, this.size - this.offset);
        } else {
            return (int) Math.min(this.recipe.getChunkSize(), this.size - this.offset);
        }
    }
    
    @Override
    public synchronized void seek(long l) throws IOException {
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
    public synchronized long skip(long l) throws IOException {
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
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    private synchronized void loadChunkData(long offset) throws IOException {
        if(this.cachedChunkData != null) {
            if(this.cachedChunkData.getOffset() <= offset &&
                    this.cachedChunkData.getOffset() + this.cachedChunkData.getLength() > this.offset) {
                // safe to reuse
                return;
            }
        }
        
        byte[] data = null;
        RecipeChunk chunk = this.recipe.getChunk(offset);
        try {
            InputStream dataChunkIS = this.httpUserInterfaceClient.getDataChunk(this.recipe.getMetadata().getPath().getClusterName(), chunk.getHashString());
            data = IOUtils.toByteArray(dataChunkIS);
            dataChunkIS.close();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        
        if (data == null || data.length != chunk.getLength()) {
            throw new IOException("received chunk data does not match to requested size");
        }

        ChunkData chunkdata = new ChunkData(chunk.getOffset(), chunk.getLength(), data);
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
        
        loadChunkData(this.offset);
        
        int inoffset = (int) (this.offset - this.cachedChunkData.getOffset());
        byte ch = this.cachedChunkData.getData()[inoffset];
        
        this.offset++;
        return ch;
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
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
            loadChunkData(this.offset);

            int inoffset = (int) (this.offset - this.cachedChunkData.getOffset());
            int inlength = (int) (this.cachedChunkData.getLength() - inoffset);
            
            System.arraycopy(this.cachedChunkData.getData(), inoffset, bytes, doff, inlength);
            this.offset += inlength;
            doff += inlength;
            remain -= inlength;
        }
        return copied;
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.offset = 0;
    }
    
    @Override
    public synchronized boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
