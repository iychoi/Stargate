/*
 * The MIT License
 *
 * Copyright 2016 iychoi.
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
package stargate.server.blockcache;

/**
 *
 * @author iychoi
 */
public class BlockCacheEntry {
    
    private BlockCacheMetadata metadata;
    private byte[] blockData;
    
    public BlockCacheEntry(BlockCacheMetadata metadata, byte[] blockdata) {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is empty or null");
        }
        
        this.metadata = metadata;
        this.blockData = blockdata;
    }

    public BlockCacheMetadata getMetadata() {
        return this.metadata;
    }
    
    public byte[] getBlockData() {
        return this.blockData;
    }
    
    public boolean isEmpty() {
        if(this.metadata.isEmpty() || this.blockData == null) {
            return true;
        }
        return false;
    }
}
