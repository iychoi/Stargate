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
package stargate.server.volume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.utils.DateTimeUtils;
import stargate.server.blockcache.BlockCacheEntry;
import stargate.server.blockcache.BlockCacheManager;
import stargate.server.blockcache.BlockCacheMetadata;
import stargate.server.recipe.RecipeGeneratorManager;

/**
 *
 * @author iychoi
 */
public class CachedInputStreamHandler implements IInterceptableInputStreamHandler {
    
    private static final Log LOG = LogFactory.getLog(CachedInputStreamHandler.class);
    
    private BlockCacheManager blockCacheManager;
    private RecipeGeneratorManager recipeGeneratorManager;
    private String hash;
    private ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    public CachedInputStreamHandler(BlockCacheManager blockCacheManager, RecipeGeneratorManager recipeGeneratorManager, String hash) {
        if(blockCacheManager == null) {
            throw new IllegalArgumentException("blockCacheManager is null");
        }
        
        if(recipeGeneratorManager == null) {
            throw new IllegalArgumentException("recipeGeneratorManager is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is empty or null");
        }
        
        this.blockCacheManager = blockCacheManager;
        this.recipeGeneratorManager = recipeGeneratorManager;
        this.hash = hash;
    }

    @Override
    public void onRead(int b) {
        if(b == -1) {
            done();
        } else {
            this.baos.write(b);
        }
    }

    @Override
    public void onRead(int readLen, byte[] buffer) {
        if(readLen != 0) {
            this.baos.write(buffer, 0, readLen);
        } else {
            done();
        }
    }

    @Override
    public void onRead(int readLen, byte[] buffer, int offset, int len) {
        if(readLen != 0) {
            this.baos.write(buffer, offset, readLen);
        } else {
            done();
        }
    }

    protected void done() {
        byte[] blockData = this.baos.toByteArray();
        
        try {
            String hash = this.recipeGeneratorManager.getHash(blockData);
            if(hash.equalsIgnoreCase(this.hash)) {
                // good
                BlockCacheMetadata metadata = new BlockCacheMetadata(hash, blockData.length, DateTimeUtils.getCurrentTime());
                BlockCacheEntry entry = new BlockCacheEntry(metadata, blockData);
                this.blockCacheManager.addBlockCache(entry);
            } else {
                LOG.error("hash of received data does not match to expected - discard");
            }
        } catch (IOException ex) {
            LOG.error("Could not get a hashcode from bytes", ex);
        }
    }
}
