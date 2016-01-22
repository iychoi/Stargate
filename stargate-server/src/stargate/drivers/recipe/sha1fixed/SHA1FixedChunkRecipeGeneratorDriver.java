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
package stargate.drivers.recipe.sha1fixed;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.drivers.ADriverConfiguration;
import stargate.commons.recipe.ARecipeGeneratorDriver;
import stargate.commons.recipe.ARecipeGeneratorDriverConfiguration;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;

/**
 *
 * @author iychoi
 */
public class SHA1FixedChunkRecipeGeneratorDriver extends ARecipeGeneratorDriver {

    private static final Log LOG = LogFactory.getLog(SHA1FixedChunkRecipeGeneratorDriver.class);
    
    private SHA1FixedChunkRecipeGeneratorDriverConfiguration config;
    private int chunkSize;
    private static final int BUFFER_SIZE = 100*1024;
    private static final String HASH_ALGORITHM = "SHA-1";
    
    public SHA1FixedChunkRecipeGeneratorDriver(ADriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof SHA1FixedChunkRecipeGeneratorDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of SHA1FixedChunkRecipeGeneratorDriverConfiguration");
        }
        
        this.config = (SHA1FixedChunkRecipeGeneratorDriverConfiguration) config;
        this.chunkSize = this.config.getChunkSize();
    }
    
    public SHA1FixedChunkRecipeGeneratorDriver(ARecipeGeneratorDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof SHA1FixedChunkRecipeGeneratorDriverConfiguration)) {
            throw new IllegalArgumentException("config is not an instance of SHA1FixedChunkRecipeGeneratorDriverConfiguration");
        }
        
        this.config = (SHA1FixedChunkRecipeGeneratorDriverConfiguration) config;
    }
    
    public SHA1FixedChunkRecipeGeneratorDriver(SHA1FixedChunkRecipeGeneratorDriverConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void startDriver() throws IOException {
        
    }

    @Override
    public synchronized void stopDriver() throws IOException {
    }
    
    @Override
    public synchronized int getChunkSize() {
        return this.chunkSize;
    }
    
    @Override
    public synchronized String getHashAlgorithm() {
        return HASH_ALGORITHM;
    }
    
    @Override
    public synchronized String getDriverName() {
        return "SHA1FixedChunkRecipeGeneratorDriver";
    }

    @Override
    public synchronized Recipe getRecipe(DataObjectMetadata metadata, InputStream is) throws IOException {
        if(metadata == null || metadata.isEmpty()) {
            throw new IllegalArgumentException("metadata is null or empty");
        }

        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        List<RecipeChunk> chunk = new ArrayList<RecipeChunk>();
        
        int bufferSize = Math.min(this.chunkSize, BUFFER_SIZE);
        byte[] buffer = new byte[bufferSize];
            
        try {    
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
            DigestInputStream dis = new DigestInputStream(is, messageDigest);
            
            long chunkOffset = 0;
            
            while(chunkOffset < metadata.getObjectSize()) {
                int chunkLength = 0;
                int nread = 0;
                int toread = this.chunkSize;

                while((nread = dis.read(buffer, 0, Math.min(toread, bufferSize))) > 0) {
                    chunkLength += nread;
                    toread -= nread;
                    if(toread == 0) {
                        break;
                    }
                }
                
                byte[] digest = messageDigest.digest();
                chunk.add(new RecipeChunk(chunkOffset, chunkLength, digest));
                messageDigest.reset();
                
                if(nread <= 0) {
                    //EOF
                    break;
                }
                
                chunkOffset += chunkLength;
            }
            
            dis.close();
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        }
        
        return new Recipe(metadata, HASH_ALGORITHM, this.chunkSize, chunk);
    }
}
