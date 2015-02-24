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

package edu.arizona.cs.stargate.common.chunk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class FixedSizeLocalFileChunking extends AChunking {

    private static final int BUFFER_SIZE = 100*1024;
    private int chunkSize;
    
    public FixedSizeLocalFileChunking(int chunkSize) {
        this.chunkSize = chunkSize;
    }
    
    @Override
    public Collection<Chunk> chunk(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        return chunk(new File(resourcePath), hashAlgorithm);
    }
    
    public Collection<Chunk> chunk(File file, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        InputStream is = new FileInputStream(file);
        
        MessageDigest messageDigest = MessageDigest.getInstance(hashAlgorithm);
        DigestInputStream dis = new DigestInputStream(is, messageDigest);
        
        int bufferSize = Math.min((int)this.chunkSize, BUFFER_SIZE);
        byte[] buffer = new byte[bufferSize];
        int nread = 0;
        int toread = this.chunkSize;
        long chunkOffset = 0;
        int chunkSize = 0;
        
        List<Chunk> chunks = new ArrayList<Chunk>();
        
        while((nread = dis.read(buffer, 0, Math.min(toread, bufferSize))) > 0) {
            toread -= nread;
            chunkSize += nread;
            if(toread == 0) {
                byte[] digest = messageDigest.digest();
                chunks.add(new Chunk(file.getAbsoluteFile().toURI(), chunkOffset, chunkSize, digest));
                chunkOffset += chunkSize;
                chunkSize = 0;
                toread = this.chunkSize;
            }
        }
        
        if(chunkSize != 0) {
            // has some data
            byte[] digest = messageDigest.digest();
            chunks.add(new Chunk(file.getAbsoluteFile().toURI(), chunkOffset, chunkSize, digest));
        }
        
        dis.close();
        
        return Collections.unmodifiableCollection(chunks);
    }

    @Override
    public Recipe generateRecipe(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Collection<Chunk> chunks = chunk(resourcePath, hashAlgorithm);
        return new Recipe(resourcePath, hashAlgorithm, chunks);
    }
    
    public Recipe generateRecipe(File file, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Collection<Chunk> chunks = chunk(file, hashAlgorithm);
        return new Recipe(file, hashAlgorithm, chunks);
    }
}
