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

package edu.arizona.cs.stargate.common.recipe;

import edu.arizona.cs.stargate.common.cluster.ClusterNodeInfo;
import edu.arizona.cs.stargate.gatekeeper.service.LocalClusterManager;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class FixedSizeLocalFileRecipeGenerator extends ARecipeGenerator {

    private static final Log LOG = LogFactory.getLog(FixedSizeLocalFileRecipeGenerator.class);
    
    private static final int BUFFER_SIZE = 100*1024;
    private int chunkSize;
    
    public FixedSizeLocalFileRecipeGenerator(int chunkSize) {
        this.chunkSize = chunkSize;
    }
    
    private Collection<RecipeChunkInfo> chunk(File file, int chunkSize) throws IOException, NoSuchAlgorithmException {
        String ownerHostName = null;
        
        LocalClusterManager lcm = LocalClusterManager.getInstance();
        ClusterNodeInfo localNode = lcm.getLocalNode();
        
        if(localNode != null) {
            ownerHostName = localNode.getName();
        }
        
        long fileLen = file.length();
        
        List<RecipeChunkInfo> chunks = new ArrayList<RecipeChunkInfo>();
        int numChunks = (int) (fileLen / chunkSize);
        if(fileLen % chunkSize != 0) {
            numChunks++;
        }
        
        long chunkOffset = 0;
        int curChunkSize = 0;
        for(int i=0;i<numChunks;i++) {
            chunkOffset = i * chunkSize;
            curChunkSize = (int) Math.min(fileLen - chunkOffset, chunkSize);
            
            chunks.add(new RecipeChunkInfo(chunkOffset, curChunkSize, new String[] {ownerHostName}));
        }
        
        return Collections.unmodifiableCollection(chunks);
    }
    
    private void hash(File file, String hashAlgorithm, RecipeChunkInfo chunk) throws IOException, NoSuchAlgorithmException {
        InputStream is = new FileInputStream(file);
        long chunkStart = chunk.getChunkStart();
        int chunkSize = chunk.getChunkLen();
        
        if(chunkStart > 0) {
            is.skip(chunkStart);
        }
        
        MessageDigest messageDigest = MessageDigest.getInstance(hashAlgorithm);
        DigestInputStream dis = new DigestInputStream(is, messageDigest);
        
        int bufferSize = Math.min((int)chunkSize, BUFFER_SIZE);
        byte[] buffer = new byte[bufferSize];
        int nread = 0;
        int toread = chunkSize;
        
        while((nread = dis.read(buffer, 0, Math.min(toread, bufferSize))) > 0) {
            toread -= nread;
            if(toread == 0) {
                byte[] digest = messageDigest.digest();
                chunk.setHash(digest);
            }
        }
        
        dis.close();
    }
    
    private Collection<RecipeChunkInfo> chunkAndHash(File file, String hashAlgorithm, int chunkSize) throws IOException, NoSuchAlgorithmException {
        String ownerHostName = null;
        
        LocalClusterManager lcm = LocalClusterManager.getInstance();
        ClusterNodeInfo localNode = lcm.getLocalNode();
        
        if(localNode != null) {
            ownerHostName = localNode.getName();
        }
        
        InputStream is = new FileInputStream(file);
        
        MessageDigest messageDigest = MessageDigest.getInstance(hashAlgorithm);
        DigestInputStream dis = new DigestInputStream(is, messageDigest);
        
        int bufferSize = Math.min((int)chunkSize, BUFFER_SIZE);
        byte[] buffer = new byte[bufferSize];
        int nread = 0;
        int toread = chunkSize;
        long chunkOffset = 0;
        int curChunkSize = 0;
        
        List<RecipeChunkInfo> chunks = new ArrayList<RecipeChunkInfo>();
        
        while((nread = dis.read(buffer, 0, Math.min(toread, bufferSize))) > 0) {
            toread -= nread;
            curChunkSize += nread;
            if(toread == 0) {
                byte[] digest = messageDigest.digest();
                chunks.add(new RecipeChunkInfo(chunkOffset, curChunkSize, digest));
                chunkOffset += curChunkSize;
                curChunkSize = 0;
                toread = chunkSize;
            }
        }
        
        if(curChunkSize != 0) {
            // has some data
            byte[] digest = messageDigest.digest();
            chunks.add(new RecipeChunkInfo(chunkOffset, curChunkSize, digest, new String[] {ownerHostName}));
        }
        
        dis.close();
        
        return Collections.unmodifiableCollection(chunks);
    }

    @Override
    public LocalClusterRecipe generateRecipe(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        // test hash algorithm
        MessageDigest md = MessageDigest.getInstance(hashAlgorithm);
        
        File file = new File(resourcePath.normalize());
        Collection<RecipeChunkInfo> chunks = chunkAndHash(file, hashAlgorithm, this.chunkSize);
        return new LocalClusterRecipe(resourcePath.normalize(), hashAlgorithm, chunks);
    }
    
    public LocalClusterRecipe generateRecipe(File file, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Collection<RecipeChunkInfo> chunks = chunkAndHash(file, hashAlgorithm, this.chunkSize);
        URI resourceUri = file.toURI().normalize();
        return new LocalClusterRecipe(resourceUri, hashAlgorithm, chunks);
    }

    @Override
    public LocalClusterRecipe generateRecipeWithoutHash(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        File file = new File(resourcePath.normalize());
        Collection<RecipeChunkInfo> chunks = chunk(file, this.chunkSize);
        return new LocalClusterRecipe(resourcePath, hashAlgorithm, chunks);
    }
    
    public LocalClusterRecipe generateRecipeWithoutHash(File file, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Collection<RecipeChunkInfo> chunks = chunk(file, this.chunkSize);
        URI resourceUri = file.toURI().normalize();
        return new LocalClusterRecipe(resourceUri, hashAlgorithm, chunks);
    }

    @Override
    public void hashRecipe(LocalClusterRecipe recipe) throws IOException, NoSuchAlgorithmException {
        File file = new File(recipe.getResourcePath());
        if(file.exists() && file.isFile()) {
            String hashAlgorithm = recipe.getHashAlgorithm();

            Collection<RecipeChunkInfo> chunks = recipe.getAllChunk();
            for(RecipeChunkInfo chunk : chunks) {
                if(!chunk.isHashed()) {
                    if(chunk.getChunkStart() >= 0) {
                        if(chunk.getChunkLen() > 0) {
                            // hash chunk
                            hash(file, hashAlgorithm, chunk);
                        }
                    }
                }
            }
        }
    }
}
