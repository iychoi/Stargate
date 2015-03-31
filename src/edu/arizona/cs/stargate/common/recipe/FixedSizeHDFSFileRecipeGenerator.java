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
import java.io.IOException;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class FixedSizeHDFSFileRecipeGenerator extends ARecipeGenerator {

    private static final Log LOG = LogFactory.getLog(FixedSizeHDFSFileRecipeGenerator.class);
    
    private static final int BUFFER_SIZE = 100*1024;
    private int chunkSize;
    private Configuration hadoopConf;
    
    public FixedSizeHDFSFileRecipeGenerator(Configuration hadoopConf, int chunkSize) {
        this.chunkSize = chunkSize;
        this.hadoopConf = hadoopConf;
    }
    
    private Collection<RecipeChunkInfo> chunk(Path path, int chunkSize) throws IOException, NoSuchAlgorithmException {
        FileSystem fs = path.getFileSystem(this.hadoopConf);
        FileStatus fileStatus = fs.getFileStatus(path);
        
        long fileLen = fileStatus.getLen();
        
        List<RecipeChunkInfo> chunks = new ArrayList<RecipeChunkInfo>();
        int numChunks = (int) (fileLen / chunkSize);
        if(fileLen % chunkSize != 0) {
            numChunks++;
        }
        
        LocalClusterManager lcm = LocalClusterManager.getInstance();
        
        long chunkOffset = 0;
        int curChunkSize = 0;
        for(int i=0;i<numChunks;i++) {
            chunkOffset = i * chunkSize;
            curChunkSize = (int) Math.min(fileLen - chunkOffset, chunkSize);
            
            ArrayList<String> ownerHosts = new ArrayList<String>();
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, chunkOffset, curChunkSize);
            for(BlockLocation blockLocation : fileBlockLocations) {
                for(String host : blockLocation.getHosts()) {
                    if(host.equalsIgnoreCase("localhost")) {
                        ownerHosts.add("*");
                    } else {
                        ClusterNodeInfo node = lcm.findNodeByAddress(host);
                        if(node != null) {
                            ownerHosts.add(node.getName());
                        } else {
                            LOG.info("unable to find host : " + host);
                            ownerHosts.add("*");
                        }
                    }
                }
            }
            
            String[] ownerHostsString = ownerHosts.toArray(new String[0]);
            chunks.add(new RecipeChunkInfo(chunkOffset, curChunkSize, ownerHostsString));
        }
        
        return Collections.unmodifiableCollection(chunks);
    }
    
    private void hash(FileSystem fs, Path path, String hashAlgorithm, RecipeChunkInfo chunk) throws IOException, NoSuchAlgorithmException {
        FSDataInputStream is = fs.open(path, BUFFER_SIZE);
        
        long chunkStart = chunk.getChunkStart();
        int chunkSize = chunk.getChunkLen();
        
        if(chunkStart > 0) {
            is.seek(chunkStart);
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
    
    private Collection<RecipeChunkInfo> chunkAndHash(Path path, String hashAlgorithm, int chunkSize) throws IOException, NoSuchAlgorithmException {
        FileSystem fs = path.getFileSystem(this.hadoopConf);
        FileStatus fileStatus = fs.getFileStatus(path);
        FSDataInputStream is = fs.open(path, BUFFER_SIZE);
        
        MessageDigest messageDigest = MessageDigest.getInstance(hashAlgorithm);
        DigestInputStream dis = new DigestInputStream(is, messageDigest);
        
        int bufferSize = Math.min((int)chunkSize, BUFFER_SIZE);
        byte[] buffer = new byte[bufferSize];
        int nread = 0;
        int toread = chunkSize;
        long chunkOffset = 0;
        int curChunkSize = 0;
        
        List<RecipeChunkInfo> chunks = new ArrayList<RecipeChunkInfo>();
        
        LocalClusterManager lcm = LocalClusterManager.getInstance();
        
        while((nread = dis.read(buffer, 0, Math.min(toread, bufferSize))) > 0) {
            toread -= nread;
            curChunkSize += nread;
            if(toread == 0) {
                byte[] digest = messageDigest.digest();
                
                ArrayList<String> ownerHosts = new ArrayList<String>();
                BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, chunkOffset, curChunkSize);
                for (BlockLocation blockLocation : fileBlockLocations) {
                    for (String host : blockLocation.getHosts()) {
                        if(host.equalsIgnoreCase("localhost")) {
                            ownerHosts.add("*");
                        } else {
                            ClusterNodeInfo node = lcm.findNodeByAddress(host);
                            if (node != null) {
                                ownerHosts.add(node.getName());
                            } else {
                                LOG.info("unable to find host : " + host);
                                ownerHosts.add("*");
                            }
                        }
                    }
                }

                String[] ownerHostsString = ownerHosts.toArray(new String[0]);
                
                chunks.add(new RecipeChunkInfo(chunkOffset, curChunkSize, digest, ownerHostsString));
                chunkOffset += curChunkSize;
                curChunkSize = 0;
                toread = chunkSize;
            }
        }
        
        if(curChunkSize != 0) {
            // has some data
            byte[] digest = messageDigest.digest();
            chunks.add(new RecipeChunkInfo(chunkOffset, curChunkSize, digest));
        }
        
        dis.close();
        
        return Collections.unmodifiableCollection(chunks);
    }

    @Override
    public Recipe generateRecipe(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        // test hash algorithm
        MessageDigest md = MessageDigest.getInstance(hashAlgorithm);
        
        Path path = new Path(resourcePath.normalize());
        Collection<RecipeChunkInfo> chunks = chunkAndHash(path, hashAlgorithm, this.chunkSize);
        return new Recipe(resourcePath.normalize(), hashAlgorithm, chunks);
    }
    
    public Recipe generateRecipe(Path path, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Collection<RecipeChunkInfo> chunks = chunkAndHash(path, hashAlgorithm, this.chunkSize);
        URI resourceUri = path.toUri().normalize();
        return new Recipe(resourceUri, hashAlgorithm, chunks);
    }

    @Override
    public Recipe generateRecipeWithoutHash(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Path path = new Path(resourcePath.normalize());
        Collection<RecipeChunkInfo> chunks = chunk(path, this.chunkSize);
        return new Recipe(resourcePath, hashAlgorithm, chunks);
    }
    
    public Recipe generateRecipeWithoutHash(Path path, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Collection<RecipeChunkInfo> chunks = chunk(path, this.chunkSize);
        URI resourceUri = path.toUri().normalize();
        return new Recipe(resourceUri, hashAlgorithm, chunks);
    }

    @Override
    public void hashRecipe(Recipe recipe) throws IOException, NoSuchAlgorithmException {
        Path path = new Path(recipe.getResourcePath());
        FileSystem fs = path.getFileSystem(this.hadoopConf);
        if(fs.exists(path) && fs.isFile(path)) {
            String hashAlgorithm = recipe.getHashAlgorithm();

            Collection<RecipeChunkInfo> chunks = recipe.getAllChunk();
            for(RecipeChunkInfo chunk : chunks) {
                if(!chunk.isHashed()) {
                    if(chunk.getChunkStart() >= 0) {
                        if(chunk.getChunkLen() > 0) {
                            // hash chunk
                            hash(fs, path, hashAlgorithm, chunk);
                        }
                    }
                }
            }
        }
    }
}
