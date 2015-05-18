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

import edu.arizona.cs.stargate.gatekeeper.cluster.ClusterNode;
import edu.arizona.cs.stargate.gatekeeper.cluster.LocalClusterManager;
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
    
    private final LocalClusterManager localClusterManager;
    private Configuration hadoopConf;
    private int chunkSize;
    
    public FixedSizeHDFSFileRecipeGenerator(LocalClusterManager localClusterManager, Configuration hadoopConf, int chunkSize) {
        this.localClusterManager = localClusterManager;
        this.chunkSize = chunkSize;
        this.hadoopConf = hadoopConf;
    }
    
    private Collection<RecipeChunk> chunk(FileSystem fs, FileStatus status, int chunkSize) throws IOException, NoSuchAlgorithmException {
        long fileLen = status.getLen();
        
        List<RecipeChunk> chunks = new ArrayList<RecipeChunk>();
        int numChunks = (int) (fileLen / chunkSize);
        if(fileLen % chunkSize != 0) {
            numChunks++;
        }
        
        long chunkOffset = 0;
        int curChunkSize = 0;
        for(int i=0;i<numChunks;i++) {
            chunkOffset = i * chunkSize;
            curChunkSize = (int) Math.min(fileLen - chunkOffset, chunkSize);
            
            ArrayList<String> ownerHosts = new ArrayList<String>();
            boolean bAllNode = false;
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(status, chunkOffset, curChunkSize);
            for(BlockLocation blockLocation : fileBlockLocations) {
                for(String host : blockLocation.getHosts()) {
                    if(host.equalsIgnoreCase("localhost")) {
                        if(!bAllNode) {
                            ownerHosts.add("*");
                            bAllNode = true;
                        }
                    } else {
                        ClusterNode node = this.localClusterManager.findNodeByAddress(host);
                        if(node != null) {
                            ownerHosts.add(node.getName());
                        } else {
                            LOG.info("unable to find host : " + host);
                            if(!bAllNode) {
                                ownerHosts.add("*");
                                bAllNode = true;
                            }
                        }
                    }
                }
            }
            
            String[] ownerHostsString = ownerHosts.toArray(new String[0]);
            chunks.add(new RecipeChunk(chunkOffset, curChunkSize, ownerHostsString));
        }
        
        return Collections.unmodifiableCollection(chunks);
    }
    
    private void hash(FileSystem fs, Path path, String hashAlgorithm, RecipeChunk chunk) throws IOException, NoSuchAlgorithmException {
        FSDataInputStream is = fs.open(path, BUFFER_SIZE);
        
        long chunkStart = chunk.getOffset();
        int chunkSize = chunk.getLength();
        
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
    
    @Override
    public LocalRecipe generateRecipe(URI resourcePath, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        Path path = new Path(resourcePath.normalize());
        FileSystem fs = path.getFileSystem(this.hadoopConf);
        FileStatus fileStatus = fs.getFileStatus(path);
        
        Collection<RecipeChunk> chunks = chunk(fs, fileStatus, this.chunkSize);
        
        return new LocalRecipe(resourcePath, hashAlgorithm, fileStatus.getLen(), fileStatus.getModificationTime(), this.chunkSize, chunks);
    }
    
    public LocalRecipe generateRecipe(Path path, String hashAlgorithm) throws IOException, NoSuchAlgorithmException {
        FileSystem fs = path.getFileSystem(this.hadoopConf);
        FileStatus fileStatus = fs.getFileStatus(path);
        
        Collection<RecipeChunk> chunks = chunk(fs, fileStatus, this.chunkSize);
        
        URI resourceUri = path.toUri().normalize();
        return new LocalRecipe(resourceUri, hashAlgorithm, fileStatus.getLen(), fileStatus.getModificationTime(), this.chunkSize, chunks);
    }

    @Override
    public void hashRecipe(LocalRecipe recipe) throws IOException, NoSuchAlgorithmException {
        Path path = new Path(recipe.getResourcePath());
        FileSystem fs = path.getFileSystem(this.hadoopConf);
        if(fs.exists(path) && fs.isFile(path)) {
            String hashAlgorithm = recipe.getHashAlgorithm();

            Collection<RecipeChunk> chunks = recipe.getAllChunks();
            for(RecipeChunk chunk : chunks) {
                if(!chunk.isHashed()) {
                    if(chunk.getOffset() >= 0) {
                        if(chunk.getLength() > 0) {
                            // hash chunk
                            hash(fs, path, hashAlgorithm, chunk);
                        }
                    }
                }
            }
        }
    }
}
