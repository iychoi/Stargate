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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class ChunkReaderFactory {
    private static final Log LOG = LogFactory.getLog(ChunkReaderFactory.class);
    
    private static final int BUFFER_SIZE = 100*1024;
    
    public static ChunkReader getChunkReader(URI resourceUri, long offset, int size) throws IOException {
        if(resourceUri.getScheme().equalsIgnoreCase("hdfs") || resourceUri.getScheme().equalsIgnoreCase("dfs")) {
            return getHDFSFileChunkReader(resourceUri, offset, size);
        } else if(resourceUri.getScheme().equalsIgnoreCase("file")) {
            return getLocalFileChunkReader(resourceUri, offset, size);
        }
        
        throw new IOException("Unknown resource scheme");
    }
    
    public static ChunkReader getHDFSFileChunkReader(URI resourceUri, long offset, int size) throws IOException {
        Path path = new Path(resourceUri.normalize());
        FileSystem fs = path.getFileSystem(new Configuration());
        FSDataInputStream is = fs.open(path, BUFFER_SIZE);
        
        return new ChunkReader(is, offset, size);
    }

    public static ChunkReader getLocalFileChunkReader(URI resourceUri, long offset, int size) throws FileNotFoundException, IOException {
        File file = new File(resourceUri);
        InputStream is = new FileInputStream(file);
        
        return new ChunkReader(is, offset, size);
    }
}
