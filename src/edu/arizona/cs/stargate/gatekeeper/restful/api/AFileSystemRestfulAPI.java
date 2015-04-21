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

package edu.arizona.cs.stargate.gatekeeper.restful.api;

import edu.arizona.cs.stargate.gatekeeper.cluster.Cluster;
import edu.arizona.cs.stargate.gatekeeper.dataexport.VirtualFileStatus;
import edu.arizona.cs.stargate.gatekeeper.recipe.RecipeChunk;
import java.util.Collection;

/**
 *
 * @author iychoi
 */
public abstract class AFileSystemRestfulAPI {
    public static final String BASE_PATH = "/filesystem";
    public static final String VIRTUAL_FILE_STATUS_PATH = "/vfstatus";
    public static final String FS_BLOCK_SIZE_PATH = "/blocksize";
    public static final String LOCAL_CLUSTER_PATH = "/localcluster";
    public static final String CHUNK_DATA_PATH = "/chunkdata";
    
    public abstract Collection<VirtualFileStatus> getAllVirtualFileStatus() throws Exception;

    public abstract long getBlockSize() throws Exception;

    public abstract Cluster getLocalCluster() throws Exception;

    public abstract byte[] readChunkData(String clusterName, String virtualPath, long offset, long size) throws Exception;
}
