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
package stargate.commons.userinterface;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import stargate.commons.cluster.RemoteCluster;
import stargate.commons.recipe.DataObjectMetadata;
import stargate.commons.recipe.DataObjectPath;
import stargate.commons.recipe.Recipe;
import stargate.commons.volume.Directory;

/**
 *
 * @author iychoi
 */
public abstract class AUserInterfaceAPI {
    public abstract boolean isLive();
    
    public abstract RemoteCluster getCluster() throws IOException;

    public abstract Directory getDirectory(DataObjectPath path) throws IOException;
    public abstract DataObjectMetadata getDataObjectMetadata(DataObjectPath path) throws IOException;
    public abstract Recipe getRecipe(DataObjectPath path) throws IOException;
    public abstract Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectPath path) throws IOException;
    public abstract InputStream getDataChunk(String clusterName, String hash) throws IOException;
    public abstract URI getLocalResourcePath(DataObjectPath path) throws IOException;
}
