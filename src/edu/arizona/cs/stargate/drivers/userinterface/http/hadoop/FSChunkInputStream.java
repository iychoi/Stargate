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

package edu.arizona.cs.stargate.drivers.userinterface.http.hadoop;

import edu.arizona.cs.stargate.recipe.Recipe;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPChunkInputStream;
import edu.arizona.cs.stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/**
 *
 * @author iychoi
 */
public class FSChunkInputStream extends HTTPChunkInputStream implements Seekable, PositionedReadable {

    private static final Log LOG = LogFactory.getLog(FSChunkInputStream.class);
    
    public FSChunkInputStream(HTTPUserInterfaceClient client, Recipe recipe) {
        super(client, recipe);
    }
    
    @Override
    public synchronized void seek(long l) throws IOException {
        super.seek(l);
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return super.getPos();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public synchronized int read(long pos, byte[] bytes, int off, int len) throws IOException {
        super.seek(pos);
        if(super.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        int available = super.available();
        
        if(available > 0) {
            return super.read(bytes, off, Math.min(len, available));
        } else {
            return super.read(bytes, off, len);
        }
    }

    @Override
    public synchronized void readFully(long pos, byte[] bytes, int off, int len) throws IOException {
        super.seek(pos);
        if(super.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        super.read(bytes, off, len);
    }

    @Override
    public synchronized void readFully(long pos, byte[] bytes) throws IOException {
        super.seek(pos);
        if(super.getPos() != pos) {
            throw new IOException("Cannot find position : " + pos);
        }
        super.read(bytes, 0, bytes.length);
    }
}
