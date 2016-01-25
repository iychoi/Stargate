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
package stargate.commons.temporalstorage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import stargate.commons.drivers.ADriver;

/**
 *
 * @author iychoi
 */
public abstract class APersistentTemporalStorageDriver extends ADriver {
    public abstract TemporalFileMetadata getMetadata(URI path) throws IOException, FileNotFoundException;
    public abstract boolean exists(URI path) throws IOException;
    public abstract boolean isDirectory(URI path) throws IOException, FileNotFoundException;
    public abstract boolean isFile(URI path) throws IOException, FileNotFoundException;
    
    public abstract Collection<URI> listDirectory(URI path) throws IOException, FileNotFoundException;
    public abstract Collection<TemporalFileMetadata> listDirectoryWithMetadata(URI path) throws IOException, FileNotFoundException;
    
    public abstract boolean makeDirs(URI path) throws IOException;
    public abstract boolean remove(URI path) throws IOException, FileNotFoundException;
    public abstract boolean removeDir(URI path, boolean recursive) throws IOException, FileNotFoundException;
    
    public abstract InputStream getInputStream(URI path) throws IOException, FileNotFoundException;
    public abstract OutputStream getOutputStream(URI path) throws IOException;
}
