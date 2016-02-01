/*
 * The MIT License
 *
 * Copyright 2016 iychoi.
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
package stargate.server.volume;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class InterceptableInputStream extends InputStream {
    private static final Log LOG = LogFactory.getLog(InterceptableInputStream.class);
    
    private InputStream inputStream;
    private IInterceptableInputStreamHandler handler;
    
    public InterceptableInputStream(InputStream inputStream, IInterceptableInputStreamHandler handler) {
        if(inputStream == null) {
            throw new IllegalArgumentException("inputStream is null");
        }
        
        if(handler == null) {
            throw new IllegalArgumentException("handler is null");
        }
        
        initialize(inputStream, handler);
    }

    private void initialize(InputStream inputStream, IInterceptableInputStreamHandler handler) {
        if(inputStream == null) {
            throw new IllegalArgumentException("inputStream is null");
        }
        
        if(handler == null) {
            throw new IllegalArgumentException("handler is null");
        }
        
        this.inputStream = inputStream;
        this.handler = handler;
    }

    @Override
    public int read() throws IOException {
        int read = this.inputStream.read();
        this.handler.onRead(read);
        return read;
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        int read = this.inputStream.read(buffer);
        this.handler.onRead(read, buffer);
        return read;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) throws IOException {
        int read = this.inputStream.read(buffer, offset, len);
        this.handler.onRead(read, buffer, offset, len);
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        byte[] buffer = new byte[1024 * 4];
        long left = n;
        while(left > 0) {
            int toRead = (int) Math.min(left, 1024*4);
            int read = this.inputStream.read(buffer, 0, toRead);
            this.handler.onRead(read, buffer, 0, toRead);
            if(read == 0) {
                break;
            }
            left -= read;
        }
        return n - left;
    }

    @Override
    public int available() throws IOException {
        return this.inputStream.available();
    }

    @Override
    public synchronized void close() throws IOException {
        // consume all
        byte[] buffer = new byte[1024 * 4];
        while(true) {
            int toRead = 1024 * 4;
            int read = this.inputStream.read(buffer, 0, toRead);
            this.handler.onRead(read, buffer, 0, toRead);
            if(read == 0) {
                break;
            }
        }
        
        this.inputStream.close();
    }

    @Override
    public synchronized boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
