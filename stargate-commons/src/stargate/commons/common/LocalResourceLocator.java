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

package stargate.commons.common;

import java.io.File;

/**
 *
 * @author iychoi
 */
public class LocalResourceLocator {
    
    public static final String DEFAULT_RESOURCE_LOCATION = ".";
    private File resourceBase;
    
    public LocalResourceLocator() {
        this.resourceBase = new File(DEFAULT_RESOURCE_LOCATION);
    }
    
    public LocalResourceLocator(File f) {
        this.resourceBase = f;
    }
    
    public File getResourceBase() {
        return this.resourceBase;
    }
    
    public File getResourceLocation(String f) {
        if(f.startsWith("/")) {
            return new File(f);
        } else {
            return new File(this.resourceBase, f);
        }
    }
}
