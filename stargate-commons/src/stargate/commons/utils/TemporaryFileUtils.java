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

package stargate.commons.utils;

import java.io.File;

/**
 *
 * @author iychoi
 */
public class TemporaryFileUtils {
    private static final String TEMP_PATH_ROOT = "/tmp/stargate/";
    
    public static File getTempRoot() {
        return new File(TEMP_PATH_ROOT);
    }
    
    public static File getTempFile(String name) {
        return new File(TEMP_PATH_ROOT, name);
    }
    
    public static boolean prepareTempRoot() {
        File f = new File(TEMP_PATH_ROOT);
        if(!f.exists()) {
            return f.mkdirs();
        } else {
            clearTempRoot();
            return true;
        }
    }
    
    public static void clearTempRoot() {
        File f = new File(TEMP_PATH_ROOT);
        if(f.exists()) {
            File[] files = f.listFiles();
            for(File file : files) {
                remoteRecursively(file);
            }
        }
    }
    
    public static void remoteRecursively(File f) {
        if(f.isFile()) {
            f.delete();
        } else if(f.isDirectory()) {
            File[] files = f.listFiles();
            for(File file : files) {
                remoteRecursively(file);
            }
        }
    }
}
