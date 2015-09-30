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

package edu.arizona.cs.stargate.common.utils;

import java.net.URI;

/**
 *
 * @author iychoi
 */
public class PathUtils {
    public static String getFileName(URI path) {
        return getFileName(path.normalize().getPath());
    }
    
    public static String getFileName(String path) {
        int idx = path.lastIndexOf("/");
        String filename = path;
        if(idx >= 0) {
            filename = path.substring(idx+1, path.length());
        }
        return filename;
    }
    
    public static String getParent(URI path) {
        return getParent(path.normalize().getPath());
    }
    
    public static String getParent(String path) {
        // check root
        if(path.equals("/")) {
            return null;
        }
        
        int lastIdx = path.lastIndexOf("/");
        if(lastIdx > 0) {
            return path.substring(0, lastIdx);
        } else {
            return "/";
        }
    }
    
    public static String concatPath(String path1, String path2) {
        StringBuffer sb = new StringBuffer();
        sb.append("/");
        
        if(path1 != null && !path1.isEmpty()) {
            if(path1.startsWith("/")) {
                sb.append(path1.substring(1, path1.length()));
            } else {
                sb.append(path1);
            }
            
            if(!path1.endsWith("/")) {
                sb.append("/");
            }
        }
        
        if(path2 != null && !path2.isEmpty()) {
            if(path2.startsWith("/")) {
                sb.append(path2.substring(1, path2.length()));
            } else {
                sb.append(path2);
            }
        }
        
        return sb.toString();
    }
}
