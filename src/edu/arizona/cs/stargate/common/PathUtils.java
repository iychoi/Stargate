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

package edu.arizona.cs.stargate.common;

import edu.arizona.cs.stargate.gatekeeper.dataexport.VirtualFileStatus;

/**
 *
 * @author iychoi
 */
public class PathUtils {
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
    
    public static String getPath(VirtualFileStatus status) {
        return getPath(status.getClusterName(), status.getVirtualPath());
    }
    
    public static String getPath(String clusterName, String virtualPath) {
        String path = "/";
        if(clusterName != null && !clusterName.isEmpty()) {
            path += clusterName;
        }
        
        if(virtualPath != null && !virtualPath.isEmpty()) {
            if(virtualPath.startsWith("/")) {
                path += virtualPath;
            } else {
                path += "/" + virtualPath;
            }
        }
        
        if(!path.equals("/") && path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        } else {
            return path;
        }
    }
}
