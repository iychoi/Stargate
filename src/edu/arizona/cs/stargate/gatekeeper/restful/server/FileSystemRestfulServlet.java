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

package edu.arizona.cs.stargate.gatekeeper.restful.server;

import com.google.inject.Singleton;
import edu.arizona.cs.stargate.gatekeeper.restful.api.AFileSystemRestfulAPI;
import java.net.URI;
import javax.ws.rs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
@Path(AFileSystemRestfulAPI.BASE_PATH)
@Singleton
public class FileSystemRestfulServlet {
    
    private static final Log LOG = LogFactory.getLog(FileSystemRestfulServlet.class);
            
    
    private String[] splitPath(URI path) {
        String[] paths = new String[2];
        
        String uriPath = path.getPath();
        int startIdx = 0;
        if(uriPath.startsWith("/")) {
            startIdx++;
        }
        
        int endIdx = uriPath.indexOf("/", startIdx);
        if(endIdx > 0) {
            // cluster
            paths[0] = uriPath.substring(startIdx, endIdx);
            // vpath
            paths[1] = uriPath.substring(endIdx);
        } else {
            // cluster
            paths[0] = "local";
            // vpath
            paths[1] = "/";
        }
        
        return paths;
    }
    
    private String extractClusterName(URI path) {
        String[] splitPath = splitPath(path);
        return splitPath[0];
    }
    
    private String extractVPath(URI path) {
        String[] splitPath = splitPath(path);
        return splitPath[1];
    }
}
