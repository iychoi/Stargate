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
package stargate.commons.recipe;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class DataObjectPath implements Comparable {

    private static final Log LOG = LogFactory.getLog(DataObjectPath.class);
    
    public static final String STARGATE_SCHEME = "sgt";

    private URI uri;
    
    public DataObjectPath() {
        
    }
    
    public DataObjectPath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is empty or null");
        }
        
        if(!path.startsWith(STARGATE_SCHEME + "://")) {
            if(path.startsWith("/")) {
                initialize(STARGATE_SCHEME + "://" + path.substring(1));
            } else {
                initialize(STARGATE_SCHEME + "://" + path);
            }
        } else {
            initialize(path);
        }
    }
    
    public DataObjectPath(String cluster, String parent, String child) {
        if(parent == null || parent.isEmpty()) {
            throw new IllegalArgumentException("parent is empty or null");
        }
        
        if(child == null || child.isEmpty()) {
            throw new IllegalArgumentException("child is empty or null");
        }
        
        initialize(cluster, parent, child);
    }
    
    public DataObjectPath(String cluster, String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is empty or null");
        }
        
        initialize(cluster, path);
    }
    
    public DataObjectPath(URI uri) {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        initialize(uri);
    }
    
    public DataObjectPath(DataObjectPath parent, String child) {
        if(parent == null) {
            throw new IllegalArgumentException("parent is null");
        }
        
        if(child == null || child.isEmpty()) {
            throw new IllegalArgumentException("child is empty or null");
        }
        
        initialize(parent, child);
    }
    
    private void initialize(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path is empty or null");
        }
        
        String uriScheme = null;
        String uriCluster = null;
        String uriPath = null;
        
        int start = 0;

        // parse uri scheme
        int colon = path.indexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("uri scheme is not given");
        }
        
        uriScheme = path.substring(0, colon);
        start = colon + 1;
        
        if(!uriScheme.equalsIgnoreCase(STARGATE_SCHEME)) {
            throw new IllegalArgumentException("uri scheme is not stargate scheme (" + uriScheme + " was given)");
        }

        // parse uri authority
        if (path.startsWith("//", start) && (path.length() - start > 2)) {
            // have authority
            int nextSlash = path.indexOf('/', start + 2);
            int authEnd;
            if (nextSlash != -1) {
                authEnd = nextSlash;
            } else {
                authEnd = path.length();
            }

            uriCluster = path.substring(start + 2, authEnd);
            start = authEnd;
        }
        
        // uri path
        if(start < path.length()) {
            uriPath = path.substring(start, path.length());
        } else {
            uriPath = "/";
        }
        
        this.uri = createPathUri(uriCluster, uriPath);
    }
    
    private void initialize(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        String scheme = uri.getScheme();
        if(scheme != null && !scheme.equalsIgnoreCase(STARGATE_SCHEME)) {
            throw new IllegalArgumentException("uri scheme is not stargate scheme (" + scheme + " was given)");
        }
        
        String authority = uri.getAuthority();
        if(authority == null || authority.isEmpty()) {
            throw new IllegalArgumentException("authority of given uri is empty or null");
        }
        
        this.uri = createPathUri(authority, uri.getPath());
    }
    
    private void initialize(String cluster, String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is empty or null");
        }
        
        this.uri = createPathUri(cluster, path);
    }
    
    private void initialize(String cluster, String parent, String child) {
        if (parent == null || parent.isEmpty()) {
            throw new IllegalArgumentException("parent is empty or null");
        }
        
        this.uri = createPathUri(cluster, parent);
        this.uri.resolve(normalizePath(child)).normalize();
    }
    
    private void initialize(DataObjectPath parent, String child) {
        if (parent == null) {
            throw new IllegalArgumentException("parent is null");
        }
        
        if (child == null || child.isEmpty()) {
            throw new IllegalArgumentException("child is empty or null");
        }
        
        this.uri = parent.uri.resolve(normalizePath(child)).normalize();
    }
    
    private URI createPathUri(String authority, String path) {
        try {
            URI uri = new URI(STARGATE_SCHEME, authority, normalizePath(path), null, null);
            return uri.normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    private String normalizePath(String path) {
        // replace all "//" and "\" to "/"
        path = path.replace("//", "/");
        path = path.replace("\\", "/");

        // trim trailing slash
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }
    
    @JsonProperty("uri")
    public URI toUri() {
        return this.uri;
    }
    
    @JsonProperty("uri")
    public void setUri(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        String scheme = uri.getScheme();
        if(scheme != null && !scheme.equalsIgnoreCase(STARGATE_SCHEME)) {
            throw new IllegalArgumentException("uri scheme is not stargate scheme (" + scheme + " was given)");
        }
        
        String authority = uri.getAuthority();
        if(authority == null || authority.isEmpty()) {
            throw new IllegalArgumentException("authority of given uri is empty or null");
        }
        
        this.uri = uri;
    }
    
    @JsonIgnore
    public String getClusterName() {
        return this.uri.getAuthority();
    }
    
    @JsonIgnore
    public String getName() {
        String path = this.uri.getPath();
        int slash = path.lastIndexOf('/');
        return path.substring(slash + 1, path.length());
    }
    
    @JsonIgnore
    public DataObjectPath getParent() {
        String path = this.uri.getPath();
        int lastSlash = path.lastIndexOf('/');
        
        // empty
        if (path.length() == 0) {
            return null;
        }
        
        // root
        if (path.length() == 1 && lastSlash == 0) {
            return null;
        }
        
        if (lastSlash == -1) {
            return new DataObjectPath(createPathUri(this.uri.getAuthority(), "."));
        } else if (lastSlash == 0) {
            return new DataObjectPath(createPathUri(this.uri.getAuthority(), "/"));
        } else {
            String parent = path.substring(0, lastSlash);
            return new DataObjectPath(createPathUri(this.uri.getAuthority(), parent));
        }
    }
    
    @JsonIgnore
    public boolean isRoot() {
        String cluster = this.uri.getAuthority();
        if(cluster == null || cluster.isEmpty()) {
            return true;
        }
        return false;
    }
    
    @JsonIgnore
    public boolean isClusterRoot() {
        if(isRoot()) {
            return false;
        }
        
        String path = this.uri.getPath();
        int lastSlash = path.lastIndexOf('/');
        
        // empty
        if (path.length() == 0) {
            return true;
        }
        
        // root
        if (path.length() == 1 && lastSlash == 0) {
            return true;
        }
        return false;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        String scheme = this.uri.getScheme();
        if (scheme != null) {
            sb.append(scheme);
            sb.append(":");
        }
        
        String authority = this.uri.getAuthority();
        if (authority != null) {
            sb.append("//");
            sb.append(authority);
        }
        
        String path = this.uri.getPath();
        if (path != null) {
            sb.append(path);
        }
        
        return sb.toString();
    }
    
    @JsonIgnore
    public String getPath() {
        return this.uri.getPath();
    }
    
    @Override
    @JsonIgnore
    public boolean equals(Object o) {
        if (!(o instanceof DataObjectPath))
            return false;
        
        DataObjectPath other = (DataObjectPath) o;
        return this.uri.equals(other.uri);
    }
    
    @Override
    @JsonIgnore
    public int hashCode() {
        return this.uri.hashCode();
    }
    
    @Override
    @JsonIgnore
    public int compareTo(Object o) {
        DataObjectPath other = (DataObjectPath) o;
        return this.uri.compareTo(other.uri);
    } 
}
