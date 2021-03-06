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

package stargate.commons.restful;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author iychoi
 */
public class WebParamBuilder {
    private String resourceURL;
    private Map<String, String> params = new HashMap<String, String>();
    
    public WebParamBuilder(String contentURL) {
        this.resourceURL = contentURL;
    }
    
    public void addParam(String key, String value) {
        this.params.put(key, value);
    }
    
    public void addParam(String key, int value) {
        this.params.put(key, Integer.toString(value));
    }
    
    public void addParam(String key, long value) {
        this.params.put(key, Long.toString(value));
    }
    
    public void removeParam(String key) {
        this.params.remove(key);
    }
    
    public String build() {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<String, String>> entrySet = this.params.entrySet();
        for(Map.Entry<String, String> entry : entrySet) {
            if(sb.length() != 0) {
                sb.append("&");
            }
            
            if(entry.getValue() == null) {
                sb.append(entry.getKey());
            } else {
                sb.append(entry.getKey()).append("=").append(entry.getValue());
            }
        }
        return this.resourceURL + "?" + sb.toString();
    }
}
