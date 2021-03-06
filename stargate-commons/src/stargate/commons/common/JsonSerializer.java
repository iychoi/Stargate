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
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 *
 * @author iychoi
 */
public class JsonSerializer {
    
    private ObjectMapper mapper;
            
    public JsonSerializer() {
        this.mapper = new ObjectMapper();
    }
    
    public JsonSerializer(boolean prettyformat) {
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, prettyformat);
    }
    
    public String toJson(Object obj) throws IOException {
        StringWriter writer = new StringWriter();
        this.mapper.writeValue(writer, obj);
        return writer.getBuffer().toString();
    }
    
    public void toJsonFile(File f, Object obj) throws IOException {
        this.mapper.writeValue(f, obj);
    }
    
    public Object fromJson(String json, Class<?> cls) throws IOException {
        if(json == null) {
            return null;
        }
        StringReader reader = new StringReader(json);
        return this.mapper.readValue(reader, cls);
    }
    
    public Object fromJsonFile(File f, Class<?> cls) throws IOException {
        return this.mapper.readValue(f, cls);
    }
}
