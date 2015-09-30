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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    
    public void toJsonConfiguration(Configuration conf, String key, Object obj) throws IOException {
        String jsonString = toJson(obj);
        
        conf.set(key, jsonString);
    }
    
    public void toJsonFile(File f, Object obj) throws IOException {
        this.mapper.writeValue(f, obj);
    }
    
    public void toJsonFile(FileSystem fs, Path file, Object obj) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream ostream = fs.create(file, true, 64 * 1024);
        this.mapper.writeValue(ostream, obj);
        ostream.close();
    }
    
    public Object fromJson(String json, Class<?> cls) throws IOException {
        if(json == null) {
            return null;
        }
        StringReader reader = new StringReader(json);
        return this.mapper.readValue(reader, cls);
    }
    
    public Object fromJsonConfiguration(Configuration conf, String key, Class<?> cls) throws IOException {
        String jsonString = conf.get(key);
        
        if(jsonString == null) {
            return null;
        }
        
        return fromJson(jsonString, cls);
    }
    
    public Object fromJsonFile(File f, Class<?> cls) throws IOException {
        return this.mapper.readValue(f, cls);
    }
    
    public Object fromJsonFile(FileSystem fs, Path file, Class<?> cls) throws IOException {
        DataInputStream istream = fs.open(file);
        Object obj = this.mapper.readValue(istream, cls);
        
        istream.close();
        return obj;
    }
}
