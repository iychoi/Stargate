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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.json.JSONObject;

/**
 *
 * @author iychoi
 */
public abstract class AJsonSerializable {
    public void fromJson(String json) {
        fromJsonObj(new JSONObject(json));
    }
    
    public String toJson() {
        return toJsonObj().toString();
    }
    
    public void toLocalFile(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        if(file.getParentFile() != null) {
            if(!file.getParentFile().exists()) {
                boolean mkdirs = file.getParentFile().mkdirs();
                if(!mkdirs) {
                    throw new IOException("failed to create parent directory : " + file.getParent());
                }
            }
        }
        
        BufferedWriter bwriter = new BufferedWriter(new FileWriter(file));
        
        try {
            bwriter.write(toJson());
        } finally {
            bwriter.close();
        }
    }
    
    public void fromLocalFile(File file) throws FileNotFoundException, IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        if(!file.exists() || !file.isFile()) {
            throw new FileNotFoundException("file not found");
        }
        
        BufferedReader breader = new BufferedReader(new FileReader(file));
        
        try {
            StringBuilder sb = new StringBuilder();
            String line = breader.readLine();

            while(line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = breader.readLine();
            }
            
            fromJson(sb.toString());
        } finally {
            breader.close();
        }
    }
    
    public abstract void fromJsonObj(JSONObject jsonobj);
    public abstract JSONObject toJsonObj();
}
