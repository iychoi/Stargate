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

import java.io.IOException;
import java.util.Formatter;

/**
 *
 * @author iychoi
 */
public class DataFormatter {

    public static String toHTMLFormat(Object obj) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public static String toJSONFormat(Object obj) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return serializer.toJson(obj);
    }
    
    public static String toHexString(byte[] arr) {
        Formatter formatter = new Formatter();
        for (byte b : arr) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
    
    public static byte[] hexToBytes(String hex) {
        return hexToBytes(hex.toCharArray());
    }
    
    public static byte[] hexToBytes(char[] hex) {
        byte[] raw = new byte[hex.length / 2];
        for (int src = 0, dst = 0; dst < raw.length; ++dst) {
            int hi = Character.digit(hex[src++], 16);
            int lo = Character.digit(hex[src++], 16);
            if ((hi < 0) || (lo < 0)) {
                throw new IllegalArgumentException();
            }
            raw[dst] = (byte) (hi << 4 | lo);
        }
        return raw;
    }
}
