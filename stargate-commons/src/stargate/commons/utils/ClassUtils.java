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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 *
 * @author iychoi
 */
public class ClassUtils {
    
    public static Class findClass(String className) throws ClassNotFoundException {
        return findClass(className, null);
    }
    
    public static Class findClass(String className, String[] lookupPaths) throws ClassNotFoundException {
        if(className == null || className.isEmpty()) {
            throw new IllegalArgumentException("className is not given");
        }
        
        Class clazz = null;
        
        // check whether the given className is full package path
        try {
            clazz = Class.forName(className);
        } catch(ClassNotFoundException ex) {
        }

        // if the given className is not a full package path
        if(clazz == null) {
            if(lookupPaths != null) {
                for(String pkg : lookupPaths) {
                    String newClassName = pkg + "." + className;
                    try {
                        clazz = Class.forName(newClassName);
                    } catch(ClassNotFoundException ex) {
                    }

                    if(clazz != null) {
                        break;
                    }
                }
            }
        }

        if(clazz == null) {
            throw new ClassNotFoundException("Class was not found : " + className);
        }
        
        return clazz;
    }
    
    public static Object getClassInstance(Class clazz) throws InstantiationException, IllegalAccessException {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        
        return clazz.newInstance();
    }
    
    public static void invokeMain(Class clazz, String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        
        Method method = null;

        try {
            // find main function
            Class[] argTypes = new Class[] { String[].class };
            method = clazz.getDeclaredMethod("main", argTypes);
        } catch(NoSuchMethodException ex) {
            throw new NoSuchMethodException("main function was not found in " + clazz.getName() + " class");
        }
        try {
            method.invoke(null, (Object)args);
        } catch (IllegalAccessException ex) {
            throw ex;
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (InvocationTargetException ex) {
            throw ex;
        }
    }
}
