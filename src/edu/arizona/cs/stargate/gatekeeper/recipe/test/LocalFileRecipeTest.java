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

package edu.arizona.cs.stargate.gatekeeper.recipe.test;

import edu.arizona.cs.stargate.common.JsonSerializer;
import edu.arizona.cs.stargate.gatekeeper.recipe.FixedSizeLocalFileRecipeGenerator;
import edu.arizona.cs.stargate.gatekeeper.recipe.LocalRecipe;
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author iychoi
 */
public class LocalFileRecipeTest {
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            String filename = "./libs/";
            if(args.length != 0) {
                filename = args[0];
            }
            
            File givenFile = new File("./", filename);
            if(!givenFile.exists()) {
                System.err.println("file not exists");
                return;
            }
            
            List<File> files = new ArrayList<File>();
            FilenameFilter filter = new FilenameFilter() {

                @Override
                public boolean accept(File file, String string) {
                    if(string.endsWith(".recipe")) {
                        return false;
                    }
                    return true;
                }
                
            };
            if(givenFile.isDirectory()) {
                File[] listFiles = givenFile.listFiles(filter);
                for(File f : listFiles) {
                    files.add(f);
                }
            } else {
                files.add(givenFile);
            }
            
            JsonSerializer serializer = new JsonSerializer(true);
            FixedSizeLocalFileRecipeGenerator gen = new FixedSizeLocalFileRecipeGenerator(1024*1024);
            for(File f : files) {
                //Recipe recipe = gen.generateRecipe(f, "SHA-1");
                LocalRecipe recipe = gen.generateRecipe(f, "SHA-1");
                gen.hashRecipe(recipe);
                File recipeFile = new File(f.getAbsoluteFile().getCanonicalPath() + ".recipe");
                serializer.toJsonFile(recipeFile, recipe);
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
