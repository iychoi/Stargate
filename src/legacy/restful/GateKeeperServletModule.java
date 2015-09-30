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

package legacy.restful;
/*
import edu.arizona.cs.stargate.gatekeeper.restful.server.GateKeeperRestfulServlet;
import legacy.restful.DataExportManagerRestfulServlet;
import legacy.restful.RecipeManagerRestfulServlet;
import legacy.restful.InterClusterDataTransferRestfulServlet;
import legacy.restful.ClusterManagerRestfulServlet;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import legacy.restful.FileSystemRestfulServlet;
import java.util.HashMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.eclipse.jetty.servlet.DefaultServlet;
*/
/**
 *
 * @author iychoi
 */
public class GateKeeperServletModule /*extends ServletModule*/ {
    /*
    @Override
    protected void configureServlets() {
        bind(DefaultServlet.class).in(Singleton.class);
        bind(GateKeeperRestfulServlet.class).in(Singleton.class); 
        bind(ClusterManagerRestfulServlet.class).in(Singleton.class); 
        bind(DataExportManagerRestfulServlet.class).in(Singleton.class);
        bind(RecipeManagerRestfulServlet.class).in(Singleton.class);
        bind(InterClusterDataTransferRestfulServlet.class).in(Singleton.class);
        bind(FileSystemRestfulServlet.class).in(Singleton.class);

        bind(MessageBodyReader.class).to(JacksonJsonProvider.class); 
        bind(MessageBodyWriter.class).to(JacksonJsonProvider.class); 
 
        HashMap <String, String> options = new HashMap<>();
        options.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
        serve("/*").with(GuiceContainer.class, options);
    }
    */
}
