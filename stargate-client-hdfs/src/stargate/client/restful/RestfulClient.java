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
package stargate.client.restful;

import com.sun.jersey.api.client.AsyncWebResource;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

/**
 *
 * @author iychoi
 */
public class RestfulClient {
    
    private static final Log LOG = LogFactory.getLog(RestfulClient.class);
    
    private URI serviceURL;
    private ClientConfig httpClientConfig;
    private Client httpClient;
    
    public RestfulClient(URI serviceURL, int threadPoolSize) {
        if(serviceURL == null) {
            throw new IllegalArgumentException("serviceURL is null");
        }
        
        if(threadPoolSize <= 0) {
            throw new IllegalArgumentException("threadPoolSize is invalid");
        }
        
        this.serviceURL = serviceURL;
        
        this.httpClientConfig = new DefaultClientConfig();
        this.httpClientConfig.getClasses().add(JacksonJsonProvider.class);
        this.httpClientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        this.httpClientConfig.getProperties().put(ClientConfig.PROPERTY_THREADPOOL_SIZE, threadPoolSize);

        this.httpClient = Client.create(this.httpClientConfig);
    }
    
    public void close() {
        this.httpClient.getExecutorService().shutdownNow();
        this.httpClient.destroy();
    }
    
    public Object post(String path, Object request, GenericType<?> generic) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(request == null) {
            throw new IllegalArgumentException("request is null");
        }
        
        if(generic == null) {
            throw new IllegalArgumentException("generic is null");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        Future<ClientResponse> future = (Future<ClientResponse>) webResource.accept("application/json").type("application/json").post(ClientResponse.class, request);
        
        // wait for completion
        try {
            ClientResponse response = future.get();
            if(response.getStatus() != 200) {
                throw new IOException("HTTP error code : " + response.getStatus());
            }

            return response.getEntity(generic);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
    
    public Object get(String path, GenericType<?> generic) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(generic == null) {
            throw new IllegalArgumentException("generic is null");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        Future<ClientResponse> future = (Future<ClientResponse>) webResource.accept("application/json").type("application/json").get(ClientResponse.class);
        
        // wait for completion
        try {
            ClientResponse response = future.get();
            if(response.getStatus() != 200) {
                throw new IOException("HTTP error code : " + response.getStatus());
            }

            return response.getEntity(generic);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
    
    public Object delete(String path, GenericType<?> generic) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(generic == null) {
            throw new IllegalArgumentException("generic is null");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        Future<ClientResponse> future = (Future<ClientResponse>) webResource.accept("application/json").type("application/json").delete(ClientResponse.class);
        
        // wait for completion
        try {
            ClientResponse response = future.get();
            if(response.getStatus() != 200) {
                throw new IOException("HTTP error code : " + response.getStatus());
            }

            return response.getEntity(generic);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
    
    public InputStream download(String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        URI requestURL = this.serviceURL.resolve(path);
        
        AsyncWebResource webResource = this.httpClient.asyncResource(requestURL);
        Future<ClientResponse> future = (Future<ClientResponse>) webResource.accept("application/octet-stream").type("application/json").get(ClientResponse.class);
        
        // wait for completition
        try {
            ClientResponse response = future.get();
            if(response.getStatus() != 200) {
                throw new IOException("HTTP error code : " + response.getStatus());
            }

            return response.getEntityInputStream();
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        } catch (ExecutionException ex) {
            throw new IOException(ex);
        }
    }
}
