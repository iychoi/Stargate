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

package edu.arizona.cs.stargate.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class IPUtils {
    
    private static final Log LOG = LogFactory.getLog(IPUtils.class);
    
    private static String cached_public_ip;
    private static List<String> cached_host_addr = new ArrayList<String>();
    private static List<String> cached_ip_addr = new ArrayList<String>();
    
    
    private static final String IPADDRESS_PATTERN = "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
    
    private static final String DOMAINNAME_PATTERN = "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}$";
    
    private static Pattern IP_pattern = Pattern.compile(IPADDRESS_PATTERN);
    private static Pattern Domain_pattern = Pattern.compile(DOMAINNAME_PATTERN);
    
    private static boolean isProperHostAddress(String addr) {
        if(addr == null || addr.isEmpty()) {
            return false;
        }
        
        if(addr.equalsIgnoreCase("localhost")) {
            return false;
        } else if(addr.equalsIgnoreCase("ip6-localhost")) {
            return false;
        } else if(addr.equalsIgnoreCase("127.0.0.1")) {
            return false;
        } else if(addr.indexOf(":") > 0) {
            return false;
        }
        return true;
    }
    
    public static Collection<String> getHostAddress() {
        if(!cached_host_addr.isEmpty()) {
            return Collections.unmodifiableCollection(cached_host_addr);
        } else {
            try {
                Enumeration e = NetworkInterface.getNetworkInterfaces();
                while (e.hasMoreElements()) {
                    NetworkInterface n = (NetworkInterface) e.nextElement();
                    Enumeration ee = n.getInetAddresses();
                    while (ee.hasMoreElements()) {
                        InetAddress i = (InetAddress) ee.nextElement();

                        String hostAddress = i.getHostAddress();
                        if(isProperHostAddress(hostAddress)) {
                            if(!cached_host_addr.contains(hostAddress)) {
                                cached_host_addr.add(hostAddress);
                            }
                        }

                        String hostName = i.getHostName();
                        if(isProperHostAddress(hostName)) {
                            if(!cached_host_addr.contains(hostName)) {
                                cached_host_addr.add(hostName);
                            }
                        }

                        String canonicalHostName = i.getCanonicalHostName();
                        if(isProperHostAddress(canonicalHostName)) {
                            if(!cached_host_addr.contains(canonicalHostName)) {
                                cached_host_addr.add(canonicalHostName);
                            }
                        }
                    }
                }
            } catch (SocketException ex) {
                LOG.error(ex);
            }
            
            return Collections.unmodifiableCollection(cached_host_addr);
        }
    }
    
    public static Collection<String> getIPAddress() {
        if(!cached_ip_addr.isEmpty()) {
            return Collections.unmodifiableCollection(cached_ip_addr);
        } else {
            try {
                Enumeration e = NetworkInterface.getNetworkInterfaces();
                while (e.hasMoreElements()) {
                    NetworkInterface n = (NetworkInterface) e.nextElement();
                    Enumeration ee = n.getInetAddresses();
                    while (ee.hasMoreElements()) {
                        InetAddress i = (InetAddress) ee.nextElement();
                        if(!i.isLoopbackAddress()) {
                            String hostAddress = i.getHostAddress();
                            cached_ip_addr.add(hostAddress);
                        }
                    }
                }
            } catch (SocketException ex) {
                LOG.error(ex);
            }

            return Collections.unmodifiableCollection(cached_ip_addr);
        }
    }
    
    public static String getPublicIPAddress() {
        if(cached_public_ip != null && !cached_public_ip.isEmpty()) {
            return cached_public_ip;
        } else {
            try {
                URL whatismyip = new URL("http://checkip.amazonaws.com");
                BufferedReader in = null;
                try {
                    in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
                    String ip = in.readLine();
                    
                    // cache
                    cached_public_ip = ip;

                    return ip;
                } catch (IOException ex) {
                    LOG.error(ex);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                            LOG.error(e);
                        }
                    }
                }
            } catch (MalformedURLException ex) {
                LOG.error(ex);
            }
        }
        return null;
    }
    
    public static boolean isIPAddress(String address) {
        Matcher matcher = IP_pattern.matcher(address);
        return matcher.matches();
    }
    
    public static boolean isDomainName(String address) {
        Matcher matcher = Domain_pattern.matcher(address);
        return matcher.matches();
    }

    public static boolean isPublicIPAddress(String address) {
        Matcher matcher = IP_pattern.matcher(address);
        if(matcher.matches()) {
            String first = matcher.group(1);
            String second = matcher.group(2);
            
            int f = Integer.parseInt(first);
            int s = Integer.parseInt(second);
            
            if(f == 192 && s == 168) {
                return false;
            }
            
            if(f == 172 && s >= 16 && s <= 31) {
                return false;
            }
            
            if(f == 10) {
                return false;
            }
            
            return true;
        }
        
        return false;
    }
}
