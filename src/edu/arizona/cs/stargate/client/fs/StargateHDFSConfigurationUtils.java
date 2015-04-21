package edu.arizona.cs.stargate.client.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class StargateHDFSConfigurationUtils {
    
    public static final Log LOG = LogFactory.getLog(StargateHDFSConfigurationUtils.class);
    
    public static final String CONFIG_GATEKEEPER_HOSTS_KEY = "fs.stargate.gatekeeper.hosts";
    
    public static String[] getGateKeeperHosts(Configuration conf) {
        return conf.getStrings(CONFIG_GATEKEEPER_HOSTS_KEY, null);
    }
    
    public static void setGateKeeperHosts(Configuration conf, String[] hosts) {
        conf.setStrings(CONFIG_GATEKEEPER_HOSTS_KEY, hosts);
    }
}
