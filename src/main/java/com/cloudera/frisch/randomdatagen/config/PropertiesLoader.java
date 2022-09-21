package com.cloudera.frisch.randomdatagen.config;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


@Component
public class PropertiesLoader {

    @Autowired
    private static SpringConfig springConfig;

    private PropertiesLoader() { throw new IllegalStateException("Could not initialize this class"); }

    private static final Logger logger = Logger.getLogger(PropertiesLoader.class);
    private static final Properties properties = loadProperties();

    private static Properties loadProperties() {
        // Load config file
        java.util.Properties properties = new java.util.Properties();

        String pathToApplicationProperties = "src/main/resources/application.properties";
        if(springConfig.activeProfile.equalsIgnoreCase("cdp")) {
            logger.info("Detected to be in cdp profile, so will load service.properties file");
            pathToApplicationProperties = "service.properties";
        }

        try {
            logger.info("Reading properties from file : " + pathToApplicationProperties);
            FileInputStream fileInputStream = new FileInputStream(pathToApplicationProperties);
            properties.load(fileInputStream);
        } catch (IOException e) {
            logger.error("Property file not found !", e);
        }

        autoDiscover();

        return properties;
    }

    public static void setProperty(String key, String value){
        try {
            properties.setProperty(key, value);
        } catch (Exception e) {
            logger.error("Cannot set property: " + key + " to value: " + value);
        }
    }

    public static String getProperty(String key) {
        String property = "null";
        try {
             property = properties.getProperty(key);
            if(property.length() > 1 && property.substring(0,2).equalsIgnoreCase("#{")) {
                property = PropertiesLoader.getProperty(property.substring(2,property.length()-1));
            }
        } catch (Exception e) {
            logger.warn("Could not get property : " + key + " due to following error: ", e);
        }
        logger.debug("For key: " + key + " returning property: " + property);
        return property;
    }


    // TODO: Foreach sink, a copy of the PropertiesLoader should be set and send to the sink,
    //  so an API call can override some variables
    public static Properties getPropertiesCopy() {
        return new Properties(properties);
    }

    // TODO: At startup make Auto-Discovery: all important properties should be checked foreach service
    /*
    - If properties for a service are not set (ex: hdfs.uri) => Look for config file's location property
    - If this is empty or file does not exists => WARNING: You should provide info on API call to use this sink
    - Otherwise, load the file and set the required property (hdfs.uri for example)
     */
    private static void autoDiscover() {
        logger.info("Starting auto-discover of properties after load of properties file");

        if(getProperty("hdfs.uri").isEmpty() && !getProperty("hadoop.core.site.path").isEmpty() && !getProperty("hadoop.hdfs.site.path").isEmpty()) {
            logger.info("Going to auto-discover hdfs.uri");
        }

        if(getProperty("hbase.zookeeper.quorum").isEmpty() && !getProperty("hadoop.hbase.site.path").isEmpty()) {
            logger.info("Going to auto-discover hbase.zookeeper.quorum");
        }

        if(getProperty("hbase.zookeeper.port").isEmpty() && !getProperty("hadoop.hbase.site.path").isEmpty()) {
            logger.info("Going to auto-discover hbase.zookeeper.port");
        }

        if(getProperty("hbase.zookeeper.znode").isEmpty() && !getProperty("hadoop.hbase.site.path").isEmpty()) {
            logger.info("Going to auto-discover hbase.zookeeper.znode");
        }

        if(getProperty("ozone.service.id").isEmpty() && !getProperty("hadoop.ozone.site.path").isEmpty()) {
            logger.info("Going to auto-discover ozone.service.id");
        }

        if(getProperty("hive.zookeeper.quorum").isEmpty() && !getProperty("hadoop.hive.site.path").isEmpty()) {
            logger.info("Going to auto-discover hive.zookeeper.quorum");
        }

        if(getProperty("hive.zookeeper.znode").isEmpty() && !getProperty("hadoop.hive.site.path").isEmpty()) {
            logger.info("Going to auto-discover hive.zookeeper.znode");
        }

        if(getProperty("solr.server.url").isEmpty() && !getProperty("solr.env.path").isEmpty()) {
            logger.info("Going to auto-discover solr.server.url");
        }

        if(getProperty("solr.server.port").isEmpty() && !getProperty("solr.env.path").isEmpty()) {
            logger.info("Going to auto-discover solr.server.port");
        }

        if(getProperty("kafka.brokers").isEmpty() && !getProperty("kafka.conf.client.path").isEmpty() && !getProperty("kafka.conf.cluster.path").isEmpty()) {
            logger.info("Going to auto-discover kafka.brokers");
        }

        if(getProperty("kafka.security.protocol").isEmpty() && !getProperty("kafka.conf.client.path").isEmpty() && !getProperty("kafka.conf.cluster.path").isEmpty()) {
            logger.info("Going to auto-discover kafka.security.protocol");
        }

        if(getProperty("schema.registry.url").isEmpty() && !getProperty("schema.registry.conf.path").isEmpty()) {
            logger.info("Going to auto-discover schema.registry.url");
        }

        if(getProperty("kudu.master.server").isEmpty() && !getProperty("kudu.conf.path").isEmpty()) {
            logger.info("Going to auto-discover kudu.master.server");
        }

    }

}
