package com.cloudera.frisch.randomdatagen.config;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

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
            FileInputStream fileInputStream = new FileInputStream(pathToApplicationProperties);
            properties.load(fileInputStream);
        } catch (IOException e) {
            logger.error("Property file not found !", e);
        }
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
            if(property.length() > 1 && property.substring(0,2).equalsIgnoreCase("${")) {
                property = PropertiesLoader.getProperty(property.substring(2,property.length()-1));
            }
        } catch (Exception e) {
            logger.warn("Could not get property : " + key + " due to following error: ", e);
        }
        logger.debug("For key: " + key + " returning property: " + property);
        return property;
    }

    // TODO: At startup make Auto-Discovery: all important properties should be checked foreach service
    /*
    - If properties for a service are not set (ex: hdfs.uri) => Look for config file's location property
    - If this is empty or file does not exists => WARNING: You should provide info on API call to use this sink
    - Otherwise, load the file and set the required property (hdfs.uri for example)
     */

    // TODO: Foreach sink, a copy of the PropertiesLoader should be set and send to the sink,
    //  so an API call can override some variables
}
