package com.cloudera.frisch.randomdatagen.config;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {

    private PropertiesLoader() { throw new IllegalStateException("Could not initialize this class"); }

    private static final Logger logger = Logger.getLogger(PropertiesLoader.class);

    private static final Properties properties = loadProperties();

    private static Properties loadProperties() {
        // Load config file
        java.util.Properties properties = new java.util.Properties();

        try {
            FileInputStream fileInputStream = new FileInputStream("config.properties");
            properties.load(fileInputStream);
        } catch (IOException e) {
            logger.error("Property file not found !", e);
        }
        return properties;
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
        return property;
    }
}
