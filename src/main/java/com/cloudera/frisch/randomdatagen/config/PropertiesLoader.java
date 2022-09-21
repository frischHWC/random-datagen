package com.cloudera.frisch.randomdatagen.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


@Slf4j
@Component
public class PropertiesLoader {

    private SpringConfig springConfig;

    private Map<ApplicationConfigs, String> properties;

    @Autowired
    public PropertiesLoader(SpringConfig springConfig) {
        this.springConfig = springConfig;
        properties = new HashMap<>();

        // Load config file
        java.util.Properties propertiesAsProperties = new java.util.Properties();

        String pathToApplicationProperties = "src/main/resources/application.properties";
        if(springConfig.getActiveProfile()
            .equalsIgnoreCase("cdp")) {
            log.info("Detected to be in cdp profile, so will load service.properties file");
            pathToApplicationProperties = "service.properties";
        }

        try {
            log.info("Reading properties from file : {}", pathToApplicationProperties);
            FileInputStream fileInputStream = new FileInputStream(pathToApplicationProperties);
            propertiesAsProperties.load(fileInputStream);
        } catch (IOException e) {
            log.error("Property file not found !", e);
        }

        propertiesAsProperties.forEach((propertyKey, propertyValue) -> {
            // Keys starting with spring or server are considered as internal spring-boot used and should not be taken
            if(!propertyKey.toString().startsWith("spring.") && !propertyKey.toString().startsWith("server.")) {
                ApplicationConfigs propAsAppConfig =
                    ApplicationConfigMapper.getApplicationConfigFromProperty(
                        propertyKey.toString());
                if (propAsAppConfig != null) {
                    String propValue =
                        getPropertyResolvingPlaceholder(propertyKey.toString(),
                            propertiesAsProperties);

                    if (propValue != null && !propValue.isEmpty()) {
                        properties.put(propAsAppConfig, propValue);
                    }
                }
            }
        });
        
        log.info("Printing all properties before auto-discovery: ");
        printAllProperties();

        autoDiscover();

        log.info("Printing all properties after auto-discovery: ");
        printAllProperties();
    }

    private String getPropertyResolvingPlaceholder(String key, Properties propertiesAsProperties) {
        String property = "null";
        try {
             property = propertiesAsProperties.getProperty(key);
            if(property.length() > 1 && property.substring(0,2).equalsIgnoreCase("#{")) {
                property = getPropertyResolvingPlaceholder(property.substring(2,property.length()-1), propertiesAsProperties);
            }
        } catch (Exception e) {
            log.warn("Could not get property : " + key + " due to following error: ", e);
        }
        log.debug("For key: " + key + " returning property: " + property);
        return property;
    }
    
    private void printAllProperties() {
        properties.forEach((key, value) -> log.info("Key: {} and Value: {}", key.toString(), value));
    }


    public Map<ApplicationConfigs, String> getPropertiesCopy() {
        return new HashMap<>(properties);
    }

    // TODO: At startup make Auto-Discovery: all important properties should be checked foreach service
    /*
    - If properties for a service are not set (ex: hdfs.uri) => Look for config file's location property
    - If this is empty or file does not exists => WARNING: You should provide info on API call to use this sink
    - Otherwise, load the file and set the required property (hdfs.uri for example)
     */
    private void autoDiscover() {
        log.info("Starting auto-discover of properties after load of properties file");

        if(properties.get(ApplicationConfigs.HDFS_URI)==null
            && properties.get(ApplicationConfigs.HADOOP_HDFS_SITE_PATH)!=null
            && properties.get(ApplicationConfigs.HADOOP_CORE_SITE_PATH)!=null) {
            log.info("Going to auto-discover hdfs.uri");

        }

        if(properties.get(ApplicationConfigs.HBASE_ZK_QUORUM)==null
            && properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH)!=null) {
            log.info("Going to auto-discover hbase.zookeeper.quorum");
        }

        if(properties.get(ApplicationConfigs.HBASE_ZK_QUORUM_PORT)==null
            && properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH)!=null) {
            log.info("Going to auto-discover hbase.zookeeper.port");
        }

        if(properties.get(ApplicationConfigs.HBASE_ZK_ZNODE)==null
            && properties.get(ApplicationConfigs.HADOOP_HBASE_SITE_PATH)!=null) {
            log.info("Going to auto-discover hbase.zookeeper.znode");
        }

        if(properties.get(ApplicationConfigs.OZONE_SERVICE_ID)==null
            && properties.get(ApplicationConfigs.HADOOP_OZONE_SITE_PATH)!=null) {
            log.info("Going to auto-discover ozone.service.id");
        }

        if(properties.get(ApplicationConfigs.HIVE_ZK_QUORUM)==null
            && properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH)!=null) {
            log.info("Going to auto-discover hive.zookeeper.quorum");
        }

        if(properties.get(ApplicationConfigs.HIVE_ZK_ZNODE)==null
            && properties.get(ApplicationConfigs.HADOOP_HIVE_SITE_PATH)!=null) {
            log.info("Going to auto-discover hive.zookeeper.znode");
        }

        if(properties.get(ApplicationConfigs.SOLR_SERVER_HOST)==null
            && properties.get(ApplicationConfigs.SOLR_ENV_PATH)!=null) {
            log.info("Going to auto-discover solr.server.host");
        }

        if(properties.get(ApplicationConfigs.SOLR_SERVER_PORT)==null
            && properties.get(ApplicationConfigs.SOLR_ENV_PATH)!=null) {
            log.info("Going to auto-discover solr.server.port");
        }

        if(properties.get(ApplicationConfigs.KAFKA_BROKERS)==null
            && properties.get(ApplicationConfigs.KAFKA_CONF_CLIENT_PATH)!=null
            && properties.get(ApplicationConfigs.KAFKA_CONF_CLUSTER_PATH)!=null) {
            log.info("Going to auto-discover kafka.brokers");
        }

        if(properties.get(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL)==null
            && properties.get(ApplicationConfigs.KAFKA_CONF_CLIENT_PATH)!=null
            && properties.get(ApplicationConfigs.KAFKA_CONF_CLUSTER_PATH)!=null) {
            log.info("Going to auto-discover kafka.security.protocol");
        }

        if(properties.get(ApplicationConfigs.SCHEMA_REGISTRY_URL)==null
            && properties.get(ApplicationConfigs.SCHEMA_REGISTRY_CONF_PATH)!=null) {
            log.info("Going to auto-discover schema.registry.url");
        }

        if(properties.get(ApplicationConfigs.KUDU_URL)==null
            && properties.get(ApplicationConfigs.KUDU_CONF_PATH)!=null) {
            log.info("Going to auto-discover kudu.master.server");
        }

    }

}
