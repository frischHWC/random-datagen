package com.cloudera.frisch.randomdatagen.model;

import org.apache.log4j.Logger;

public class OptionsConverter {

    private static final Logger logger = Logger.getLogger(OptionsConverter.class);

    public enum PrimaryKeys {
        KAFKA_MSG_KEY,
        HBASE_PRIMARY_KEY,
        OZONE_BUCKET,
        OZONE_KEY,
        KUDU_HASH_KEYS
    }

    static PrimaryKeys convertOptionToPrimaryKey(String option) {
        switch (option.toUpperCase()) {
            case "KAFKA_MSG_KEY":
                return PrimaryKeys.KAFKA_MSG_KEY;
            case "HBASE_PRIMARY_KEY":
                return PrimaryKeys.HBASE_PRIMARY_KEY;
            case "OZONE_BUCKET":
                return PrimaryKeys.OZONE_BUCKET;
            case "OZONE_KEY":
                return PrimaryKeys.OZONE_KEY;
            case "KUDU_HASH_KEYS":
                return PrimaryKeys.KUDU_HASH_KEYS;
            default:
                logger.warn("Option was not recognized: " + option + " , please verify your JSON");
                return null;
        }
    }

    public enum TableNames {
        HDFS_FILE_PATH,
        HDFS_FILE_NAME,
        HIVE_HDFS_FILE_PATH,
        HBASE_TABLE_NAME,
        HBASE_NAMESPACE,
        KAFKA_TOPIC,
        OZONE_VOLUME,
        SOLR_COLLECTION,
        HIVE_DATABASE,
        HIVE_TABLE_NAME,
        HIVE_TEMPORARY_TABLE_NAME,
        KUDU_TABLE_NAME,
        LOCAL_FILE_PATH,
        LOCAL_FILE_NAME,
        AVRO_NAME
    }

    static TableNames convertOptionToTableNames(String option) {
        switch (option.toUpperCase()) {
            case "HDFS_FILE_PATH":
                return TableNames.HDFS_FILE_PATH;
            case "HDFS_FILE_NAME":
                return TableNames.HDFS_FILE_NAME;
            case "HIVE_HDFS_FILE_PATH":
                return TableNames.HIVE_HDFS_FILE_PATH;
            case "HBASE_TABLE_NAME":
                return TableNames.HBASE_TABLE_NAME;
            case "HBASE_NAMESPACE":
                return TableNames.HBASE_NAMESPACE;
            case "KAFKA_TOPIC":
                return TableNames.KAFKA_TOPIC;
            case "OZONE_VOLUME":
                return TableNames.OZONE_VOLUME;
            case "SOLR_COLLECTION":
                return TableNames.SOLR_COLLECTION;
            case "HIVE_DATABASE":
                return TableNames.HIVE_DATABASE;
            case "HIVE_TABLE_NAME":
                return TableNames.HIVE_TABLE_NAME;
            case "HIVE_TEMPORARY_TABLE_NAME":
                return TableNames.HIVE_TEMPORARY_TABLE_NAME;
            case "KUDU_TABLE_NAME":
                return TableNames.KUDU_TABLE_NAME;
            case "LOCAL_FILE_PATH":
                return TableNames.LOCAL_FILE_PATH;
            case "LOCAL_FILE_NAME":
                return TableNames.LOCAL_FILE_NAME;
            case "AVRO_NAME":
                return TableNames.AVRO_NAME;
            default:
                logger.warn("Option was not recognized: " + option + " , please verify your JSON");
                return null;
        }
    }

    public enum Options {
        HBASE_COLUMN_FAMILIES_MAPPING,
        SOLR_SHARDS,
        SOLR_REPLICAS
    }

    static Options convertOptionToOption(String option) {
        switch (option.toUpperCase()) {
            case "HBASE_COLUMN_FAMILIES_MAPPING":
                return Options.HBASE_COLUMN_FAMILIES_MAPPING;
            case "SOLR_SHARDS":
                return Options.SOLR_SHARDS;
            case "SOLR_REPLICAS":
                return Options.SOLR_REPLICAS;
            default:
                logger.warn("Option was not recognized: " + option + " , please verify your JSON");
                return null;
        }
    }
}