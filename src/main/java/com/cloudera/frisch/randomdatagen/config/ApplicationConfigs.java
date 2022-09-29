package com.cloudera.frisch.randomdatagen.config;

public enum ApplicationConfigs {

  APP_NAME,
  APP_PORT,

  HADOOP_USER,
  HADOOP_HOME,

  THREADS,
  NUMBER_OF_BATCHES_DEFAULT,
  NUMBER_OF_ROWS_DEFAULT,
  DATA_MODEL_PATH_DEFAULT,
  DATA_MODEL_DEFAULT,
  CUSTOM_DATA_MODEL_DEFAULT,
  SCHEDULER_FILE_PATH,

  ADMIN_USER,
  ADMIN_PASSWORD,

  KERBEROS_ENABLED,
  KERBEROS_USER,
  KERBEROS_KEYTAB,

  TLS_ENABLED,
  TRUSTSTORE_LOCATION,
  TRUSTSTORE_PASSWORD,
  KEYSTORE_LOCATION,
  KEYSTORE_PASSWORD,
  KEYSTORE_KEYPASSWORD,

  HADOOP_CORE_SITE_PATH,
  HADOOP_HDFS_SITE_PATH,
  HADOOP_OZONE_SITE_PATH,
  HADOOP_HBASE_SITE_PATH,
  HADOOP_HIVE_SITE_PATH,
  SOLR_ENV_PATH,
  KAFKA_CONF_CLIENT_PATH,
  KAFKA_CONF_CLUSTER_PATH,
  SCHEMA_REGISTRY_CONF_PATH,
  KUDU_CONF_PATH,

  HDFS_URI,
  HDFS_AUTH_KERBEROS,
  HDFS_AUTH_KERBEROS_USER,
  HDFS_AUTH_KERBEROS_KEYTAB,

  HBASE_ZK_QUORUM,
  HBASE_ZK_QUORUM_PORT,
  HBASE_ZK_ZNODE,
  HBASE_AUTH_KERBEROS,
  HBASE_AUTH_KERBEROS_USER,
  HBASE_AUTH_KERBEROS_KEYTAB,

  OZONE_SERVICE_ID,
  OZONE_AUTH_KERBEROS,
  OZONE_AUTH_KERBEROS_USER,
  OZONE_AUTH_KERBEROS_KEYTAB,

  HIVE_ZK_QUORUM,
  HIVE_ZK_ZNODE,
  HIVE_AUTH_KERBEROS,
  HIVE_AUTH_KERBEROS_USER,
  HIVE_AUTH_KERBEROS_KEYTAB,
  HIVE_TRUSTSTORE_LOCATION,
  HIVE_TRUSTSTORE_PASSWORD,

  SOLR_SERVER_HOST,
  SOLR_SERVER_PORT,
  SOLR_TLS_ENABLED,
  SOLR_AUTH_KERBEROS,
  SOLR_AUTH_KERBEROS_USER,
  SOLR_AUTH_KERBEROS_KEYTAB,
  SOLR_TRUSTSTORE_LOCATION,
  SOLR_TRUSTSTORE_PASSWORD,
  SOLR_KEYSTORE_LOCATION,
  SOLR_KEYSTORE_PASSWORD,

  KAFKA_BROKERS,
  KAFKA_SECURITY_PROTOCOL,
  SCHEMA_REGISTRY_URL,
  SCHEMA_REGISTRY_TLS_ENABLED,
  KAFKA_AUTH_KERBEROS_USER,
  KAFKA_AUTH_KERBEROS_KEYTAB,
  KAFKA_TRUSTSTORE_LOCATION,
  KAFKA_TRUSTSTORE_PASSWORD,
  KAFKA_KEYSTORE_LOCATION,
  KAFKA_KEYSTORE_PASSWORD,
  KAFKA_KEYSTORE_KEYPASSWORD,
  KAFKA_SASL_MECHANISM,
  KAFKA_SASL_KERBEROS_SERVICE_NAME,

  KUDU_URL,
  KUDU_AUTH_KERBEROS,
  KUDU_AUTH_KERBEROS_USER,
  KUDU_AUTH_KERBEROS_KEYTAB,
  KUDU_TRUSTSTORE_LOCATION,
  KUDU_TRUSTSTORE_PASSWORD

}

