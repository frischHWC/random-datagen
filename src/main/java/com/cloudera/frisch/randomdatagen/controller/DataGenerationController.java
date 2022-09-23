package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.service.DataGenerationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@RestController
@RequestMapping("/datagen")
public class DataGenerationController {

  @Autowired
  private DataGenerationService dataGenerationService;

  // TODO: Make a map of future and be able to retrieve status of a generation sent


  @PostMapping(value = "/multiplesinks")
  public void generateIntoMultipleSinks(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(name = "sinks") List<String> sinks
  ) {
    StringBuffer sinkList = new StringBuffer();
    sinks.forEach(s -> {sinkList.append(s) ; sinkList.append(" ; ");});
    log.debug("Received request with model: {} , threads: {} , batches: {}, rows: {}, to sinks: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch, sinkList);
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches, rowsPerBatch, sinks, null);
  }

  @PostMapping(value = "/csv")
  public void generateIntoCsv(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    log.debug("Received request for CSV with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("CSV"), null);
  }

  @PostMapping(value = "/json")
  public void generateIntoJson(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    log.debug("Received request for JSON with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("JSON"), null);
  }

  @PostMapping(value = "/avro")
  public void generateIntoAvro(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    log.debug("Received request for Avro with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("AVRO"), null);
  }

  @PostMapping(value = "/parquet")
  public void generateIntoParquet(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    log.debug("Received request for Parquet with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("PARQUET"), null);
  }

  @PostMapping(value = "/orc")
  public void generateIntoOrc(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    log.debug("Received request for ORC with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("ORC"), null);
  }

  // TODO: Add extra properties optional for below sinks

  @PostMapping(value = "/hdfs-csv")
  public void generateIntoHdfsCsv(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hdfs_site_path") String hdfsSitePath,
      @RequestParam(required = false, name = "core_site_path") String coreSitePath,
      @RequestParam(required = false, name = "hdfs_uri") String hdfsUri,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HDFS-CSV with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hdfsSitePath!=null && !hdfsSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HDFS_SITE_PATH, hdfsSitePath);
    }
    if(hdfsUri!=null && !hdfsUri.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_URI, hdfsUri);
    }
    if(coreSitePath!=null && !coreSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_CORE_SITE_PATH, coreSitePath);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-CSV"), extraProperties);
  }

  @PostMapping(value = "/hdfs-avro")
  public void generateIntoHdfsAvro(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hdfs_site_path") String hdfsSitePath,
      @RequestParam(required = false, name = "core_site_path") String coreSitePath,
      @RequestParam(required = false, name = "hdfs_uri") String hdfsUri,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HDFS-AVRO with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hdfsSitePath!=null && !hdfsSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HDFS_SITE_PATH, hdfsSitePath);
    }
    if(hdfsUri!=null && !hdfsUri.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_URI, hdfsUri);
    }
    if(coreSitePath!=null && !coreSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_CORE_SITE_PATH, coreSitePath);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-AVRO"), extraProperties);
  }

  @PostMapping(value = "/hdfs-json")
  public void generateIntoHdfsJson(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hdfs_site_path") String hdfsSitePath,
      @RequestParam(required = false, name = "core_site_path") String coreSitePath,
      @RequestParam(required = false, name = "hdfs_uri") String hdfsUri,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HDFS-JSON with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hdfsSitePath!=null && !hdfsSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HDFS_SITE_PATH, hdfsSitePath);
    }
    if(hdfsUri!=null && !hdfsUri.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_URI, hdfsUri);
    }
    if(coreSitePath!=null && !coreSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_CORE_SITE_PATH, coreSitePath);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-JSON"), extraProperties);
  }

  @PostMapping(value = "/hdfs-parquet")
  public void generateIntoHdfsParquet(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hdfs_site_path") String hdfsSitePath,
      @RequestParam(required = false, name = "core_site_path") String coreSitePath,
      @RequestParam(required = false, name = "hdfs_uri") String hdfsUri,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HDFS-PARQUET with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hdfsSitePath!=null && !hdfsSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HDFS_SITE_PATH, hdfsSitePath);
    }
    if(hdfsUri!=null && !hdfsUri.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_URI, hdfsUri);
    }
    if(coreSitePath!=null && !coreSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_CORE_SITE_PATH, coreSitePath);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-PARQUET"), extraProperties);
  }

  @PostMapping(value = "/hdfs-orc")
  public void generateIntoHdfsOrc(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hdfs_site_path") String hdfsSitePath,
      @RequestParam(required = false, name = "core_site_path") String coreSitePath,
      @RequestParam(required = false, name = "hdfs_uri") String hdfsUri,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HDFS-ORC with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hdfsSitePath!=null && !hdfsSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HDFS_SITE_PATH, hdfsSitePath);
    }
    if(hdfsUri!=null && !hdfsUri.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_URI, hdfsUri);
    }
    if(coreSitePath!=null && !coreSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_CORE_SITE_PATH, coreSitePath);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HDFS_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-ORC"), extraProperties);
  }

  @PostMapping(value = "/hbase")
  public void generateIntoHbase(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hbase_site_path") String hbaseSitePath,
      @RequestParam(required = false, name = "hbase_zk_quorum") String hbaseZkQuorum,
      @RequestParam(required = false, name = "hbase_zk_port") String hbaseZkPort,
      @RequestParam(required = false, name = "hbase_zk_znode") String hbaseZkZnode,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HBASE with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hbaseSitePath!=null && !hbaseSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HBASE_SITE_PATH, hbaseSitePath);
    }
    if(hbaseZkQuorum!=null && !hbaseZkQuorum.isEmpty()){
      extraProperties.put(ApplicationConfigs.HBASE_ZK_QUORUM, hbaseZkQuorum);
    }
    if(hbaseZkPort!=null && !hbaseZkPort.isEmpty()){
      extraProperties.put(ApplicationConfigs.HBASE_ZK_QUORUM_PORT, hbaseZkPort);
    }
    if(hbaseZkZnode!=null && !hbaseZkZnode.isEmpty()){
      extraProperties.put(ApplicationConfigs.HBASE_ZK_ZNODE, hbaseZkZnode);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HBASE_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HBASE_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HBASE_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HBASE"), extraProperties);
  }

  @PostMapping(value = "/hive")
  public void generateIntoHive(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "hive_site_path") String hiveSitePath,
      @RequestParam(required = false, name = "hive_zk_quorum") String hiveZkQuorum,
      @RequestParam(required = false, name = "hive_zk_znode") String hiveZkZnode,
      @RequestParam(required = false, name = "truststore_location") String trustoreLocation,
      @RequestParam(required = false, name = "truststore_password") String trustorePassword,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for HIVE with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(hiveSitePath!=null && !hiveSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HIVE_SITE_PATH, hiveSitePath);
    }
    if(hiveZkQuorum!=null && !hiveZkQuorum.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_ZK_QUORUM, hiveZkQuorum);
    }
    if(hiveZkZnode!=null && !hiveZkZnode.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_ZK_ZNODE, hiveZkZnode);
    }
    if(trustoreLocation!=null && !trustoreLocation.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_TRUSTSTORE_LOCATION, trustoreLocation);
    }
    if(trustorePassword!=null && !trustorePassword.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_TRUSTSTORE_PASSWORD, trustorePassword);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.HIVE_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HIVE"), extraProperties);
  }

  @PostMapping(value = "/ozone")
  public void generateIntoOzone(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "ozone_site_path") String ozoneSitePath,
      @RequestParam(required = false, name = "ozone_service_id") String ozoneServiceId,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for OZONE with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(ozoneSitePath!=null && !ozoneSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_OZONE_SITE_PATH, ozoneSitePath);
    }
    if(ozoneSitePath!=null && !ozoneSitePath.isEmpty()){
      extraProperties.put(ApplicationConfigs.OZONE_SERVICE_ID, ozoneServiceId);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.OZONE_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.OZONE_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.OZONE_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("OZONE"), extraProperties);
  }

  @PostMapping(value = "/kafka")
  public void generateIntoKafka(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "brokers") String kafkaBrokers,
      @RequestParam(required = false, name = "protocol") String kafkaProtocol,
      @RequestParam(required = false, name = "sr_url") String schemaRegistryUrl,
      @RequestParam(required = false, name = "sr_tls") String schemaRegistryTls,
      @RequestParam(required = false, name = "keystore_location") String kafkaKeystoreLocation,
      @RequestParam(required = false, name = "keystore_password") String kafkaKeystorePassword,
      @RequestParam(required = false, name = "keystore_key_password") String kafkaKeystoreKeyPassword,
      @RequestParam(required = false, name = "truststore_location") String trustoreLocation,
      @RequestParam(required = false, name = "truststore_password") String trustorePassword,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for KAFKA with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(kafkaBrokers!=null && !kafkaBrokers.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_BROKERS, kafkaBrokers);
    }
    if(kafkaProtocol!=null && !kafkaProtocol.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_SECURITY_PROTOCOL, kafkaProtocol);
    }
    if(schemaRegistryUrl!=null && !schemaRegistryUrl.isEmpty()){
      extraProperties.put(ApplicationConfigs.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
    }
    if(schemaRegistryTls!=null && !schemaRegistryTls.isEmpty()){
      extraProperties.put(ApplicationConfigs.SCHEMA_REGISTRY_TLS_ENABLED, schemaRegistryTls);
    }
    if(kafkaKeystoreLocation!=null && !kafkaKeystoreLocation.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_KEYSTORE_LOCATION, kafkaKeystoreLocation);
    }
    if(kafkaKeystorePassword!=null && !kafkaKeystorePassword.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_KEYSTORE_PASSWORD, kafkaKeystorePassword);
    }
    if(kafkaKeystoreKeyPassword!=null && !kafkaKeystoreKeyPassword.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_KEYSTORE_KEYPASSWORD, kafkaKeystoreKeyPassword);
    }
    if(trustoreLocation!=null && !trustoreLocation.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_TRUSTSTORE_LOCATION, trustoreLocation);
    }
    if(trustorePassword!=null && !trustorePassword.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_TRUSTSTORE_PASSWORD, trustorePassword);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.KAFKA_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("KAFKA"), extraProperties);
  }

  @PostMapping(value = "/solr")
  public void generateIntoSolR(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "solr_host") String solrHost,
      @RequestParam(required = false, name = "solr_port") String solrPort,
      @RequestParam(required = false, name = "solr_tls") String solrTls,
      @RequestParam(required = false, name = "truststore_location") String trustoreLocation,
      @RequestParam(required = false, name = "truststore_password") String trustorePassword,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for SOLR with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(solrHost!=null && !solrHost.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_SERVER_HOST, solrHost);
    }
    if(solrPort!=null && !solrPort.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_SERVER_PORT, solrPort);
    }
    if(solrTls!=null && !solrTls.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_TLS_ENABLED, solrTls);
    }
    if(trustoreLocation!=null && !trustoreLocation.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_TRUSTSTORE_LOCATION, trustoreLocation);
    }
    if(trustorePassword!=null && !trustorePassword.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_TRUSTSTORE_PASSWORD, trustorePassword);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.SOLR_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("SOLR"), extraProperties);
  }

  @PostMapping(value = "/kudu")
  public void generateIntoKudu(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = false, name = "kudu_servers") String kuduServers,
      @RequestParam(required = false, name = "truststore_location") String trustoreLocation,
      @RequestParam(required = false, name = "truststore_password") String trustorePassword,
      @RequestParam(required = false, name = "kerb_auth") String kerberosEnabled,
      @RequestParam(required = false, name = "kerb_user") String kerberosUser,
      @RequestParam(required = false, name = "kerb_keytab") String kerberosKeytab
  ) {
    log.debug("Received request for KUDU with model: {} , threads: {} , batches: {}, rows: {}", modelFilePath, threads, numberOfBatches, rowsPerBatch);

    Map<ApplicationConfigs, String> extraProperties = new HashMap<>();
    if(kuduServers!=null && !kuduServers.isEmpty()){
      extraProperties.put(ApplicationConfigs.HADOOP_HDFS_SITE_PATH, kuduServers);
    }
    if(trustoreLocation!=null && !trustoreLocation.isEmpty()){
      extraProperties.put(ApplicationConfigs.KUDU_TRUSTSTORE_LOCATION, trustoreLocation);
    }
    if(trustorePassword!=null && !trustorePassword.isEmpty()){
      extraProperties.put(ApplicationConfigs.KUDU_TRUSTSTORE_PASSWORD, trustorePassword);
    }
    if(kerberosEnabled!=null && !kerberosEnabled.isEmpty()){
      extraProperties.put(ApplicationConfigs.KUDU_AUTH_KERBEROS, kerberosEnabled);
    }
    if(kerberosUser!=null && !kerberosUser.isEmpty()){
      extraProperties.put(ApplicationConfigs.KUDU_AUTH_KERBEROS_USER, kerberosUser);
    }
    if(kerberosKeytab!=null && !kerberosKeytab.isEmpty()){
      extraProperties.put(ApplicationConfigs.KUDU_AUTH_KERBEROS_KEYTAB, kerberosKeytab);
    }
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("KUDU"), extraProperties);
  }



}
