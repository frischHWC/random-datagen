package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.DataGenerationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;


@Slf4j
@RestController
@RequestMapping("/datagen")
public class DataGenerationController {

  @Autowired
  private DataGenerationService dataGenerationService;

  // TODO: Make a map of future and be able to retrieve status of a generation sent

  // TODO: Make a test generator that returns one json row of a model

  // TODO: Make a health poller to report status

  // TODO: Add some metrics to gather through entire life of this process

  @PostMapping(value = "/multiplesinks")
  public void generateIntoMultipleSinks(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch,
      @RequestParam(required = true, name = "sinks") List<String> sinks
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        sinks, null);
  }

  @PostMapping(value = "/csv")
  public void generateIntoCsv(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
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
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("ORC"), null);
  }

  // TODO: Add extra properties optional for below sinks

  @PostMapping(value = "/hdfs-csv")
  public void generateIntoHdfsCsv(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-CSV"), null);
  }

  @PostMapping(value = "/hdfs-avro")
  public void generateIntoHdfsAvro(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-AVRO"), null);
  }

  @PostMapping(value = "/hdfs-json")
  public void generateIntoHdfsJson(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-JSON"), null);
  }

  @PostMapping(value = "/hdfs-parquet")
  public void generateIntoHdfsParquet(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-PARQUET"), null);
  }

  @PostMapping(value = "/hdfs-orc")
  public void generateIntoHdfsOrc(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HDFS-ORC"), null);
  }

  @PostMapping(value = "/hbase")
  public void generateIntoHbase(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HBASE"), null);
  }

  @PostMapping(value = "/hive")
  public void generateIntoHive(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("HIVE"), null);
  }

  @PostMapping(value = "/kafka")
  public void generateIntoKafka(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("KAFKA"), null);
  }

  @PostMapping(value = "/ozone")
  public void generateIntoOzone(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("OZONE"), null);
  }

  @PostMapping(value = "/solr")
  public void generateIntoSolR(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("SOLR"), null);
  }

  @PostMapping(value = "/kudu")
  public void generateIntoKudu(
      @RequestParam(required = false, name = "model") String modelFilePath,
      @RequestParam(required = false, name = "threads") Integer threads,
      @RequestParam(required = false, name = "batches") Long numberOfBatches,
      @RequestParam(required = false, name = "rows") Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("KUDU"), null);
  }



}
