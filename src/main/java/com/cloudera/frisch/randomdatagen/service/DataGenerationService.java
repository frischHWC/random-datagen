package com.cloudera.frisch.randomdatagen.service;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.parsers.JsonParser;
import com.cloudera.frisch.randomdatagen.parsers.Parser;
import com.cloudera.frisch.randomdatagen.sink.SinkInterface;
import com.cloudera.frisch.randomdatagen.sink.SinkSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Service
@Slf4j
public class DataGenerationService {
  
  @Autowired
  private PropertiesLoader propertiesLoader;

  public void generateData(@Nullable String modelFilePath,
                           @Nullable Integer numberOfThreads,
                           @Nullable Long numberOfBatches,
                           @Nullable Long rowsPerBatch,
                           List<String> sinksListAsString,
                           @Nullable Map<ApplicationConfigs, String> extraProperties)
  {
    log.info("Starting Generation");
    long start = System.currentTimeMillis();

    // Get default values if some are not set
    Map<ApplicationConfigs, String> properties = propertiesLoader.getPropertiesCopy();

    if(extraProperties!=null && !extraProperties.isEmpty()) {
      log.info("Found extra properties sent with the call, these will replace defaults ones");
      properties.putAll(extraProperties);
    }

    int threads = 1;
    if(numberOfThreads!=null) {
      threads = numberOfThreads;
    } else if (properties.get(ApplicationConfigs.THREADS)!=null) {
      threads = Integer.parseInt(properties.get(ApplicationConfigs.THREADS));
    }
    log.info("Will run generation using {} thread(s)", threads);

    Long batches = 1L;
    if(numberOfBatches!=null) {
      batches = numberOfBatches;
    } else if (properties.get(ApplicationConfigs.NUMBER_OF_BATCHES_DEFAULT)!=null) {
      batches = Long.valueOf(properties.get(ApplicationConfigs.NUMBER_OF_BATCHES_DEFAULT));
    }
    log.info("Will run generation for {} batches", batches);

    Long rows = 1L;
    if(rowsPerBatch!=null) {
      rows = rowsPerBatch;
    } else if (properties.get(ApplicationConfigs.NUMBER_OF_ROWS_DEFAULT)!=null) {
      rows = Long.valueOf(properties.get(ApplicationConfigs.NUMBER_OF_ROWS_DEFAULT));
    }
    log.info("Will run generation for {} rows", rows);

    String modelFile = modelFilePath;
    if(modelFilePath==null) {
      log.info("No model file passed, will default to custom data model or default defined one in configuration");
      if(properties.get(ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT)!=null) {
        modelFile = properties.get(ApplicationConfigs.CUSTOM_DATA_MODEL_DEFAULT);
      } else {
        modelFile = properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT) +
            properties.get(ApplicationConfigs.DATA_MODEL_DEFAULT);
      }
    }
    if(modelFilePath!=null && !modelFilePath.contains("/")){
      log.info("Model file passed is identified as one of the one provided, so will look for it in data model path: {} ",
          properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT));
      modelFile = properties.get(ApplicationConfigs.DATA_MODEL_PATH_DEFAULT) + modelFilePath;
    }

    // Parsing model
    log.info("Parsing of model file: {}", modelFile);
    Parser parser = new JsonParser(modelFile);
    Model model = parser.renderModelFromFile();

    // Creation of sinks
    List<SinkParser.sinks> sinksList = new ArrayList<>();
    try {
      if(sinksListAsString==null || sinksListAsString.isEmpty()){
        log.info("No Sink has been defined, so defaulting to JSON sink");
        sinksList.add(SinkParser.stringToSink("JSON"));
      }
      for(String s: sinksListAsString) {
        sinksList.add(SinkParser.stringToSink(s));
      }
    } catch (Exception e) {
      log.warn("Could not parse list of sinks passed, check if it's well formed");
    }

    log.info("Initialization of all Sinks");
    List<SinkInterface> sinks = SinkSender.sinksInit(model, properties, sinksList);


    // Launch Generation of data
    for(long i = 0; i < batches; i++) {
      log.info("Start to process batch {}/{} of {} rows", i, batches, rows);

      List<Row> randomDataList = model.generateRandomRows(rows, threads);

      // Send Data to sinks in parallel if there are multiple sinks
      sinks.parallelStream().forEach(sink -> sink.sendOneBatchOfRows(randomDataList));

      // For tests only: print generated data
      if(log.isDebugEnabled()) {
        randomDataList.forEach(data -> log.debug("Data is : " + data.toString()));
      }

      log.info("Finished to process batch {}/{} of {} rows", i, batches, rows);
    }

    // Terminate all sinks
    sinks.forEach(SinkInterface::terminate);

    // Recap of what has been generated
    Utils.recap(batches, rows, sinksList, model);

    // Compute and print time taken
    log.info("Generation Finished");
    log.info("Data Generation took : {} to run", Utils.formatTimetaken(System.currentTimeMillis()-start));
  }
}
