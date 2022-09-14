package com.cloudera.frisch.randomdatagen.service;

import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.parsers.JsonParser;
import com.cloudera.frisch.randomdatagen.parsers.Parser;
import com.cloudera.frisch.randomdatagen.sink.SinkInterface;
import com.cloudera.frisch.randomdatagen.sink.SinkSender;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;


@Service
public class DataGenerationService {

  private static final Logger logger = Logger.getLogger(DataGenerationService.class);

  public void generateDate(String modelFilePath, Long numberOfBatches, Long rowsPerBatch, List<String> sinksListAsString) {
    logger.info("Starting Generation");
    long start = System.currentTimeMillis();

    logger.info("Parsing of model file");
    Parser parser = new JsonParser(modelFilePath);
    Model model = parser.renderModelFromFile();

    List<SinkParser.sinks> sinksList = Collections.emptyList();
    try {
      for(String s: sinksListAsString) {
        sinksList.add(SinkParser.stringToSink(s));
      }
    } catch (Exception e) {
      logger.warn("Could not parse list of sinks passed, check if it's well formed");
    }

    logger.info("Initialization of all Sinks");
    List<SinkInterface> sinks = SinkSender.sinksInit(model, sinksList);

    int threads = 1;
    try {
      threads =
          Integer.parseInt(PropertiesLoader.getProperty("threads"));
    } catch (Exception e){
      logger.warn("Could nor load property of threads number, default to 1 thread");
    }
    logger.info("Will run generation using " + threads + " thread(s)");

    for(long i = 0; i < numberOfBatches; i++) {
      logger.info("Start to process batch " + (i+1) + "/" + numberOfBatches + " of " + rowsPerBatch + " rows");

      List<Row> randomDataList = model.generateRandomRows(rowsPerBatch, threads);

      // Send Data to sinks in parallel if there are multiple sinks
      sinks.parallelStream().forEach(sink -> sink.sendOneBatchOfRows(randomDataList));

      // For tests only: print generated data
      if(logger.isDebugEnabled()) {
        randomDataList.forEach(data -> logger.debug("Data is : " + data.toString()));
      }

      logger.info("Finished to process batch " + (i+1) + "/" + numberOfBatches + " of " + rowsPerBatch + " rows");
    }

    // Terminate all sinks
    sinks.forEach(SinkInterface::terminate);

    // Recap of what has been generated
    Utils.recap(numberOfBatches, rowsPerBatch, sinksList, model);

    // Compute and print time taken
    logger.info("Generation Finished");
    logger.info("Data Generation took : " + Utils.formatTimetaken(System.currentTimeMillis()-start) + " to run");
  }
}
