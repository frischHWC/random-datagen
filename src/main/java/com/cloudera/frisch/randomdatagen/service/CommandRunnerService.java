package com.cloudera.frisch.randomdatagen.service;


import com.cloudera.frisch.randomdatagen.Utils;
import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.PropertiesLoader;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.parsers.JsonParser;
import com.cloudera.frisch.randomdatagen.parsers.Parser;
import com.cloudera.frisch.randomdatagen.sink.SinkInterface;
import com.cloudera.frisch.randomdatagen.sink.SinkSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


@Service
@Slf4j
public class CommandRunnerService {

  @Autowired
  private PropertiesLoader propertiesLoader;

  @Autowired
  private MetricsService metricsService;

  private final Map<UUID, Command> commands = new HashMap<>();
  private final ConcurrentLinkedQueue<Command> commandsToProcess = new ConcurrentLinkedQueue<>();

  public String getCommandAsString(UUID uuid) {
    Command command = commands.get(uuid) ;
    return command != null ? command.toString() : "null";
  }

  public List<String> getAllCommands() {
    List<String> commandsAsList = new ArrayList<>();
    commands.forEach((u,c) -> commandsAsList.add(c.toString()));
    return commandsAsList;
  }

  public List<String> getCommandsByStatus(Command.CommandStatus status) {
    List<String> commandsAsList = new ArrayList<>();
    commands.forEach((u,c) -> {
      if(c.getStatus()==status) {
        commandsAsList.add(c.toString());
      }
    });
    return commandsAsList;
  }


  /**
   * Create a command by solving all properties, model and all empty vars and queue the command to be processed
   * @param modelFilePath
   * @param numberOfThreads
   * @param numberOfBatches
   * @param rowsPerBatch
   * @param sinksListAsString
   * @param extraProperties
   */
  public String generateData(@Nullable String modelFilePath,
                           @Nullable Integer numberOfThreads,
                           @Nullable Long numberOfBatches,
                           @Nullable Long rowsPerBatch,
                           @Nullable Boolean scheduled,
                           @Nullable Long delayBetweenExecutions,
                           List<String> sinksListAsString,
                           @Nullable Map<ApplicationConfigs, String> extraProperties) {

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
    JsonParser parser = new JsonParser(modelFile);
    if(parser.getRoot()==null) {
      log.warn("Error when parsing model file");
      return "{ \"commandUuid\": \"\" , \"error\": \"Error with Model File - Verify its path and structure\" }";
    }
    Model model = parser.renderModelFromFile();

    // Creation of sinks
    List<SinkParser.Sink> sinksList = new ArrayList<>();
    try {
      if(sinksListAsString==null || sinksListAsString.isEmpty()){
        log.info("No Sink has been defined, so defaulting to JSON sink");
        sinksList.add(SinkParser.stringToSink("JSON"));
      } else {
        for (String s : sinksListAsString) {
          sinksList.add(SinkParser.stringToSink(s));
        }
      }
    } catch (Exception e) {
      log.warn("Could not parse list of sinks passed, check if it's well formed");
      return "{ \"commandUuid\": \"\" , \"error\": \"Wrong Sinks\" }";
    }

    // Creation of command and queued to be processed
    Command command = new Command(modelFile, model, threads, batches, rows, scheduled, delayBetweenExecutions, sinksList, properties);
    commands.put(command.getCommandUuid(), command);
    commandsToProcess.add(command);

    log.info("Command: {} has been queued to be processed", command.getCommandUuid());

    return "{ \"commandUuid\": \"" + command.getCommandUuid() + "\" , \"error\": \"\" }";

  }

  /**
   * Processer of the command queued
   */
  @Scheduled(fixedDelay = 1000, initialDelay = 10000)
  public void processCommands() {
    Command command = commandsToProcess.poll();

    if(command!=null) {
      command.setStatus(Command.CommandStatus.STARTED);
      long start = System.currentTimeMillis();

      try {
        log.info("Starting Generation for command: {}",
            command.getCommandUuid());


        log.info("Initialization of all Sinks");
        List<SinkInterface> sinks =
            SinkSender.sinksInit(command.getModel(), command.getProperties(),
                command.getSinksListAsString());

        // Launch Generation of data
        command.setStatus(Command.CommandStatus.RUNNING);
        for (long i = 1; i <= command.getNumberOfBatches(); i++) {
          log.info("Start to process batch {}/{} of {} rows", i,
              command.getNumberOfBatches(), command.getRowsPerBatch());

          List<Row> randomDataList = command.getModel()
              .generateRandomRows(command.getRowsPerBatch(),
                  command.getNumberOfThreads());

          // Send Data to sinks in parallel if there are multiple sinks
          sinks.parallelStream()
              .forEach(sink -> sink.sendOneBatchOfRows(randomDataList));

          // For tests only: print generated data
          if (log.isDebugEnabled()) {
            randomDataList.forEach(
                data -> log.debug("Data is : " + data.toString()));
          }

          log.info("Finished to process batch {}/{} of {} rows", i,
              command.getNumberOfBatches(), command.getRowsPerBatch());
          command.setDurationSeconds(System.currentTimeMillis() - start);
          command.setProgress(
              ((double) i / (double) command.getNumberOfBatches()) * 100.0);
        }

        // Terminate all sinks
        sinks.forEach(SinkInterface::terminate);

        // Add metrics
        metricsService.updateMetrics(command.getNumberOfBatches(), command.getRowsPerBatch(), command.getSinksListAsString());

        // Recap of what has been generated
        Utils.recap(command.getNumberOfBatches(), command.getRowsPerBatch(), command.getSinksListAsString(), command.getModel());
        command.setStatus(Command.CommandStatus.FINISHED);

      } catch (Exception e) {
        log.warn("An error occurred on command: {} => Mark this command as failed", command.getCommandUuid());
        command.setStatus(Command.CommandStatus.FAILED);
      }

      // Compute and print time taken
      log.info("Generation Finished");
      log.info("Data Generation for command: {} took : {} to run", command.getCommandUuid(), Utils.formatTimetaken(System.currentTimeMillis()-start));
    }
  }

}
