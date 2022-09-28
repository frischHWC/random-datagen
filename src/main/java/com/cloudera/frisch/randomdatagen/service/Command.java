package com.cloudera.frisch.randomdatagen.service;

import com.cloudera.frisch.randomdatagen.config.ApplicationConfigs;
import com.cloudera.frisch.randomdatagen.config.SinkParser;
import com.cloudera.frisch.randomdatagen.model.Model;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.UUID;


@Setter
@Getter
@Slf4j
public class Command {

  private UUID commandUuid;
  private CommandStatus status;
  private String commandComment;
  private String modelFilePath;
  private Model model;
  private Integer numberOfThreads;
  private Long numberOfBatches;
  private Long rowsPerBatch;
  private Boolean scheduled;
  private Long delayBetweenExecutions;
  private List<SinkParser.Sink> sinksListAsString;
  private Map<ApplicationConfigs, String> properties;
  private Long durationSeconds;
  private Long lastFinishedTimestamp;
  private double progress;

  @Override
  public String toString() {
    StringBuffer sinkList = new StringBuffer();
    sinksListAsString.forEach(s -> {sinkList.append(s) ; sinkList.append(" ; ");});

    StringBuffer propertiesAsString = new StringBuffer();
    properties.forEach((config, value) -> {
      propertiesAsString.append(config);
      propertiesAsString.append(" -> ");
      String valueEscaped = value.replaceAll("\"", "\\\"");
      propertiesAsString.append(valueEscaped);
      propertiesAsString.append(" ; ");
    });

    return "{ " +
        "\"uuid\": \"" + commandUuid.toString() + "\"" +
        " , \"status\": \"" + status.toString() + "\"" +
        " , \"duration\": \"" + durationSeconds + "\"" +
        " , \"progress\": \"" + progress + "\"" +
        " , \"comment\": \"" + commandComment + "\"" +
        " , \"model_file\": \"" + modelFilePath + "\"" +
        " , \"number_of_batches\": \"" + numberOfBatches + "\"" +
        " , \"rows_per_batch\": \"" + rowsPerBatch + "\"" +
        " , \"scheduled\": \"" + scheduled + "\"" +
        " , \"delay_between_executions\": \"" + delayBetweenExecutions + "\"" +
        " , \"last_finished_timestamp\": \"" + lastFinishedTimestamp + "\"" +
        " , \"sinks\": \"" + sinkList + "\"" +
        " , \"extra_properties\": \"" + propertiesAsString + "\"" +
        " }";
  }

  public Command(String modelFilePath,
                 Model model,
                 Integer numberOfThreads,
                 Long numberOfBatches,
                 Long rowsPerBatch,
                 Boolean scheduled,
                 Long delayBewtweenExecutions,
                 List<SinkParser.Sink> sinksListAsString,
                 Map<ApplicationConfigs, String> properties) {
    this.commandUuid = UUID.randomUUID();
    this.status = CommandStatus.QUEUED;
    this.commandComment = "";
    this.model = model;
    this.modelFilePath = modelFilePath;
    this.numberOfThreads = numberOfThreads;
    this.numberOfBatches = numberOfBatches;
    this.rowsPerBatch = rowsPerBatch;
    this.scheduled = scheduled;
    this.delayBetweenExecutions = delayBewtweenExecutions;
    this.lastFinishedTimestamp = 0L;
    this.sinksListAsString = sinksListAsString;
    this.properties = properties;
    this.durationSeconds = 0L;
    this.progress = 0f;
  }


  public enum CommandStatus {
    QUEUED,
    STARTED,
    RUNNING,
    FINISHED,
    FAILED
  }

}
