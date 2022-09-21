package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.DataGenerationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;


@Slf4j
@RestController
@RequestMapping("/datagen")
public class DataGenerationController {

  @Autowired
  private DataGenerationService dataGenerationService;

  @PostMapping(value = "/csv")
  public void generateIntoCSV(
      @RequestParam String modelFilePath,
      @RequestParam Integer threads,
      @RequestParam Long numberOfBatches,
      @RequestParam Long rowsPerBatch
  ) {
    dataGenerationService.generateData(modelFilePath, threads, numberOfBatches,rowsPerBatch,
        Collections.singletonList("CSV"), null);
  }



}
