package com.cloudera.frisch.randomdatagen.controller;


import com.cloudera.frisch.randomdatagen.service.DataGenerationService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;


@RestController
@RequestMapping("/datagen")
public class DataGenerationController {

  private static final Logger logger = Logger.getLogger(DataGenerationController.class);

  @Autowired
  private DataGenerationService dataGenerationService;

  @PostMapping(value = "/json")
  public void generateIntoJson(
      @RequestParam String modelFilePath,
      @RequestParam Long numberOfBatches,
      @RequestParam Long rowsPerBatch
  ) {
    dataGenerationService.generateDate(modelFilePath, numberOfBatches,rowsPerBatch,
        Collections.singletonList("JSON"));
  }


}
