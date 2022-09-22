package com.cloudera.frisch.randomdatagen.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@RestController
@RequestMapping("/health")
public class HealthController {

  // TODO: Make a health poller to report status
}
