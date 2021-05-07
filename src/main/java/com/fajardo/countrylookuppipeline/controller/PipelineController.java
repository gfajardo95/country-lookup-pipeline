package com.fajardo.countrylookuppipeline.controller;

import com.fajardo.countrylookuppipeline.pipeline.CountryLookupPipeline;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Controller
@Slf4j
public class PipelineController implements Pipeline {

  private final CountryLookupPipeline lookupPipeline;

  @Override
  @GetMapping("pipeline")
  public void run() {
    log.info("Pipeline triggered");
    lookupPipeline.run();
    log.info("Pipeline finished");
  }
}
