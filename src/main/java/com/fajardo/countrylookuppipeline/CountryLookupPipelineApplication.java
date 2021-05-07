package com.fajardo.countrylookuppipeline;

import com.fajardo.countrylookuppipeline.pipeline.CountryLookupPipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class CountryLookupPipelineApplication {

  public static void main(String[] args) {
    SpringApplication.run(CountryLookupPipelineApplication.class, args);
  }

  @Bean
  public CountryLookupPipeline pipeline() {
    return new CountryLookupPipeline();
  }
}
