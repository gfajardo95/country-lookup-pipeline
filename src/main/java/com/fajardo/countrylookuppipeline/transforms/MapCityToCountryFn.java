package com.fajardo.countrylookuppipeline.transforms;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * The input string has the format "city,country". This transform returns the string as a KV with format of
 * "city: country"
 */
public class MapCityToCountryFn
  extends SimpleFunction<String, KV<String, String>> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public KV<String, String> apply(String line) {
    String[] tokens = line.split(",");

    return KV.of(tokens[0], tokens[1]);
  }
}
