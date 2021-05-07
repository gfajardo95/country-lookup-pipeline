package com.fajardo.countrylookuppipeline.transforms;

import java.util.Map;

import com.fajardo.countrylookuppipeline.models.Tweet;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AddCountryDataToTweetFn extends DoFn<Tweet, Tweet> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private final PCollectionView<Map<String, String>> cities;

  private Map<String, String> cityTable;

  @ProcessElement
  public void ProcessElement(ProcessContext c) {
    cityTable = c.sideInput(cities);

    if (c.element().getCountry().isEmpty()) {
      String location = c.element().getLocation().strip();
      String[] locationTokens = location.split(",");

      for (String token : locationTokens) {
        // check if the token is a key in the "cities" side input
        if (cityTable.containsKey(token)) {
          c.output(
            new Tweet(
              c.element().getText(),
              cityTable.get(token),
              c.element().getLocation()
            )
          );
        }
      }
    }
  }
}
