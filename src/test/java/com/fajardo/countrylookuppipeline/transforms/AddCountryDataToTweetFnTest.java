package com.fajardo.countrylookuppipeline.transforms;

import java.util.Map;

import com.fajardo.countrylookuppipeline.models.Tweet;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

public class AddCountryDataToTweetFnTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testProcessElementAddsCountryToTweet() {
    Tweet expectedTweet = new Tweet(
      "testText",
      "United States",
      "Fort Lauderdale, FL, USA"
    );

    // side input
    PCollectionView<Map<String, String>> cities = testPipeline
      // data comes from a local file at the root level called cities.csv
      .apply(TextIO.read().from("cities.csv"))
      // the transform that maps each city to its country (city -> country)
      .apply(MapElements.via(new MapCityToCountryFn()))
      // the beam sdk transform that returns our PCollectionView as a map
      .apply(View.<String, String>asMap());

    // the initial tweet input data that is missing the country info
    PCollection<Tweet> input = testPipeline.apply(
      Create.of(new Tweet("testText", "", "Fort Lauderdale, FL, USA"))
    );

    // the transform that is tested
    PCollection<Tweet> output = input.apply(
      ParDo
        .of(new AddCountryDataToTweetFn(cities))
        // the side input is provided here and above
        // now the city map is available for the transform to use
        .withSideInput("cities", cities)
    );

    PAssert.that(output).containsInAnyOrder(expectedTweet);

    testPipeline.run();
  }
}
