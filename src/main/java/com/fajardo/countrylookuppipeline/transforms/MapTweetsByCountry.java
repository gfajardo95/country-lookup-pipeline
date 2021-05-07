package com.fajardo.countrylookuppipeline.transforms;

import com.fajardo.countrylookuppipeline.models.Tweet;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import lombok.extern.slf4j.Slf4j;

/**
 * creates key/value pairs for the tweets where each tweet's key is the country
 * in which it's written
 */
@Slf4j
public class MapTweetsByCountry
  extends SimpleFunction<Tweet, KV<String, String>> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public KV<String, String> apply(Tweet tweet) {
    log.info(
      "country: {}, location: {}",
      tweet.getCountry(),
      tweet.getLocation()
    );

    return KV.of(tweet.getCountry(), tweet.getText());
  }
}
