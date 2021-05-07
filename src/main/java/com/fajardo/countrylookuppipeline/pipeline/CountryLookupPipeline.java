package com.fajardo.countrylookuppipeline.pipeline;

import java.util.Map;

import com.fajardo.countrylookuppipeline.models.Tweet;
import com.fajardo.countrylookuppipeline.transforms.AddCountryDataToTweetFn;
import com.fajardo.countrylookuppipeline.transforms.ExtractTweetsFn;
import com.fajardo.countrylookuppipeline.transforms.MapCityToCountryFn;
import com.fajardo.countrylookuppipeline.transforms.MapTweetsByCountry;
import com.fajardo.countrylookuppipeline.transforms.TweetMappingToString;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class CountryLookupPipeline {

  public void run() {
    Pipeline pipeline = Pipeline.create(buildDataflowPipelineOptions());

    // prepare the side input
    PCollectionView<Map<String, String>> cities = pipeline
      .apply(
        TextIO.read().from("gs://dataflow-happiness-level/cities/cities.csv")
      ) // the read transform splits the file into lines
      .apply(MapElements.via(new MapCityToCountryFn()))
      .apply(View.<String, String>asMap());

    pipeline
      .apply(
        PubsubIO
          .readStrings()
          .fromSubscription(
            "projects/happiness-level/subscriptions/pull-tweets"
          )
      )
      .apply(
        new PTransform<PCollection<String>, PCollection<Tweet>>() {
          @Override
          public PCollection<Tweet> expand(PCollection<String> input) {
            return input
              .apply(ParDo.of(new ExtractTweetsFn()))
              .apply(
                ParDo
                  .of(new AddCountryDataToTweetFn(cities))
                  .withSideInput("cities", cities)
              );
          }
        }
      )
      .apply(MapElements.via(new MapTweetsByCountry()))
      .apply(new TweetMappingToString())
      .apply(
        PubsubIO
          .writeStrings()
          .to("projects/happiness-level/topics/country-sentiments")
      );

    pipeline.run().waitUntilFinish();
  }

  private DataflowPipelineOptions buildDataflowPipelineOptions() {
    DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(
      DataflowPipelineOptions.class
    );

    pipelineOptions.setRunner(DataflowRunner.class);
    pipelineOptions.setProject("happiness-level");
    pipelineOptions.setRegion("us-east1");
    pipelineOptions.setGcpTempLocation("gs://dataflow-happiness-level/tmp");
    pipelineOptions.setWorkerMachineType("n1-standard-1");
    pipelineOptions.setMaxNumWorkers(2);

    return pipelineOptions;
  }
}
