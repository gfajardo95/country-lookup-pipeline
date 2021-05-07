package com.fajardo.countrylookuppipeline.transforms;

import com.fajardo.countrylookuppipeline.models.CountryTweet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TweetMappingToString extends PTransform<PCollection<KV<String, String>>, PCollection<String>> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public PCollection<String> expand(PCollection<KV<String, String>> row) {
        return row.apply(ParDo.of(new DoFn<KV<String, String>, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void ProcessElement(ProcessContext c) {
                CountryTweet countryTweet = new CountryTweet(c.element().getKey(), c.element().getValue());
                log.info("{}: {}", countryTweet.getCountry(), countryTweet.getText());

                try {
                    c.output(objectMapper.writeValueAsString(countryTweet));
                } catch (JsonProcessingException e) {
                    log.error("Error converting the CountrySentiment to a string: {}", e);
                }
            }
        }));
    }
}
