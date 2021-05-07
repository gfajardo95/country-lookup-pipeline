package com.fajardo.countrylookuppipeline.models;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CountryTweet {
    private String country;
    private String text;
}
