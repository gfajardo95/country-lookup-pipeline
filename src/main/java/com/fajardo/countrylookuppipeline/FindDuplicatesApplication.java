package com.fajardo.countrylookuppipeline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FindDuplicatesApplication {

  public static void main(String[] args) {
    List<List<String>> records = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader("worldcities.csv"))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] values = line.split(",");
        records.add(Arrays.asList(values));
      }
    } catch (IOException e) {
      System.exit(-1);
    }

    Map<String, String> cities = new HashMap<>();

    for (List<String> record : records) {
      cities.putIfAbsent(record.get(0), record.get(1));
    }

    File csvOutputFile = new File("cities.csv");
    try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
      for (Map.Entry<String, String> cityEntry : cities.entrySet()) {
        pw.println(cityEntry.getKey() + "," + cityEntry.getValue());
      }
    } catch (FileNotFoundException e) {
      System.exit(-1);
    }
  }
}
