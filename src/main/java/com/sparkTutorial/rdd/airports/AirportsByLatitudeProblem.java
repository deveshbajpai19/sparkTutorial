package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */

        SparkConf conf = new SparkConf().setAppName("airports-self").setMaster("local[2]");  //locally with 2 cores

        //connection to the spark cluster
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = javaSparkContext.textFile("in/airports.text");

        JavaRDD<String> linesLatMoreThan40 = lines.filter(
                line ->
                Float.parseFloat(
                        line.split(Utils.COMMA_DELIMITER)[6]) > 40.0
        );

        JavaRDD<String> linesResult = linesLatMoreThan40.map(line -> {

            String[] lineComponents = line.split(Utils.COMMA_DELIMITER);

            StringBuilder result = new StringBuilder();
            result.append(lineComponents[1]); //airport name
            result.append(",");
            result.append(lineComponents[6]); //latitude

            return result.toString();


        });

        for(String line: linesResult.collect()) {
            System.out.println(line);
        }

        linesResult.saveAsTextFile("out/airports_by_latitude_self.text");
    }
}
