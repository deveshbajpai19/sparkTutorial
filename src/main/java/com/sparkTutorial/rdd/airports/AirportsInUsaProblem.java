package com.sparkTutorial.rdd.airports;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaProblem {

    private static final String DELIMITER = ",";

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */

        SparkConf conf = new SparkConf().setAppName("airports-self").setMaster("local[2]");  //locally with 2 cores

        //connection to the spark cluster
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = javaSparkContext.textFile("in/airports.text");

        JavaRDD<String> linesUSA = lines.filter(line -> line.split(DELIMITER)[3].equals("\"United States\""));

        JavaRDD<String> linesResult = linesUSA.map(line -> {

                String[] lineComponents = line.split(DELIMITER);

                StringBuilder result = new StringBuilder();
                result.append(lineComponents[1]); //airport name
                result.append(DELIMITER);
                result.append(lineComponents[2]); //airport city

                return result.toString();


        });

        //.collect() can be an intensive operation hence only good for prototyping purposes
        for(String line: linesResult.collect()) {
            System.out.println(line);
        }


        linesResult.saveAsTextFile("out/airports_in_usa_self.txt");

    }



}
