package com.sparkTutorial.pairRdd.groupbykey;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

public class AirportsByCountryProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,
           output the the list of the names of the airports located in each country.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           "Canada", ["Bagotville", "Montreal", "Coronation", ...]
           "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
           "Papua New Guinea",  ["Goroka", "Madang", ...]
           ...
         */

        SparkConf conf = new SparkConf().setAppName("airports-self").setMaster("local[2]");  //locally with 2 cores

        //connection to the spark cluster
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = javaSparkContext.textFile("in/airports.text");

        JavaPairRDD<String, String> pairRDD = lines.mapToPair(getPairFunction()); //airport country -> airport name

        JavaPairRDD<String, Iterable<String>> pairRDDGrouped = pairRDD.groupByKey(); //airport country -> Iterable (airport names)

        for (Map.Entry<String, Iterable<String>> airports : pairRDDGrouped.collectAsMap().entrySet()) {
            System.out.println(airports.getKey() + " : " + airports.getValue());
        }

    }

    private static PairFunction<String, String, String> getPairFunction() {
        return s -> {
            String[] vals = s.split(",");
            return new Tuple2<>(vals[3],vals[1]);
        };
    }
}
