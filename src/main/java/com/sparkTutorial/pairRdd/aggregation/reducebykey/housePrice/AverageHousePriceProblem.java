package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.columnar.DOUBLE;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class AverageHousePriceProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           output the average price for houses with different number of bedrooms.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

           (3, 325000)
           (1, 266356)
           (2, 325000)
           ...

           3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.
         */

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AvgHouse").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");

        //represent as Bedroom -> Cost PairRDD
        JavaPairRDD<Integer, Double> realEstateBedroomCostPairRDD = lines

                .filter(line -> !line.contains("Bedrooms")) //avoiding header line
                .mapToPair(
                    line -> {

                        String[] vals = line.split(",");
                        return new Tuple2<>(Integer.parseInt(vals[3]), Double.parseDouble(vals[2]));
                    }
        );

        //represent as Bedroom -> AvgCost(1, Cost) PairRDD
        JavaPairRDD<Integer, AvgCount> realEstateBedroomAvgCountPairRDD = realEstateBedroomCostPairRDD.
                mapToPair(tuple -> {
                    return new Tuple2<>(tuple._1, new AvgCount(1, tuple._2));
                });

        //represent as Bedroom -> AvgCost(total count, total cost) PairRDD (reduced by key)
        JavaPairRDD<Integer, AvgCount> realEstateBedroomAvgCountReducedPairRDD = realEstateBedroomAvgCountPairRDD.
                reduceByKey((tuple1, tuple2) -> new AvgCount(tuple1.getCount() + tuple2.getCount(), tuple1.getTotal() + tuple2.getTotal()));

        //represent as Bedroom -> Double value of Average Cost (total count/total cost) PairRDD
        JavaPairRDD<Integer, Double> realEstateBedroomAvgCountReducedFinalPairRDD = realEstateBedroomAvgCountReducedPairRDD.mapValues(value -> value.getTotal()/value.getCount());

        //print
        for (Map.Entry<Integer, Double> housePriceAvgPair : realEstateBedroomAvgCountReducedFinalPairRDD.collectAsMap().entrySet()) {
            System.out.println(housePriceAvgPair.getKey() + " : " + housePriceAvgPair.getValue());
        }

    }

}
