package com.sparkTutorial.sparkSql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class HousePriceProblem {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.

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

        +----------------+-----------------+----------+
        |        Location| avg(Price SQ Ft)|max(Price)|
        +----------------+-----------------+----------+
        |          Oceano|           1145.0|   1195000|
        |         Bradley|            606.0|   1600000|
        | San Luis Obispo|            459.0|   2369000|
        |      Santa Ynez|            391.4|   1395000|
        |         Cayucos|            387.0|   1500000|
        |.............................................|
        |.............................................|
        |.............................................|

         */

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";
    private static final String AVG_PRICE_SQ_FT = "avg(" + PRICE_SQ_FT + ")";
    private static final String LOCATION = "Location";
    private static final String DOUBLE_TYPE = "double";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("HousePrice_self").master("local[1]").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> realEstateDataset = dataFrameReader.option("header","true").csv("in/RealEstate.csv");

        //System.out.println("=== Print out schema ===");
        //realEstateDataset.printSchema();

        System.out.println("=== Cast the price and price sq-ft to double ===");
        Dataset<Row> castedRealEstateDataset = realEstateDataset
                .withColumn(PRICE, col(PRICE).cast(DOUBLE_TYPE))
                .withColumn(PRICE_SQ_FT, col(PRICE_SQ_FT).cast(DOUBLE_TYPE));


        //System.out.println("=== Print out schema ===");
        //castedRealEstateDataset.printSchema();

        RelationalGroupedDataset groupedDataset = castedRealEstateDataset.groupBy(col(LOCATION));

        groupedDataset.agg(avg(PRICE_SQ_FT), max(PRICE)).orderBy(col(AVG_PRICE_SQ_FT).desc()).show();

    }
}
