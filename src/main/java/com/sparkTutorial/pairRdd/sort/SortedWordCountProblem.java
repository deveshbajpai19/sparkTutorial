package com.sparkTutorial.pairRdd.sort;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("words-self").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");

        JavaRDD<String> wordRdd = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordPairRdd = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordPairFreqRdd = wordPairRdd.reduceByKey(Integer::sum);



        //reverse of wordPairFreqRdd
        JavaPairRDD<Integer, String> FreqWordPairRdd = wordPairFreqRdd.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        //false means descending
        JavaPairRDD<Integer, String> FreqWordPairSortedRdd = FreqWordPairRdd.sortByKey(false);
        
        //reverse it back
        JavaPairRDD<String, Integer> wordPairFreqRddSortedRdd = FreqWordPairSortedRdd.mapToPair(countToWord -> new Tuple2<>(countToWord._2(), countToWord._1()));


        for (Tuple2<String, Integer> wordToCount : wordPairFreqRddSortedRdd.collect()) {
            System.out.println(wordToCount._1() + " : " + wordToCount._2());
        }
    }

}

