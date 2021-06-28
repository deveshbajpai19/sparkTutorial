package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class UnionLogProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setAppName("nasa-self").setMaster("local[2]");  //locally with 2 cores

        //connection to the spark cluster
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines19950701 = readWithoutHeader(javaSparkContext, "in/nasa_19950701.tsv");
        JavaRDD<String> lines19950801 = readWithoutHeader(javaSparkContext, "in/nasa_19950801.tsv");

        JavaRDD<String> common1995 = lines19950701.union(lines19950801).sample(false, 0.1);

        for(String line: common1995.collect()){
            System.out.println(line);
        }

        common1995.saveAsTextFile("out/sample_nasa_logs_self.csv");

    }

    private static JavaRDD<String> readWithoutHeader(JavaSparkContext javaSparkContext, String filePath) {

        JavaRDD<String> lines = javaSparkContext.textFile(filePath);

        //extract header
        String header = lines.first();
        return lines.filter(line -> !line.equals(header));
    }
}
