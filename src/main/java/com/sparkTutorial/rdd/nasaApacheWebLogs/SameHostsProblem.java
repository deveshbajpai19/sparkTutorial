package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */

        SparkConf conf = new SparkConf().setAppName("nasa-self").setMaster("local[2]");  //locally with 2 cores

        //connection to the spark cluster
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines19950701 = readHostsWithoutHeader(javaSparkContext, "in/nasa_19950701.tsv");
        JavaRDD<String> lines19950801 = readHostsWithoutHeader(javaSparkContext, "in/nasa_19950801.tsv");

        JavaRDD<String> intersectHostNames1995 = lines19950701
                .intersection(lines19950801);

        System.out.println("Output length: "+intersectHostNames1995.count());

        for(String line: intersectHostNames1995.collect()){
            System.out.println(line);
        }

        intersectHostNames1995.saveAsTextFile("out/nasa_logs_same_hosts_self.csv");

    }

    private static JavaRDD<String> readHostsWithoutHeader(JavaSparkContext javaSparkContext, String filePath) {

        JavaRDD<String> lines = javaSparkContext.textFile(filePath);

        return lines
                .map(line -> line.split("\t")[0])
                .filter(line -> !line.equals("host"));

    }
}
