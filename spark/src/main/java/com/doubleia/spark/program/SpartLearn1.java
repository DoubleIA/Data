package com.doubleia.spark.program;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 
 * 官方文档：http://spark.apache.org/docs/1.4.1/programming-guide.html
 * @author wangyingbo
 *
 */
public class SpartLearn1 {
	
	//Initialize
	//Create JavaSparkContext to access a cluster
	SparkConf conf = new SparkConf().setAppName("SparkLearn1").setMaster("local[*]");//master is a Spark, Mesos or YARN cluster URL or a special “local” string to run in local mode
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	//Resilient Distributed Datasets (RDDs)
	//a fault-tolerant collection of elements that can be operated on in parallel
	//two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system
	// such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
	
	//Parallelized Collections
	// Spark will run one task for each partition of the cluster
	List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
	JavaRDD<Integer> distData = sc.parallelize(data);
	
	//External Datasets
	JavaRDD<String> distFile = sc.textFile("data.txt");

	//RDD Operations： two types of operations
	//transformations，which create a new dataset from an existing one
	//actions，which return a value to the driver program after running a computation on the dataset
	//All transformations in Spark are lazy，in that they do not compute their results right away.
	//The transformations are only computed when an action requires a result to be returned to the driver program.
	//each transformed RDD may be recomputed each time you run an action on it
	//you may also persist an RDD in memory using the persist (or cache) method for much faster access
	// also support for persisting RDDs on disk, or replicated across multiple nodes.
	
	
	//Basics
	
	
	//example
//	JavaRDD<String> lines = sc.textFile("data.txt");
//	JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
//	int totalLength = lineLengths.reduce((a, b) -> a + b);
	
	//If we also wanted to use lineLengths again later, we could add:
//	lineLengths.persist();
	
	
	//Passing Functions to Spark
	
	
	//two ways to create functions:
   //Implement the Function interfaces in your own class, either as an anonymous inner class or a named one, and pass an instance of it to Spark.
   //In Java 8, use lambda expressions to concisely define an implementation.
	
	//we could have written our code above as follows:
	JavaRDD<String> lines = sc.textFile("data.txt");
	JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
		public Integer call(String s) { return s.length(); }
	});
	int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer a, Integer b) { return a + b; }
	});

	//Understanding closures 
	
	
}
