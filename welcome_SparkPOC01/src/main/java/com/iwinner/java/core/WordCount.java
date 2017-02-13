package com.iwinner.java.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		
		
		
SparkConf conf=new SparkConf().setAppName("WordCountAPP").setMaster("local[*]");
		
		JavaSparkContext context=new JavaSparkContext(conf);
		
		JavaRDD<String> data1=context.textFile("file:///home/hadoop/Desktop/Input/input.txt");
		
		
		JavaRDD<String> data2=data1.flatMap(new FlatMapFunction<String,String>(){
			public Iterable<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" "));
			}
		});

		
		JavaPairRDD<String, Integer> data3=data2.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) throws Exception {
				Tuple2<String,Integer> tuple=new Tuple2<String,Integer>(s,1);
				return tuple;
			}
		});

		
		JavaPairRDD<String, Integer> counts=data3.reduceByKey(new Function2<Integer,Integer,Integer>(){
				public Integer call(Integer val1, Integer val2) throws Exception {
				
					return val1+val2;
				}
	});	

		
		
		
		List<Tuple2<String, Integer>> output = counts.collect();
		
		for (Tuple2<String, Integer> tuple : output) {
			
			System.out.println(tuple._1() + ": " + tuple._2());
			
		}
			
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		context.close();
		
		
		
		
		
	
	}
}
