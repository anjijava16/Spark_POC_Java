package com.iwinner.java.core;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.iwinner.spark.core.utils.SparkContextUtils;

public class WordCountMain {

	public static void main(String[] args) {
		
		

		
		JavaRDD<String> lines=SparkContextUtils.getSparkJavaContextUtils().textFile("file:///home/hadoop/Desktop/Input/input.txt");
		
		JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String,String>(){
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(String data) throws Exception {
				return Arrays.asList(data.split(" "));
			}
		});
		
		
		
		JavaPairRDD<String,Integer> ones=words.mapToPair(new PairFunction<String, String, Integer>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String res) throws Exception {
				Tuple2<String, Integer> tuple=new Tuple2<String,Integer>(res,1);
				return tuple;
			}
		});
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		
		JavaPairRDD<String, Integer> resultRDD=counts.coalesce(1);
		
		resultRDD.saveAsTextFile("/home/hadoop/Desktop/output/input_02");
		
		
		//counts.saveAsTextFile("/home/hadoop/Desktop/output/input_01");
		
		
		
/*					
* 
 JavaPairRDD<String, String> counts=ones.reduceByKey(new Function2<String, Integer, Integer>() {
	
	
 });*/
		
		SparkContextUtils.closeContext();

	}
}
