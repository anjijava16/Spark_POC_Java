package com.iwinner.java.core;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.iwinner.spark.core.utils.SparkContextUtils;




public class GameCodeSparkContext {

	public static <R> void main(String[] args) {
		
		
		JavaRDD<String> rdd=SparkContextUtils.getSparkJavaContextUtils().textFile("/home/hadoop/Desktop/POC&Solutions/input/gamedata");
		
System.out.println("=================================== Start here ==============================");

System.out.println("Count here is "+rdd.collect());
System.out.println("=================================== End here ==============================");
		
//JavaRDD

/*
JavaRDD<Record> rdd_records = sc.textFile(data).map(
		  new Function<String, Record>() {
		      public Record call(String line) throws Exception {
		         // Here you can use JSON
		         // Gson gson = new Gson();
		         // gson.fromJson(line, Record.class);
		         String[] fields = line.split(",");
		         Record sd = new Record(fields[0], fields[1], fields[2].trim(), fields[3]);
		         return sd;
		      }
		});*/

/*rdd.map(new Function<String, String>() {
			public String call(String data) throws Exception {
				return Arrays.asList(data.split("\t"));
			}
		
			});*/
		/*
		JavaRDD<String> words=rdd.map(new FlatMapFunction<String,String>(){
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(String data) throws Exception {
				return Arrays.asList(data.split(" "));
			}
		});
		*/
		
	}
}
