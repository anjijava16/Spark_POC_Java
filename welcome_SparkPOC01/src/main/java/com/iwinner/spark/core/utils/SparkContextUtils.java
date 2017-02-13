package com.iwinner.spark.core.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextUtils {

	private static JavaSparkContext  context=null;
	
	public static JavaSparkContext getSparkJavaContextUtils(){
		
		SparkConf conf=new SparkConf().setAppName("SparkContext").setMaster("local[*]");
		 context=new JavaSparkContext(conf);
	
		 return context;

	}
	
	public static void closeContext(){
		
		context.close();
		
	}
}
