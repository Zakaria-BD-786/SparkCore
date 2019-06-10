package com.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object WordCount {
	def main(args:Array[String]){

		val conf = new SparkConf().setAppName("Test").setMaster("local")
		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		sc.setLogLevel("WARN")
		
		//read data in to a dataframe
		val inputDatadf = sc.textFile("C:/Users/md.zakaria.barbhuiya/Downloads/stocks_data.txt")
		//inputDatadf.show(5)
		
		inputDatadf.count()
		

	}
}