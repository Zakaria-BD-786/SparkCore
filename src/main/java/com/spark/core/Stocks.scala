package com.spark.core


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row


object Stocks {
   def main(args:Array[String]){

    //creating SparkSession
		val spark = SparkSession.builder().appName("Spark ETL").master("local").getOrCreate()
		
		
		//creating rdd
		val stocksRdd = spark.sparkContext.textFile("C:/Users/md.zakaria.barbhuiya/Downloads/stocks_data_2.txt")
		
		val schema = StructType(Array(StructField("Date",StringType,true),StructField("Ticker",StringType,true),StructField("Open",FloatType,true),StructField("High",FloatType,true),StructField("Low",FloatType,true),StructField("Close",FloatType,true),StructField("Volume",IntegerType,true)))
		
		
		
		//creating schema rdd
		val rowRdd = stocksRdd.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2).toFloat,x(3).toFloat,x(4).toFloat,x(5).toFloat,x(6).toInt))
		
		
		//converting rdd into dataframe
		import spark.implicits._
		val stocksDataFrame = spark.createDataFrame(rowRdd,schema) 
		stocksDataFrame.printSchema()
		stocksDataFrame.show(5)
		val filter_df = stocksDataFrame.filter($"Ticker" === "ACE")
		filter_df.show()
		
		val count = stocksDataFrame.count()
		println(count)
		
		
		
		
		
		
  }
}