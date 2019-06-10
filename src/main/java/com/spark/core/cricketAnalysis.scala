package com.spark.core

import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object cricketAnalysis {

case class Cricket(id:Int,season:Int,city:String,date:String,team1:String,team2:String,toss_winner:String,toss_decision:String,result:String,dl_applied:String,winner:String,win_by_runs:Int,win_by_wickets:Int,player_of_match:String,venue:String,umpire1:String,umpire2:String)

def main(args:Array[String]){


	// Creating configuration and spark context
	val spark = SparkSession.builder.appName("Cricket_Aggregator").master("local").getOrCreate()


	// Reading file
  val initialRdd = spark.sparkContext.textFile("C:/Users/md.zakaria.barbhuiya/Downloads/matches.csv")
	val arr = initialRdd.take(5)
	arr.foreach(println)


	// creating dataframe from rdd, infering schema using reflection
  val schemaRdd = initialRdd.map(x=>x.split(",")).map(x => Cricket(x(0).toInt,x(1).toInt,x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11).toInt,x(12).toInt,x(13),x(14),x(15),x(16)))

	import spark.implicits._
	val initialDataFrame = schemaRdd.toDF()
	initialDataFrame.show(2)
	initialDataFrame.printSchema()
	
	//Which stadium is best suitable for first batting
	val battingFirstWinDataFrame = initialDataFrame.withColumn("bat_first_win/loss",when($"win_by_wickets" === 0,"win").otherwise("loss")).select($"venue",$"bat_first_win/loss")
	val battingFirstWinDataFrameResults = battingFirstWinDataFrame.filter($"bat_first_win/loss" === "win").groupBy($"venue").agg(count($"venue").alias("total")).orderBy($"total".desc)
	battingFirstWinDataFrameResults.show(truncate=false) 
	

}


}