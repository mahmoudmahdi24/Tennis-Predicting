package com.example

import data_cleaning.dataCleaningFunctionsImpl
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.regression.LinearRegression


object Challenge {

  val dataCleaningFunctions = new dataCleaningFunctionsImpl();

  def runCode() = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    var df = sparkSession.read
        .option("delimiter",",")
        .option("header",true)
//      .option("inferSchema", true)
        .csv(Config.data);

    //remove empty lines :
    df = dataCleaningFunctions.removeEmptyRows(df);

    //create Day,Month and Year columns from "tourney_date", and remove "tourney_date"
    df = dataCleaningFunctions.createDateColumn(df);

    //sort games by date (ascneding)
    import org.apache.spark.sql.functions._
    df = df.orderBy(desc("year"),desc("month"),desc("day"))

    //create score columns
    df = dataCleaningFunctions.createScoreColumn(df);


//    df= df.filter(col("winner_name").contains("Djokovic"))

//    df=df.where(col("tourney_name").contains("Davis"));




//    df = df.select("score","score_set1","score_set2","score_set3","score_set4","score_set5","score_set7")


// save allGames
    df.coalesce(1).write.format("csv")
      .option("header",true)
      .save(Config.data+"/allGames/allGames.csv");


    df.show()
    sparkSession.stop()

  }



}
