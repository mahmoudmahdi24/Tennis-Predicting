package com.example

import data_cleaning.dataCleaningFunctionsImpl
import org.apache.spark.sql.SparkSession


object ReadData {

  val dataCleaningFunctions = new dataCleaningFunctionsImpl();

  def runCode() = {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    var df = sparkSession.read
        .option("delimiter",",")
        .option("header",true)
        .option("inferSchema", true)
        .csv("/Users/pc/datascience-challenges/kaggle/atp-tennis/data/atp-matches-dataset/allGames");
    println(df.count());

    df.show(10);

    df.printSchema();
    sparkSession.stop()

  }



}
