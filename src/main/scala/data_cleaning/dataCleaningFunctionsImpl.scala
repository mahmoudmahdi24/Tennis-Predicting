package data_cleaning

/**
  * Created by Mahmoud MEHDI on 16/02/2017.
  */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}


class dataCleaningFunctionsImpl extends dataCleaningFunctions{

  override def removeEmptyRows(df: DataFrame): DataFrame ={
    df.filter(x => !x.anyNull);
  }

  override def createDateColumn(df: DataFrame): DataFrame = {

    //date column
    val get_Year_Month_Day: (String) => (Int,Int,Int) = {
      (date: String) => {
        (date.splitAt(4)._1.toInt,date.splitAt(4)._2.splitAt(2)._1.toInt,date.splitAt(4)._2.splitAt(2)._2.toInt)
      }
    }


    import org.apache.spark.sql.functions.udf
    val explodeDate = udf(get_Year_Month_Day)

    df.withColumn("year",explodeDate(df.col("tourney_date")).getItem("_1"))
      .withColumn("month",explodeDate(df.col("tourney_date")).getItem("_2"))
      .withColumn("day",explodeDate(df.col("tourney_date")).getItem("_3"))
      .drop("tourney_date");

  }

  override def createScoreColumn(df: DataFrame): DataFrame = {

    //score column
    val get_Scores: (String) => (String,String,String, String, String ,String) = {
      (score: String) => {
        score.split(" ").size match {
          case 6 => (score.split(" ")(0),score.split(" ")(1),score.split(" ")(2),score.split(" ")(3),score.split(" ")(4),score.split(" ")(5))
          case 5 => (score.split(" ")(0),score.split(" ")(1),score.split(" ")(2),score.split(" ")(3),score.split(" ")(4),"0")
          case 4 => (score.split(" ")(0),score.split(" ")(1),score.split(" ")(2),score.split(" ")(3),"0","0")
          case 3 => (score.split(" ")(0),score.split(" ")(1),score.split(" ")(2),"0","0","0")
          case 2 => (score.split(" ")(0),score.split(" ")(1),"0","0","0","0")
          case 1 => (score.split(" ")(0),"0","0","0","0","0")
          case _ => ("0","0","0","0","0","0")

        }
      }
    }


    import org.apache.spark.sql.functions.udf
    val explodeScore = udf(get_Scores)

    df.withColumn("score_set1",explodeScore(df.col("score")).getItem("_1"))
      .withColumn("score_set2",explodeScore(df.col("score")).getItem("_2"))
      .withColumn("score_set3",explodeScore(df.col("score")).getItem("_3"))
      .withColumn("score_set4",explodeScore(df.col("score")).getItem("_4"))
      .withColumn("score_set5",explodeScore(df.col("score")).getItem("_5"))
      .withColumn("score_set6",explodeScore(df.col("score")).getItem("_6"))

  }

  override def cleanScore(df: DataFrame): DataFrame = {

    // Removing Walkover games because they won't help us to create reliable ML models (this line is necessary to be able to inferschema )
    return df.where(!col("score").contains("Walkover"))
    // Removing games where a player couldn't play (withdrawl)
             .where(!col("score").contains("W/O"))

  }
}