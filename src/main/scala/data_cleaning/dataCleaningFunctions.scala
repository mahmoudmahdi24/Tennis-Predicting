package data_cleaning

/**
  * Created by pc on 16/02/2017.
  */

import org.apache.spark.sql.{DataFrame}

trait dataCleaningFunctions {

  def removeEmptyRows(df : DataFrame) : DataFrame

  def createDateColumn(df :DataFrame) : DataFrame

  def cleanScore(df : DataFrame): DataFrame

  def createScoreColumn(df :DataFrame) : DataFrame

// pour chaque tournamenet,group by, check that id is the same
// pour chaque player, groupby, check that the id is the same


}