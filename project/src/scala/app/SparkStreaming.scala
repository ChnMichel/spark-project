package src.scala.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object SparkStreaming{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkStreamingApp")
      .getOrCreate()

    val dataSchema = new StructType()
      .add("ID OMM station", StringType, true)
      .add("Date", StringType, true)
      .add("Pression au niveau mer", FloatType, true)
      .add("Variation de pression en 3 heures", FloatType, true)
      .add("Type de tendance barométrique", FloatType, true)
      .add("Direction du vent moyen 10 mn", FloatType, true)
      .add("Vitesse du vent moyen 10 mn", FloatType, true)
      .add("Température", FloatType, true)
      .add("Point de rosée", FloatType, true)
      .add("Humidité", FloatType, true)
  }
}
