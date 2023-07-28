package src.scala.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object SparkStreaming{


  def outputTerminal(df: DataFrame, method: String): StreamingQuery = {
    df
      .writeStream
      .outputMode(method)
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
  }

  def execQuery(df: DataFrame, method: String, F: (DataFrame, String) => StreamingQuery): Unit = {
    val query = F(df, method)
    query.awaitTermination()
  }

  def avgPerMonthTemperature(df: DataFrame) : Unit = {
    val dfWithTimestamp = df.withColumn("Timestamp", to_timestamp(col("Date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    val dfWithMonthAndYear = dfWithTimestamp.withColumn("Month", month(col("Timestamp"))).withColumn("Year", year(col("Timestamp")))
    val avgTempPerMonth = dfWithMonthAndYear.groupBy("Month").agg((mean("Température") - 273.15).alias("AverageTemperature")).orderBy("Month")
    execQuery(avgTempPerMonth, "complete", outputTerminal)
  }

  def minMaxPerStation(df: DataFrame): Unit = {
    val minMaxTempPerStation = df.groupBy("department (name)")
      .agg((min("Température") - 273.15).alias("MinTemperature"),
        (max("Température") - 273.15).alias("MaxTemperature"))
    execQuery(minMaxTempPerStation, "complete", outputTerminal)
  }

  def avgTempPerDay(df: DataFrame): Unit = {
    val dfWithTimestamp = df.withColumn("Timestamp", to_timestamp(col("Date"), "yyyy-MM-dd'T'HH:mm:ssZ"))
    val dfWithDate = dfWithTimestamp.withColumn("Date", to_date(col("Timestamp")))
    val avgTempPerDay = dfWithDate.groupBy("Date").agg(mean("Température"))
    execQuery(avgTempPerDay, "append", outputTerminal)
  }

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
      .add("Visibilité horizontale", FloatType, false)
      .add("Temps présent", FloatType, true)
      .add("Temps passé 1", FloatType, true)
      .add("Temps passé 2", FloatType, true)
      .add("Nebulosité totale", FloatType, true)
      .add("Nébulosité des nuages de l'étage inférieur", FloatType, false)
      .add("Hauteur de la base des nuages de l'étage inférieur", FloatType, true)
      .add("Type des nuages de l'étage inférieur", FloatType, true)
      .add("Type des nuages de l'étage moyen", FloatType, true)
      .add("Type des nuages de l'étage supérieur", FloatType, true)
      .add("Pression station", FloatType, false)
      .add("Niveau barométrique", FloatType, true)
      .add("Géopotentiel", FloatType, true)
      .add("Variation de pression en 24 heures", FloatType, true)
      .add("Température minimale sur 12 heures", FloatType, true)
      .add("Température minimale sur 24 heures", FloatType, true)
      .add("Température maximale sur 12 heures", FloatType, true)
      .add("Température maximale sur 24 heures", FloatType, true)
      .add("Température minimale du sol sur 12 heures", FloatType, true)
      .add("Méthode de mesure Température du thermomètre mouillé", FloatType, true)
      .add("Température du thermomètre mouillé", FloatType, true)
      .add("Rafale sur les 10 dernières minutes", FloatType, true)
      .add("Rafales sur une période", FloatType, false)
      .add("Periode de mesure de la rafale", FloatType, false)
      .add("Etat du sol", FloatType, true)
      .add("Hauteur totale de la couche de neige,glace,autre au sol", FloatType, true)
      .add("Hauteur de la neige fraîche", FloatType, true)
      .add("Periode de mesure de la neige fraiche", FloatType, true)
      .add("Précipitations dans la dernière heure", FloatType, true)
      .add("Précipitations dans les 3 dernières heures", FloatType, true)
      .add("Précipitations dans les 6 dernières heures", FloatType, true)
      .add("Précipitations dans les 12 dernières heures", FloatType, true)
      .add("Précipitations dans les 24 dernières heures", FloatType, true)
      .add("Phénomène spécial 1", FloatType, true)
      .add("Phénomène spécial 2", FloatType, true)
      .add("Phénomène spécial 3", FloatType, true)
      .add("Phénomène spécial 4", FloatType, true)
      .add("Nébulosité couche nuageuse 1", FloatType, true)
      .add("Type nuage 1", FloatType, true)
      .add("Hauteur de base 1", FloatType, true)
      .add("Nébulosité couche nuageuse 2", FloatType, true)
      .add("Type nuage 2", FloatType, true)
      .add("Hauteur de base 2", FloatType, true)
      .add("Nébulosité couche nuageuse 3", FloatType, true)
      .add("Type nuage 3", FloatType, true)
      .add("Hauteur de base 3", FloatType, true)
      .add("Nébulosité couche nuageuse 4", FloatType, true)
      .add("Type nuage 4", FloatType, true)
      .add("Hauteur de base 4", FloatType, true)
      .add("Coordonnees", StringType, true)
      .add("Nom", StringType, true)
      .add("Type de tendance barométrique.1", FloatType, true)
      .add("Temps passé 1.1", FloatType, true)
      .add("Temps présent.1", FloatType, true)
      .add("Température (°C)", FloatType, true)
      .add("Température minimale sur 12 heures (°C)", FloatType, true)
      .add("Température minimale sur 24 heures (°C)", FloatType, true)
      .add("Température maximale sur 12 heures (°C)", FloatType, true)
      .add("Température maximale sur 24 heures (°C)", FloatType, true)
      .add("Température minimale du sol sur 12 heures (en °C)", FloatType, true)
      .add("Latitude", FloatType, true)
      .add("Longitude", FloatType, true)
      .add("Altitude", FloatType, true)
      .add("communes (name)", StringType, true)
      .add("communes (code)", FloatType, true)
      .add("EPCI (name)", StringType, true)
      .add("EPCI (code)", FloatType, true)
      .add("department (name)", StringType, true)
      .add("department (code)", FloatType, true)
      .add("region (name)", StringType, true)
      .add("region (code)", FloatType, true)
      .add("mois_de_l_annee,index", StringType, true)


    val weatherData = spark
      .readStream
      .option("sep", ";")
      .option("header", "true")
      .schema(dataSchema)
      .csv("data")


    //avgPerMonthTemperature(weatherData)
    //minMaxPerStation(weatherData)
    //avgTempPerDay(weatherData)
  }


}
