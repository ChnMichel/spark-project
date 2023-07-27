package src.scala.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.duration.DurationInt

object ProducerApp extends App {

  def splitCSVFile(inputPath: String, outputPath: String, linesPerFile: Int, intervalSeconds: Int): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("ProducerApp")
      .getOrCreate()

    // Lecture du fichier CSV
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(inputPath)

    // Ajout d'une colonne d'index
    val indexedData = df.withColumn("index", monotonically_increasing_id())

    // Calcul du nombre total de fichiers à créer
    val totalFiles = Math.ceil(indexedData.count().toDouble / linesPerFile).toInt

    // Fonction pour créer un fichier CSV pour chaque intervalle
    def createCSVFile(startIndex: Long, endIndex: Long, fileIndex: Int): Unit = {
      val fileData = indexedData.filter(col("index").between(startIndex, endIndex))
      fileData
        .coalesce(1)
        .write
        .format("csv")
        .option("header", "true")
        .mode("append")
        .save(outputPath)
    }

    // Création des fichiers CSV avec les intervalles de temps spécifiés
    var fileIndex = 1
    var startIndex = 0L
    var endIndex = linesPerFile - 1L
    while (startIndex < indexedData.count()) {
      createCSVFile(startIndex, endIndex, fileIndex)

      // Calcul des index de début et de fin pour le prochain fichier
      startIndex += linesPerFile
      endIndex += linesPerFile

      // Pause pendant l'intervalle spécifié
      Thread.sleep(intervalSeconds.seconds.toMillis)

      fileIndex += 1
    }

    println(totalFiles + " fichiers ont été créé.")
    spark.stop()
  }

  //Utilisation)
  val inputPath = "data/dataset.csv"
  val outputPath = "data/juiced_dataset"
  val linesPerFile = 1000 // Nombre de lignes par fichier
  val intervalSeconds = 5 // Intervalle de 10 secondes entre chaque fichier

  splitCSVFile(inputPath, outputPath, linesPerFile, intervalSeconds)

}