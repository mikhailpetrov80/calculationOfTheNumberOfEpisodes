package calculationOfTheNumberOfEpisodes

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.json4s.scalap.scalasig.ScalaSigEntryParsers.key
import provider.{DefaultSparkSessionProvider, SparkSessionProviderComponent}

object CalculationOfTheNumberOfEpisodesDatasetApp extends App
  with SparkSessionProviderComponent
  with CalculationOfTheNumberOfEpisodesDataset {
  override def sparkSessionProvider = new DefaultSparkSessionProvider("CalculationOfTheNumberOfEpisodesDatasetApp")

  episodesCount("tv_shows_data.csv")
}

//case class Line(Key: String, count: Int)

case class Genre(value: String)

case class GenreEpisodesCount(genre: Genre, count: Long)

trait CalculationOfTheNumberOfEpisodesDataset {
  this: SparkSessionProviderComponent =>

  private val sparkSession = sparkSessionProvider.sparkSession

  import sparkSession.implicits._

  def episodesCount(path: String): Unit = {

    //val toInt = udf[Int, String]( _.toInt)

    // DataFrame is simply a type alias of Dataset[Row]

    val textDF: DataFrame = sparkSession.read
      //.format("csv")
      .option("header", "true")
      .csv(path)
      .withColumn("No_of_Episodes", $"No_of_Episodes".cast(sql.types.LongType))
      .withColumnRenamed("No_of_Episodes", "count")
      .select("Genre", "count")
      .na.drop()

    /*val genreDS = textDF
      .select("Genre")
      .as[Genre]*/


      //.as[GenreEpisodesCount]

      //.where(col("No_of_Episodes") == 9)
      //.show()

    //.
      //.textFile("tv_shows_data.csv")
      //.toDF()

    //val linesDS: Dataset[Line] = (textDF1: Dataset[Row]).as[Line]

    /*val episodesDS = linesDS.flatMap(line => line.value.split(";"))
      .filter(_.nonEmpty)
      .as[Episodes]*/

    /*val counts: Dataset[GenreEpisodesCount] =
      genreDS
        .groupByKey(identity)
        .count() // Row["key", "count(1)"]
        //        .withColumnRenamed("key", "word")
        //        .withColumnRenamed("count(1)", "count")
        //        // Row["word", "count"]
        //        .as[WordCount]
        .map { case (genre, count) => GenreEpisodesCount(genre, count) }*/
    //counts.sort(desc("count")).show(10)
    //
    textDF.show(30)
    //textDF.where(col(No_of_Episodes) == "unknown" ).show(30)
    //textDF1.printSchema()
    //linesDS.show(30)
    //genre.show(30)
 }

}
