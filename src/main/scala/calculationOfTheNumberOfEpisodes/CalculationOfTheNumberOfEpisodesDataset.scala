package calculationOfTheNumberOfEpisodes

import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import provider.{DefaultSparkSessionProvider, SparkSessionProviderComponent}

object CalculationOfTheNumberOfEpisodesDatasetApp extends App
  with SparkSessionProviderComponent
  with CalculationOfTheNumberOfEpisodesDataset {
  override def sparkSessionProvider = new DefaultSparkSessionProvider("CalculationOfTheNumberOfEpisodesDatasetApp")

  episodesCount("tv_shows_data.csv")
}

//case class Line(Key: String, count: Int)

//case class Genre(value: String)

//case class GenreEpisodesCount(genre: Genre, count: Long)

trait CalculationOfTheNumberOfEpisodesDataset {
  this: SparkSessionProviderComponent =>

  private val sparkSession = sparkSessionProvider.sparkSession

  import sparkSession.implicits._

  def episodesCount(path: String): Unit = {

    val textDF: DataFrame = sparkSession.read
      .option("header", "true")
      .csv(path)
      .withColumn("No_of_Episodes", $"No_of_Episodes".cast(sql.types.LongType))
      .withColumnRenamed("No_of_Episodes", "count")
      .select("Genre", "count")
      .na.drop()

    textDF.groupBy("Genre").agg(sum("count").as("sum"))
    .sort(desc("sum"))
    .show(10)

 }

}
