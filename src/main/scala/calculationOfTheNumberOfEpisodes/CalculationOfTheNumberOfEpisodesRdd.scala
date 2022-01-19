package calculationOfTheNumberOfEpisodes

import org.apache.spark.rdd.RDD
import provider.{DefaultSparkContextProvider, SparkContextProviderComponent}
import scala.util.Try

object CalculationOfTheNumberOfEpisodesRddApp extends App
  with SparkContextProviderComponent
  with CalculationOfTheNumberOfEpisodesRdd {

  override def sparkContextProvider = new DefaultSparkContextProvider("CalculationOfTheNumberOfEpisodesRddApp")

  genreCount("tv_shows_data.csv")
}

trait CalculationOfTheNumberOfEpisodesRdd {
  this: SparkContextProviderComponent =>

  private val sparkContext = sparkContextProvider.sparkContext

  def genreCount(path: String): Unit = {
    val textFile: RDD[String] = sparkContext.textFile(path)

    val textFile1 = textFile
      .filter(_!= "Title,Genre,Premiere,No_of_Seasons,No_of_Episodes")
      .map(_.split(","))
      .map(elem => (elem(1),Try(elem(5).toInt).getOrElse(0)))
      .reduceByKey(_ + _)

    textFile1.sortBy({ case (genre, count) => count }, ascending = false)
      .take(10)
      .foreach { case (genre, count) =>
        println(s"========== '$genre' : $count")
      }
  }

}
