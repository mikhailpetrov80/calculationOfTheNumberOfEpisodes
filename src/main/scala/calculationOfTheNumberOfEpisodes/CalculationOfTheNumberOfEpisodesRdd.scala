package calculationOfTheNumberOfEpisodes

import org.apache.spark.rdd.RDD
import provider.{DefaultSparkContextProvider, SparkContextProviderComponent}

object CalculationOfTheNumberOfEpisodesRddApp extends App
  with SparkContextProviderComponent
  with CalculationOfTheNumberOfEpisodesRdd {

  override def sparkContextProvider = new DefaultSparkContextProvider("CalculationOfTheNumberOfEpisodesRddApp")

  wordCount("tv_shows_data.csv")
}

trait CalculationOfTheNumberOfEpisodesRdd {
  this: SparkContextProviderComponent =>

  private val sparkContext = sparkContextProvider.sparkContext

  def wordCount(path: String): Unit = {
    val textFile: RDD[String] = sparkContext.textFile(path)


    val counts: RDD[(String, Int)] = textFile//.flatMap(line => line.split(","))
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.sortBy({ case (word, count) => count }, ascending = false)
      .take(10)
      .foreach { case (word, count) =>
        println(s"========== '$word' : $count")
      }
  }

}
