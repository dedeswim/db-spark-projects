package utils

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.Numeric.Implicits._

object Utils {
  def runOnCluster(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-15")
      .getOrCreate()

    spark
  }

  def runOnLocal(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-15")
      .master("local[*]")
      .getOrCreate()

    spark
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(Utils.variance(xs))

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)
    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def getFilePath(cluster: Boolean, filename: String): String = {
    if (cluster) {
      s"/user/cs422/$filename"
    } else {
      s"/$filename"
    }
  }

  def loadRDD(sqlContext: SQLContext,
              file: String,
              cluster: Boolean,
              limit: Int = 4000,
              toLimit: Boolean = true,
              delimiter: String = ",",
              header: String = "false"): RDD[Row] = {

    val partial =
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", header)
        .option("inferSchema", "true")
        .option("delimiter", delimiter)

    val loaded = if (cluster) {
      partial
        .load(file)
    } else {
      val input = new File(getClass.getResource(file).getFile).getPath
      partial
        .load(input)
    }

    if (toLimit) {
      loaded
        .limit(limit)
        .rdd
    }

    loaded.rdd
  }
}
