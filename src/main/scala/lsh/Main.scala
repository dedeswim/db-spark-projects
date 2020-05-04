package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import lsh.ExactNN


object Main {
  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */

    0.0
  }

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double = {
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */

    0.0
  }

  def query1 (sc : SparkContext, sqlContext : SQLContext) : Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-1.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact : Construction = null

    val lsh : Construction = null

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(recall(ground, res) > 0.7)
    assert(precision(ground, res) > 0.98)
  }

  def query2 (sc : SparkContext, sqlContext : SQLContext) : Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-2.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact : Construction = null

    val lsh : Construction = null

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(recall(ground, res) > 0.9)
    assert(precision(ground, res) > 0.45)
  }

  def query0 (sc : SparkContext, sqlContext : SQLContext) : Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

//    val rdd_corpus = loadRDD(sqlContext, "/lsh-corpus-small.csv")

    val query_file = new File(getClass.getResource("/lsh-query-0.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

//    val rdd_query = loadRDD(sqlContext, "/lsh-query-0.csv")

    val exact : Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)

    val lsh : Construction = new BaseConstruction(sqlContext, rdd_corpus)

    val ground = exact.eval(rdd_query)
//    ground.collect().foreach(println)
    val res = lsh.eval(rdd_query)
    res.collect().foreach(println)

    assert(recall(ground, res) > 0.83)
    assert(precision(ground, res) > 0.70)
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    query0(sc, sqlContext)
    //query1(sc, sqlContext)
    //query2(sc, sqlContext)
  }

  def loadRDD(sqlContext: SQLContext, file: String): RDD[(String, List[String])] = {
    //val input = new File(getClass.getResource(file).getFile).getPath
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .load(s"/user/cs422$file")
      .rdd
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
  }
}
