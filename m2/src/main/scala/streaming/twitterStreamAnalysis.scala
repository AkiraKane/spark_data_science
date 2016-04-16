package streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.google.gson.{JsonParser}
import com.google.gson.Gson

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
/**
  * Created by edwardcannon on 16/04/2016.
  */
object twitterStreamAnalysis {

  /**
    * Converts tweet to vector
    * @param s Text string
    * @return
    */
  def featurize(s: String): Vector = {
    val n = 1000
    val result = new Array[Double](n)
    val bigrams = s.sliding(2).toArray
    for (h <- bigrams.map(_.hashCode % n)) {
      result(h) += 1.0 / bigrams.length
    }
    Vectors.sparse(n, result.zipWithIndex.filter(_._1 != 0).map(_.swap))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    var gson = new Gson()
    val jsonParser = new JsonParser()
    val tweets = sc.textFile(args(0))
    for (tweet <- tweets.take(5)) {
      println(gson.toJson(jsonParser.parse(tweet)))
    }
    val sqlContext = new SQLContext(sc)
    //auto infer schema from json
    val tweetTable = sqlContext.read.json(args(1))
    tweetTable.registerTempTable("tweetTable")
    tweetTable.printSchema()
    sqlContext.sql(
    "SELECT user.lang, COUNT(*) as cnt FROM tweetTable " +
      "GROUP BY user.lang ORDER BY cnt DESC limit 100").collect().foreach(println)
    println("--- Training the model and persist it")
    val texts = sqlContext.sql("SELECT text from tweetTable").map(_.toString)
    val vectors = texts.map(featurize).cache()
    println(vectors.count())
    val model = KMeans.train(vectors, 10, 20)
    val some_tweets = texts.take(100)
    for (i <- 0 until 10) {
      println(s"\nCLUSTER $i:")
      some_tweets.foreach { t =>
        if (model.predict(featurize(t)) == i) {
          println(t)
        }
      }
    }
    // persist the model to disk, so we can use it for streaming
    //sc.makeRDD(model.clusterCenters, 10).saveAsObjectFile("model")
  }
}
