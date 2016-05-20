/**
  * Created by edwardcannon on 12/04/2016.
  */

package object streaming {

}
import java.io.File
import org.apache.spark.streaming.twitter._
import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Collect at least the specified number of tweets into json text files.
  */
object TwitterStreaming {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    val outputDir = new File(args(0))
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        ""))
      System.exit(1)
    }
    outputDir.mkdirs()

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    System.setProperty("twitter4j.oauth.consumerKey",args(1))
    System.setProperty("twitter4j.oauth.consumerSecret",args(2))
    System.setProperty("twitter4j.oauth.accessToken",args(3))
    System.setProperty("twitter4j.oauth.accessTokenSecret",args(4))
    val dataToFilterOn = Array("camping")
    val tweetStream = TwitterUtils.createStream(ssc, None,dataToFilterOn).map(gson.toJson(_))

    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        outputRDD.saveAsTextFile(args(0) + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > 100) {
          System.exit(0)
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}