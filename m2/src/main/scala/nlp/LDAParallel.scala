package nlp

/**
  * Created by edwardcannon on 16/03/2016.
  */

import org.apache.spark.mllib.clustering.LDA
//import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.clustering.OnlineLDAOptimizer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}

import utils.SetUpSpark

object LDAMain {

  def createDataFrame(sc : SQLContext, iFile : String) : org.apache.spark.sql.DataFrame = {
    val df = sc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(iFile)
    return df
  }

  def main(args: Array[String]): Unit = {
    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val numTopics: Int = 10 //upto 10K
    val maxIterations: Int = 100 //upto 100K
    val vocabSize: Int = 10000 //18739
    val df = LDAMain.createDataFrame(sqlContext, args(0))
    df.columns.foreach(println)
    val df1 = df.select("Index","`case_description`").withColumnRenamed("case_description","text")
      .withColumnRenamed("Index","docId")

    println(df1.show())
    // Split each document into words
    val tokens = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\p{L}+")
      .setInputCol("text")
      .setOutputCol("words")
      .transform(df1)
    println(tokens.show())
    // Filter out stopwords
    val stopwords: Array[String] = Array("the", "and", "if", "but","I")
    val filteredTokens = new StopWordsRemover()
        .setStopWords(stopwords)
        .setCaseSensitive(false)
        .setInputCol("words")
        .setOutputCol("filtered")
        .transform(tokens)

    println(filteredTokens.show())

    // Limit to top `vocabSize` most common words and convert to word count vector features
    val cvModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setVocabSize(vocabSize)
      .fit(filteredTokens)

    val countVectors = cvModel.transform(filteredTokens)
    println(countVectors.show())
    val x = countVectors.select("docId","features").limit(20).collect()
    for (xy <-x){
      println(xy)
    }
     val res = countVectors.select("docId", "features").limit(10).map {
        case Row(docId: Long, countVector: Vector) => (docId, countVector)
        }
        .cache()

    /**
      * Configure and run LDA
      */
    val mbf = {
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      val corpusSize = countVectors.count()
      2.0 / maxIterations + 1.0 / corpusSize
    }
    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf)))
      .setK(numTopics)
      .setMaxIterations(2)
      .setDocConcentration(-1) // use default symmetric document-topic prior
      .setTopicConcentration(-1) // use default symmetric topic-word prior

    val startTime = System.nanoTime()
    val ldaModel = lda.run(res)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    /**
      * Print results.
      */
    println(s"Finished training LDA model.  Summary:")
    println(s"Training time (sec)\t$elapsed")
    println(s"==========")

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val vocabArray = cvModel.vocabulary
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabArray(_)).zip(termWeights)
    }
    println(s"$numTopics topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) => println(s"$term\t$weight") }
      println(s"==========")
    }
  }
}
