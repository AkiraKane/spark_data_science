package nlp
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._

import utils.SetUpSpark

/**
  * Created by edwardcannon on 17/05/2016.
  */
object StanfordNLPTK {

  def main (args: Array[String]): Unit = {
    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val input = Seq(
      (1, "<xml>Cambridge University is located in Cambridge. It is the best University in england and Europe.</xml>")
    ).toDF("id", "text")

    val output = input
      .select(cleanxml('text).as('doc))
      .select(explode(ssplit('doc)).as('sentence))
      .select('sentence, tokenize('sentence).as('words), ner('sentence).as('nerTags))
    output.show(truncate = false)
  }
}
