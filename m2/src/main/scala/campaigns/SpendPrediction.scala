package campaigns
import utils.{SetUpSpark,SparkCSVHelper}
/**
  * Created by edwardcannon on 15/03/2016.
  * Predict spend for campaign example
  */

class SpendPrediction {

}

object DataFramePreprocess {
  /**
    * Iterates over a data frame, to get number of distinct values per column
    * @param df - input data frame
    * @return - Map[Col Name:Distinct Count]
    */
  def GetDistinctEntriesPerColumn(df : org.apache.spark.sql.DataFrame): collection.mutable.Map[String,Long] ={
    val columnNames = df.columns
    var df_column_count_map = collection.mutable.Map[String, Long]()
    for(col <- columnNames){
      df_column_count_map(col) = df.select(col).distinct().count()
    }
    return df_column_count_map
  }
}

object SpendPredictionMain {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("No input table <inputPath>")
        return
        }

    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val csvHelperSpark = new SparkCSVHelper()
    csvHelperSpark.setInputFile(args(0))
    val df = csvHelperSpark.createDataFrame(sqlContext)
    val distinctColCounterMap = DataFramePreprocess.GetDistinctEntriesPerColumn(df)
    println(distinctColCounterMap)

  }
}