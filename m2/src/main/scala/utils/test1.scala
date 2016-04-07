package utils

/**
  * Created by edwardcannon on 06/02/2016.
  * //spark://localhost:7077 // only for a cluster
  * Utility functions for Spark
  * //Register RDD as a temp table
    //df.registerTempTable("df")
    //` the back stick is used to select columns with white space in the name!
    //val df1 = sqlContext.sql("SELECT `Campaign ID`, `Interest Targeting`, `Campaign Budget`, impressions FROM df WHERE impressions > 19890 ").collect().foreach(println)
    //val res2 = df.groupBy("Campaign ID","Interest Targeting").count()
  */

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode, _}
import org.apache.spark.{SparkConf, SparkContext}

class CSVReader(val fName : String){
  var fileName: String = fName

  /***
    * Reads all lines of a file
    *
    * @return String Iterator over all file lines
    */
  def readFile(): Iterator[String] ={
    val lines: Iterator[String] = scala.io.Source.fromFile(fileName).getLines()
    return lines
  }
}
object SetUpSpark {
  def configure(): SparkContext ={
    val conf = new SparkConf().setAppName("tester").setMaster("local")
      .set("spark.driver.port","7078")
      .set("spark.broadcast.port","7079")
      .set("spark.replClassServer.port","7080")
      .set("spark.blockManager.port","7081")
      .set("spark.executor.port","7082")
    val sc = new SparkContext(conf)
    return sc
  }
}

class SparkCSVHelper {
  var iFile: String = ""
  var oFile: String = ""
  var df: org.apache.spark.sql.DataFrame = null

  def setInputFile(inputFile : String) : Unit = {
    iFile = inputFile
  }

  def setOutputFile(outputFile : String) : Unit = {
    oFile = outputFile
  }

  def createDataFrame(sc : SQLContext) : org.apache.spark.sql.DataFrame = {
    df = sc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(iFile)
    return df
  }
}

object DataFrameHelper {
  /**
    * Drops multiple columns from DataFrame
    *
    * @param inputDF - Input data frame
    * @param cols2Drop - Array containing column names to drop
    * @return - Reduced data frame
    */
  def DropMultipleColumns(inputDF : org.apache.spark.sql.DataFrame, cols2Drop : Array[String]): DataFrame ={
    val columnsToKeep: Array[Column] = inputDF.columns.diff(cols2Drop).map(x => inputDF.col(x))
    val newDataFrame: DataFrame = inputDF.select(columnsToKeep: _*)
    return newDataFrame
  }

  /**
    * Outputs a data frame to a single file (part)
    *
    * @param df - Input data frame
    * @param outFile - Output file
    */
  def SaveDataFrameWithOnePartition(df : org.apache.spark.sql.DataFrame,outFile : String): Unit = {
    df.repartition(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header","true")
      .save(outFile)
  }

  /**
    * Adds a binary label to a dataframe for ML binary classification
    *
    * @param df - Input data frame
    * @param threshold - Threshold criteria for label
    * @param inputColumn2Transform - Input column to apply threshold to get labeled column
    * @return
    */
  def AddBinaryNumericLabels(df : org.apache.spark.sql.DataFrame, threshold : Int,
                             inputColumn2Transform : String): DataFrame ={
    val addNumericLabels: (Int => Double) = (arg: Int) => {if (arg < threshold) 0.0 else 1.0}
    val addNumericLabelsFunc = udf(addNumericLabels)
    val labeled_df = df.withColumn("label", addNumericLabelsFunc(col(inputColumn2Transform)))
    return labeled_df
  }

  /**
    * Replaces NAs in data frame with value
    *
    * @param df - input data frame
    * @param fillValue - value to replace NAs
    * @param cols2Apply2 - array of column names to apply to
    * @return
    */
  def Replace_NAs(df: org.apache.spark.sql.DataFrame,fillValue : Int,
                  cols2Apply2 : Array[String]) : DataFrame = {
    return df.na.fill(fillValue,cols2Apply2)
  }

  /**
    * Vectorize column in data frame as input features
    * @param df - input data frame
    * @param cols2Vectorize - columns to vectorize
    * @return
    */
  def CreateFeatureVector(df : org.apache.spark.sql.DataFrame,cols2Vectorize : Array[String]): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(cols2Vectorize)
      .setOutputCol("features")
    val output = assembler.transform(df)
    return output.select("label","features")
  }

  /**
    * Split data frame into training and test set
    * @param df - input data frame
    * @param train - train set fraction
    * @param test - test set fraction
    * @return
    */
  def RandomSplitTrainingTestSet(df : org.apache.spark.sql.DataFrame,train : Double,
                                 test : Double) : (DataFrame,DataFrame) ={
    val splits = df.randomSplit(Array(train, test))
    return (splits(0), splits(1))
  }

  /**
    * Convert Data frame to RDD[LabeledPoint] for ML
    * @param df - input data frame
    * @return
    */
  def MapData2LabeledPoint(df : org.apache.spark.sql.DataFrame) : RDD[LabeledPoint] = {
    val processed = df.map(row => LabeledPoint(row.getDouble(0),row.getAs[org.apache.spark.mllib.linalg.Vector](1)))
    return processed
  }
}

object MLHelper {

  /**
    * Trains simple logistic regression model
    *
    * @param trainingData - training data frame
    * @param maxIter - Number of iterations
    * @return
    */
  def TrainLogisticRegression(trainingData : org.apache.spark.sql.DataFrame,maxIter :Int) : LogisticRegressionModel = {
    val lr = new LogisticRegression().setMaxIter(maxIter)
    val model = lr.fit(trainingData)
    return model
  }

  /**
    * Run logistic regression model on test data
    *
    * @param testData - test data frame
    * @param mod - logistic regression model
    * @return
    */
  def TestLogisticRegression(testData : org.apache.spark.sql.DataFrame,mod : LogisticRegressionModel) : Array[Row] = {
    val results = mod.transform(testData).collect()
    return results
  }

  /**
    * Build random Forest Model
    * @param trainingData - Input RDD[LabeledPoint]
    * @param numClasses
    * @param numTrees
    * @param featureSubsetStrategy
    * @param impurity
    * @param maxDepth
    * @param maxBins
    * @return
    */
  def TrainRandomForest(trainingData : RDD[LabeledPoint],numClasses : Int = 2,
                        numTrees : Int = 3,featureSubsetStrategy : String = "auto",
                        impurity : String = "gini", maxDepth : Int = 4,
                        maxBins : Int = 32) : RandomForestModel = {
    val categoricalFeaturesInfo = Map[Int, Int]()
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    return model
  }

  /**
    * Test RandomForest Model accuracy on Testdata
    * @param testData - RDD[LabeledPoint] for test data
    * @param rfm - Built RandomForest Model
    */
  def TestRandomForestModel(testData : RDD[LabeledPoint],rfm : RandomForestModel) : RDD[(Double, Double)] = {
    val labelAndPreds = testData.map { point =>
      val prediction = rfm.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + rfm.toDebugString)
    return labelAndPreds
  }
}

object test1 {
  def main(args: Array[String]): Unit = {
    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val csvHelperSpark = new SparkCSVHelper()
    csvHelperSpark.setInputFile(args(0))
    val df = csvHelperSpark.createDataFrame(sqlContext)
    val res2 = df.groupBy("Campaign ID","Interest Targeting").count()
    DataFrameHelper.SaveDataFrameWithOnePartition(res2,args(1))
    val dfy = DataFrameHelper.AddBinaryNumericLabels(df,8000,"`Campaign Budget`")
    val colList = Array("Campaign Name","Date","Objective")
    val reduced_df = DataFrameHelper.DropMultipleColumns(dfy,colList)
    val dfy2 = DataFrameHelper.Replace_NAs(dfy,0,Array("engagements","hashtagclicks","impressions","usdspend","spend","label"))
    val selection_df = dfy2.select("engagements","hashtagclicks","impressions","usdspend","spend","label")
    val full_mat = DataFrameHelper.CreateFeatureVector(selection_df,Array("engagements","hashtagclicks","impressions","usdspend","spend"))
    val (trainingData,testData) = DataFrameHelper.RandomSplitTrainingTestSet(full_mat,0.7,0.3)
    val trainedModel = MLHelper.TrainLogisticRegression(trainingData,40)
    val results = MLHelper.TestLogisticRegression(testData,trainedModel)
    for (x <- results){
      println(x.toString())
    }
    //Transform Dataframe to RDD[LabeledPoint]
    val parsedData = DataFrameHelper.MapData2LabeledPoint(trainingData)
    val testDataMapped = DataFrameHelper.MapData2LabeledPoint(testData)
    //Built RandomForest Model
    val randomForestBuiltModel = MLHelper.TrainRandomForest(parsedData)
    // Evaluate model on test instances and compute test error
    val rfResults = MLHelper.TestRandomForestModel(testDataMapped,randomForestBuiltModel)
    val metrics = new BinaryClassificationMetrics(rfResults)
    val auROC = metrics.areaUnderROC()
    println("Area under ROC ="+auROC)
  }
}
