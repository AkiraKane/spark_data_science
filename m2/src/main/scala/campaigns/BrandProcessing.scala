package campaigns

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SQLContext}
import utils.{DataFrameHelper, SetUpSpark}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by edwardcannon on 15/05/2016.
  */
object BrandProcessing {

  def createDataFrame(sc : SQLContext, iFile : String) : org.apache.spark.sql.DataFrame = {
    val df = sc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(iFile)
    return df
  }

  /**
    * Maps campaign type to an integer
    *
    * @return
    */
  def productTypeToIntegerUdf = udf((productType: String) => {
    productType match {
      case "Followers" => 0
      case "Qualified impressions" => 1
      case "Tweet engagements" => 2
      case "Tweets (custom)" => 3
      case "Video views" => 4
      case _ => 5
    }})

  /**
    * Train Linear Regression model with Stochastic Gradient Descent
    * @param labelledPointData - RDD of LabeledPoint (training data)
    * @param numIterations - Integer representing number of iterations
    * @param stepSize - Double representing step size
    */
  def trainLinearRegressionWithSGD(labelledPointData : RDD[LabeledPoint],
                                   numIterations : Int,
                                   stepSize : Double) : Unit = {
    val model = LinearRegressionWithSGD.train(labelledPointData, numIterations, stepSize)
    val valuesAndPreds = labelledPointData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    valuesAndPreds.foreach(println)
  }

  /**
    * Decision tree regression model
    * @param data Input dataframe
    * @param trainSplit Fraction for train data
    * @param maxDepth Max tree depth
    * @param bins Max number of bins
    */
  def trainDecisionTreeRegressor(data : RDD[LabeledPoint],trainSplit : Double,
                                 maxDepth : Int, bins : Int) : Unit = {
    val test = 1-trainSplit
    val splits = data.randomSplit(Array(trainSplit, test))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v - p, 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression tree model:\n" + model.toDebugString)
  }

  /**
    * Calculates covariance & correlation coefficient between selected attribute
    * and all other attributes in a data frame
    * @param df DataFrame
    * @param attrOfInterest Attribution of Interest
    */
  def evaluateAttributeCovarianceAndCorrelation(df : DataFrame,attrOfInterest : String) : Unit = {
    var covarianceValues = ArrayBuffer[Double]()
    var correlationValues = ArrayBuffer[Double]()
    for(col <- df.columns)
    {
      covarianceValues += df.stat.cov(attrOfInterest,col)
      correlationValues += df.stat.corr(attrOfInterest,col)
    }
    (covarianceValues,correlationValues)
  }

  def main(args: Array[String]) : Unit = {
    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //brand twitter campaigns
    val brandDf = createDataFrame(sqlContext,args(0))
    val reducedDf = DataFrameHelper.DropMultipleColumns(brandDf,
      Array("App click rate","app clicks","Pre-roll video played 25%",
      "Pre-roll video played 50%","Pre-roll video played 75%","Pre-roll video views",
      "call to action","campaign id","campaign","campaign url",
      "card engagements","cost per lead","currency","eCPAC","funding instrument name",
      "lead rate","leads","end date","start date","time","video starts",
      "video views","Z-Scaled_vid_75%","Pre-roll video completions"))
    val redDF = reducedDf.withColumn("productTypeInt",productTypeToIntegerUdf(reducedDf("product type")))
      .drop("product type")
    redDF.show(10)
    //Summarize Conversions per engagement, plus some extra attrs: count, mean, stdev, min, max
    redDF.describe("CPE","clicks","current total budget","impressions").show()
    //BrandProcessing.evaluateAttributeCovarianceAndCorrelation(redDF,"CPE")
    var features2Select:Array[String] = Array("clicks", "current total budget", "impressions")
    val redDF1 = redDF.withColumnRenamed("CPE", "label")
    val featurizedVectors = DataFrameHelper.CreateFeatureVector(redDF1,features2Select)
    val labelledPointData = DataFrameHelper.MapData2LabeledPoint(featurizedVectors)
  }
}
