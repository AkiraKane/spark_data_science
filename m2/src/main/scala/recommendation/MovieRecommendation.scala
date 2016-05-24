package recommendation
import java.io.File
/**
  * Created by edwardcannon on 24/05/2016.
  */
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import utils.SetUpSpark

/**
  * Container for user
  * @param userId - User ID
  * @param gender - Character representing gender of user
  * @param age - Int age of user
  * @param occupation - String representation of occupation
  * @param zipcode - User zip code
  */
case class User (userId : Int, gender : String, age : Int,
                 occupation : Int, zipcode : String)

object MovieRecommendation {

  def main(args: Array[String]) : Unit = {
    val DIR = args(0)
    val sc = SetUpSpark.configure()

    //Load ratings data
    val ratings = sc.textFile(new File(DIR, "ratings.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    //Load movies data
    val movies = sc.textFile(new File(DIR, "movies.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collect().toMap

    //Load user data
    val users = sc.textFile(new File(DIR,"users.dat").toString).map { line =>
      val fields = line.split("::") //1::F::1::10::48067
      (User(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt,fields(4)))
    }
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)
  }
}

