/**
  * Created by edwardcannon on 07/04/2016.
  */
package tutorial

import java.util.Date

import scala.collection.mutable.ArrayBuffer

case class TemperatureForecastInfo(name : String, dat : Date, avgError : Long){
      var location = name
      var date = dat
      var error = avgError
}

object tutes {

  def toInt(s: String): Option[Int] = {
    try {
      Some(Integer.parseInt(s.trim))
    } catch {
      // catch Exception to catch null 's'
      case e: Exception => None
    }
  }

  def main(args: Array[String]): Unit = {
    val numbers = Array(1,2,3,4)
    val num1 = Array(5,6,7)
    val bignumbers = numbers.map(_*2)
    val largerArray = numbers++num1 //append one array to another
    //print(numbers.contains(3))
    val x = numbers.filter(_ >2)
    x.foreach(println)
    val strings = Seq("1", "2", "foo", "3", "bar")
    strings.map(toInt)
    val source = scala.io.Source.fromFile(args(0)).getLines.drop(3)
    val listTempObjs = ArrayBuffer[TemperatureForecastInfo]()
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
    for (line <- source){
      val data = line.split(",")
      val tfi = TemperatureForecastInfo(data(0), format.parse(data(1)), data(2).toLong)
      listTempObjs+=tfi
    }
    for (v <- listTempObjs){
      println(v.location)
    }
  }

}
