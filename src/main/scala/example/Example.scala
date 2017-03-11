
package example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD


object Example {
  val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local")

  val sc: SparkContext = new SparkContext(conf)

  def sumOfPlusOnes = sc.parallelize(List(1, 2, 3, 4, 5)).map(_+1).reduce(_+_)
}
