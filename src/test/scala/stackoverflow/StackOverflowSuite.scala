package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  lazy val conf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("teststack")
  lazy val sc: SparkContext = new SparkContext(conf)

  println(getClass.getResource("/stackoverflow/stackoverflow-100000.csv"))
  lazy val lines   = sc.textFile(getClass.getResource("/stackoverflow/stackoverflow-100000.csv").getPath)
  lazy val raw = testObject.rawPostings(lines)

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("rawPostings"){
    val rawList = raw.collect().toList
//    rawList.take(10).foreach(println)
    assert(rawList.length == 100000)
  }

}
