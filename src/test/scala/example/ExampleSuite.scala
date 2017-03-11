package example

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

@RunWith(classOf[JUnitRunner])
class ExampleSuite extends FunSuite with BeforeAndAfterAll {

  def initializeExample(): Boolean =
    try {
      Example
      true
    } catch {
      case _: Throwable => false
    }

  override def afterAll(): Unit = {
    assert(initializeExample(), " -- did you fill in all the values in Example (conf, sc)?")
    import Example._
    sc.stop()
  }


  test("'sumOfPlusOnes List(1, 2, 3, 4, 5)' should be equal to 20") {

    assert(initializeExample(), " -- did you fill in all the values in Example (conf, sc)?")
    import Example._
    assert(sumOfPlusOnes == 20)
  }

}

