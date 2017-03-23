package timeusage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


//  lazy val spark = SparkSession
//    .builder()
//    .master("local[3]")
//    .appName("timeusage-test")
////    .config("spark.ui.enabled", "false")
//    //    .config("spark.eventLog.dir", "/tmp/spark-events")
//    //    .config("spark.eventLog.enabled", "true")
//    .getOrCreate()

  lazy val timeUsage = TimeUsage


  lazy val (columns, initDf) = timeUsage.read("/timeusage/atussum-10000.csv")
  lazy val (primaryNeedsColumns, workColumns, otherColumns) = timeUsage.classifiedColumns(columns)
  lazy val summaryDf:DataFrame = timeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  lazy val finalDf:DataFrame = timeUsage.timeUsageGrouped(summaryDf)



//  lazy val lines = spark.read.csv(getClass.getResource("/timeusage/atussum-10000.csv").getPath)


  test("timeUsage") {
    assert(timeUsage.spark.sparkContext.appName === "Time Usage")
    assert(timeUsage.spark.sparkContext.isStopped === false)
  }

  test("dfSchema"){
    val testSchema = timeUsage.dfSchema(List("fieldA", "fieldB"))

    assert(testSchema.fields(0).name === "fieldA")
    assert(testSchema.fields(0).dataType === StringType)
    assert(testSchema.fields(1).name === "fieldB")
    assert(testSchema.fields(1).dataType === DoubleType)
  }

  test("read") {
    columns.foreach(println)
    //    raw.take(10).foreach(println)
    //
    //    println(raw.toDebugString)
    //
    //    assert(raw.count() === 100000)
  }

}
