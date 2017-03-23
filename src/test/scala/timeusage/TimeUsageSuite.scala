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

  test("row"){
    val testRow = timeUsage.row(List("fieldA", "0.3", "1"))

    assert(testRow(0).getClass.getName === "java.lang.String")
    assert(testRow(1).getClass.getName === "java.lang.Double")
    assert(testRow(2).getClass.getName === "java.lang.Double")
  }

  test("read") {
    assert(columns.size === 455)
    assert(initDf.count === 10000-1)
    initDf.show()
  }

  test("classifiedColumns") {
    val pnC = primaryNeedsColumns.map(_.toString)
    val wC = workColumns.map(_.toString)
    val oC = otherColumns.map(_.toString)


    assert(pnC.contains("t010199"))
    assert(pnC.contains("t030501"))
    assert(pnC.contains("t110101"))
    assert(pnC.contains("t180382"))
    assert(wC.contains("t050103"))
    assert(wC.contains("t180589"))
    assert(oC.contains("t020101"))
    assert(oC.contains("t180699"))

  }

  test("timeUsageSummary"){
    summaryDf.show()
  }

}
