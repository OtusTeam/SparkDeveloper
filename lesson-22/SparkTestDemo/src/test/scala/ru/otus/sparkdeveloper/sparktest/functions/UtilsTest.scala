package ru.otus.sparkdeveloper.sparktest.functions

import Utils._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FlatSpec}
import ru.otus.sparkdeveloper.sparktest.SparkSessionTestWrapper

class UtilsTest extends FlatSpec with SparkSessionTestWrapper
  with Logging with BeforeAndAfter with DataFrameComparer {

  var testDf: DataFrame = _
  var studentDf: DataFrame = _
  var countriesDf: DataFrame = _

  before {

    testDf = spark.read.format("json")
      .option("mode", "FAILFAST")
      .option("multiline", true)
      .load("src/test/resources/customer-data.json")


  }

  it should "print testDf schema and show" in {
    testDf.printSchema
    testDf.show
  }

  it should "return correct interaction list" in {

    import spark.implicits._

    val expectedDf = Seq("WEBSITE", "TWITTER", "INSTAGRAM").toDF("type")

    val resultDf = getAllInteractionTypes(testDf)

    //val resultDf = testDf.getAllInteractionTypes getAllInteractionTypes(testDf)

    assertSmallDatasetEquality(expectedDf, resultDf, orderedComparison = false)

  }

}
