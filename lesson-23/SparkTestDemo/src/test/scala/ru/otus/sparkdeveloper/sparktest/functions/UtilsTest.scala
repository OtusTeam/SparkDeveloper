package ru.otus.sparkdeveloper.sparktest.functions

import Utils._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import ru.otus.sparkdeveloper.sparktest.SparkSessionTestWrapper

class UtilsTest
    extends AnyFlatSpec
    with SparkSessionTestWrapper
    with Logging
    with BeforeAndAfter
    with DataFrameComparer {

  var testDf: DataFrame = _
  var studentDf: DataFrame = _
  var countriesDf: DataFrame = _

  before {

    testDf = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .option("multiline", value = true)
      .load("src/test/resources/customer-data.json")
  }

  it should "print testDf schema and show" in {
    testDf.printSchema
    testDf.show
  }

  it should "contain all necessary columns" in {
    val necessaryCols = Seq("Accounts", "Name")
    assert(necessaryCols.forall(testDf.columns.contains))
  }

  it should "return correct interaction list" in {

    import spark.implicits._

    val expectedDf =
      Seq("INSTAGRAM", "TWITTER", "WEBSITE").toDF("type")

    val resultDf = getAllInteractionTypes(testDf)

    assertSmallDatasetEquality(expectedDf, resultDf, orderedComparison = false)
  }
}
