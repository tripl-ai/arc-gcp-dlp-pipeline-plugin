package ai.tripl.arc.transform.gcp

import java.net.URI
import java.sql.DriverManager

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

//import ai.tripl.arc.util.TestUtils

case class DLPUser(user_id: String, name: String, dob: java.time.LocalDate, created: java.sql.Timestamp, active: java.lang.Boolean, age: Int, height: Float)

class DLPTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val port = 1080
  val inputView = "inputView"
  val outputView = "outputView"

  var requests = 0

  var userDF: DataFrame = _

  val testProjectId = sys.env.get("ARC_DLP_TRANSFORM_TEST_PROJECT_ID")
  val testRegion = sys.env.get("ARC_DLP_TRANSFORM_TEST_REGION")
  val testTemplateName = sys.env.get("ARC_DLP_TRANSFORM_TEST_TEMPLATE_NAME")

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Arc Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    import spark.implicits._

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    userDF = Seq(
        DLPUser("11111", "John Doe",
                  java.time.LocalDate.of(1994,7,12), 
                  java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2021, 1, 15, 11, 32, 0)),
                  true, 32, 183.5f),
        DLPUser("22222", "Alice Smith",
                  java.time.LocalDate.of(1982,3,7), 
                  java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2020, 3, 5, 22,54, 12)),
                  false, 12, 174.7f)
    ).toDF
  }

  after {
    session.stop
  }

  test("DLPTransform: Can de-identify data") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    (testProjectId, testRegion, testTemplateName) match {
        case (Some(projectId), Some(region), Some(templateName)) => {
            val df = userDF
            df.createOrReplaceTempView(inputView)

            val dlpStage = DLPTransformStage(
                plugin= new DLPTransform,
                id= None,
                name= "dlpUserTransform",
                description= None,
                inputView= inputView,
                outputView= outputView,
                params= Map.empty,
                persist= false,
                batchSize= 10,
                numPartitions= None,
                partitionBy= Nil,
                projectId= projectId,
                region= region,
                dlpTemplateName= templateName
            ) 

            val dataset = DLPTransformStage.execute(dlpStage).get

            dataset.cache.count

            val expectedUserDF = Seq(
                DLPUser("41479", "cPHK0d9bFeIgoDejmrOUrDchpDapWnmrKPAIMr3ZPfY=",
                          java.time.LocalDate.of(1994,4,28), 
                          java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2021, 1, 15, 11, 32, 0)),
                          true, 4, 2.0f),
                DLPUser("29591", "jBZm7CxKLihqr4MTqabX7Do+1SJqeezeg00Mtbs6UeA=",
                          java.time.LocalDate.of(1981,12,9), 
                          java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2020, 3, 5, 22,54, 12)),
                          false, 1, 1.0f)
            ).toDF

            /*println("User DF:")
            userDF.show(false)
            println("Expected: User DF:")
            expectedUserDF.show(false)
            println("Received DF:")
            dataset.show(false)*/

            val expected = expectedUserDF.select($"user_id", $"name", $"dob", $"age", $"height")
            val actual = dataset.select($"user_id", $"name", $"dob", $"age", $"height")

            assert(TestUtils.datasetEquality(expected, actual))
        }
        case _ =>
            println("Skipping DLPTransform test as project id, region or template name not set")
    }

  }


}
