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

case class DLPUser(user_id: String, name: String, dob: String)

class DLPTransformSuite extends FunSuite with BeforeAndAfter {
  import spark.implicits._

  var session: SparkSession = _
  val port = 1080
  val server = new Server(port)
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

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    userDF = Seq(
        DLPUser("11111", "John Doe", "12/07/1994"),
        DLPUser("22222", "Alice Smith", "07/03/1982")
    ).toDF
  }

  after {
    session.stop
    try {
      server.stop
    } catch {
      case e: Exception =>
    }
  }

  test("DLPTransform: Can de-identify data") {
    implicit val spark = session
    //import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext()

    (testProjectId, testRegion, testTemplateName) match {
        case None =>
            logger.warn("Skipping DLPTransform test as project id, region or template name not set")
        case (Some(projectId), Some(region), Some(templateName)) => {
            val df = userDF
            df.createOrReplaceTempView(inputView)

            val dlpStage = DLPTransformStage(
                plugin: new DLPTransform,
                id: None,
                name: "dlpUserTransform",
                description: None,
                inputView: inputView,
                outputView: outputView,
                params: Map.empty,
                persist: false,
                batchSize: 10,
                numPartitions: None,
                partitionBy: Nil,
                projectId: projectId,
                region: region,
                dlpTemplateName: templateName
            ) 

            val dataset = DLPTransformStage.execute(dlpStage).get

            dataset.cache.count

            val expectedUserDF = Seq(
                DLPUser("41479", "John Doe", "12/07/1994"),
                DLPUser("29591", "Alice Smith", "07/03/1982")
            ).toDF

            println("User DF:")
            userDF.show(false)
            println("Expected: User DF:")
            expectedUserDF.show(false)
            println("Received DF:")
            dataset.show(false)

            val expected = expectedUserDF.select(col("user_id"))
            val actual = dataset.select(col("user_id"))

            assert(TestUtils.datasetEquality(expected, actual))
        }
    }

  }

}
