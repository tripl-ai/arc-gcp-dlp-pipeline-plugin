package ai.tripl.arc.transform

import java.net.URI

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


import com.google.cloud.dlp.v2.DlpServiceClient
import com.google.privacy.dlp.v2._

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils

class DLPTransform extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "DLPTransform",
    |  "name": "DLPTransform",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "outputView": "outputView",
    |  "projectId": "gcp project id",
    |  "region": "gcp region",
    |  "dlpTemplateName": "FQN of DLP Template"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/transform/#DLPTransform")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: "batchSize" :: "numPartitions" :: "partitionBy" :: "projectId" :: "region" :: "dlpTemplateName" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val batchSize = getValue[Int]("batchSize", default = Some(1))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val projectId = getValue[String]("projectId")
    val region = getValue[String]("region")
    val dlpTemplateName = getValue[String]("dlpTemplateName")

    (id, name, description, inputView, outputView, persist, batchSize, numPartitions, partitionBy, projectId, region, dlpTemplateName, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(outputView), Right(persist), Right(batchSize), Right(numPartitions), Right(partitionBy), Right(projectId), Right(region), Right(dlpTemplateName), Right(invalidKeys)) =>

        val stage = DLPTransformStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          params=params,
          persist=persist,
          batchSize=batchSize,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          projectId=projectId,
          region=region,
          dlpTemplateName=dlpTemplateName
        )

        numPartitions.foreach { numPartitions => stage.stageDetail.put("numPartitions", Integer.valueOf(numPartitions)) }
        stage.stageDetail.put("batchSize", java.lang.Integer.valueOf(batchSize))
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("projectId", projectId)
        stage.stageDetail.put("region", region)
        stage.stageDetail.put("dlpTemplateName", dlpTemplateName)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, outputView, persist, batchSize, numPartitions, partitionBy, projectId, region, dlpTemplateName, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class DLPTransformStage(
    plugin: DLPTransform,
    id: Option[String],
    name: String,
    description: Option[String],
    inputView: String,
    outputView: String,
    params: Map[String, String],
    persist: Boolean,
    batchSize: Int,
    numPartitions: Option[Int],
    partitionBy: List[String],
    projectId: String,
    region: String,
    dlpTemplateName: String
  ) extends TransformPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    DLPTransformStage.execute(this)
  }

}

object DLPTransformStage {

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type TransformedRow = Row

  def execute(stage: DLPTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._
    import stage._

    val df = spark.table(stage.inputView)
    val schema = df.schema
    val fieldsWithIndex = schema.fields.zipWithIndex

    /** Create a dynamic RowEncoder from the provided schema. We use the phantom
      * TypeRow type to enable implicit resolution to find our encoder.
      */
    implicit val typedEncoder: Encoder[TransformedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(schema)


    var transformedDF = try {

      val dlp = DlpServiceClient.create()

      df.mapPartitions[TransformedRow] { partition: Iterator[Row] =>

        // we are using a BufferedIterator so we can 'peek' at the first row to get column types without advancing the iterator
        // meaning we don't have to keep finding fieldIndex and dataType for each row (inefficient as they will not change)
        val bufferedPartition = partition.buffered
        /*val fieldIndex = bufferedPartition.hasNext match {
          case true => bufferedPartition.head.fieldIndex(stageInputField)
          case false => 0
        }
        val dataType = bufferedPartition.hasNext match {
          case true => bufferedPartition.head.schema(fieldIndex).dataType
          case false => NullType
        }*/

        // group so we can send multiple rows per request
        val groupedPartition = bufferedPartition.grouped(batchSize)

        val tableBuilder = schemaToDLPTableBuilder(schema)

        groupedPartition.foreach { groupedRow =>
          groupedRow.foreach { row =>
            tableBuilder.addRows(sparkRowToDLPRow(fieldsWithIndex, row))
          }
        }

        val contentItem = ContentItem.newBuilder().setTable(tableBuilder.build).build

        val request = DeidentifyContentRequest.newBuilder()
                                            .setParent(LocationName.of(projectId, region).toString())
                                            .setItem(contentItem)
                                            .setDeidentifyTemplateName(dlpTemplateName)
                                            .build()

        // Send the request and receive response from the service.
        val response = dlp.deidentifyContent(request)

        val resultsTable = response.getItem().getTable()

        val transformedRows = resultsTable.getRowsList().asScala.map( row => dlpRowToSparkRow(fieldsWithIndex, row) )

        transformedRows.iterator
      }

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // re-attach metadata to result
    df.schema.fields.foreach(field => {
      transformedDF = transformedDF.withColumn(field.name, col(field.name).as(field.name, field.metadata))
    })

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions)
          case None => transformedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => transformedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => transformedDF.repartition(numPartitions, partitionCols:_*)
          case None => transformedDF.repartition(partitionCols:_*)
        }
      }
    }

    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        spark.catalog.cacheTable(stage.outputView, arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

  def schemaToDLPTableBuilder(schema: StructType): Table.Builder = {
      val builder = Table.newBuilder()

      schema.foreach { sf =>
        builder.addHeaders(FieldId.newBuilder().setName(sf.name).build())
      }

      builder
  }

  def sparkRowToDLPRow(fields: Array[(StructField, Int)], row: org.apache.spark.sql.Row): com.google.privacy.dlp.v2.Table.Row = {
    val rowBuilder = com.google.privacy.dlp.v2.Table.Row.newBuilder()

    for ( (f, idx) <- fields ) {
        f.dataType match {
            case StringType =>
                rowBuilder.addValues(Value.newBuilder().setStringValue(row.getString(idx)).build())
            case _ =>
        }
    }

    rowBuilder.build 
  }

  def dlpRowToSparkRow(fields: Array[(StructField, Int)], row: com.google.privacy.dlp.v2.Table.Row): TransformedRow = {
    val buffer = scala.collection.mutable.Buffer[Any]()

    for ( (f, idx) <- fields ) {
        val value = row.getValues(idx)
        f.dataType match {
            case StringType =>
                buffer += value.getStringValue()
            case _ =>
        }
    }

    Row.fromSeq(buffer)
  }


}
