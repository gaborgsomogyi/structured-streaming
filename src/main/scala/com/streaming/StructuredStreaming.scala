package com.streaming

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.LogManager

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.streaming.OutputMode

case class TestRecord(column1: Int, column2: Int)

object StructuredStreaming {

  @transient lazy val log = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val homeDir = System.getProperty("user.home")
    val jobDataDir = homeDir + "/jobdata"
    val inDir = jobDataDir + "/in"
    val outDir = jobDataDir + "/out"
    val checkpointDir = jobDataDir + "/checkpoint"

    FileUtils.deleteDirectory(new File(outDir))
    FileUtils.deleteDirectory(new File(checkpointDir))

    log.info("Creating spark session...")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .getOrCreate()
    log.info("OK")

    spark.sparkContext.addSparkListener(new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        log.info("Status: " + taskEnd.taskInfo.status)
        log.info("RecordsRead: " + taskEnd.taskMetrics.inputMetrics.recordsRead)
        log.info("RecordsWritten: " + taskEnd.taskMetrics.outputMetrics.recordsWritten)
        log.info("Accumulables: " + taskEnd.taskInfo.accumulables)
      }
    })

    val writeStream = spark
      .readStream
      .schema(Encoders.product[TestRecord].schema)
      .csv(inDir)
      .writeStream
      .outputMode(OutputMode.Append)
      .format("parquet")
      .option("path", outDir)
      .option("checkpointLocation", checkpointDir)
      .start()

    log.info("Waiting for termination...")
    writeStream.processAllAvailable()
    writeStream.stop()
    log.info("OK")
  }
}
