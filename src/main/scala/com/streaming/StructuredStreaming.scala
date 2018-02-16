package com.streaming

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object StructuredStreaming {

  @transient lazy val log = LogManager.getLogger(getClass)
  @transient lazy val appConf = ConfigFactory.load()

  def main(args: Array[String]): Unit = {
    val homeDir = System.getProperty("user.home")
    val jobDataDir = homeDir + "/jobdata"

    log.info("Creating spark session...")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .getOrCreate()
    log.info("OK")

    val schema = new StructType()
      .add("column1", "integer")
      .add("column2", "integer")

    val writeStream = spark
      .readStream
      .schema(schema)
      .csv(jobDataDir + "/in")
      .writeStream
      .outputMode(OutputMode.Append)
      .format("parquet")
      .option("path", jobDataDir + "/out")
      .option("checkpointLocation", jobDataDir + "/checkpoint")
      .start()

    log.info("Waiting for termination...")
    writeStream.processAllAvailable()
    writeStream.stop()
    log.info("OK")
  }
}
