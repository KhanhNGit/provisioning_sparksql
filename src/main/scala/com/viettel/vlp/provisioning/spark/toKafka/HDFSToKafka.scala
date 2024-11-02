package com.viettel.vlp.provisioning.spark.toKafka

import com.typesafe.scalalogging.Logger
import com.viettel.vlp.provisioning.utils.{Const, FunctionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, lit, struct, to_json}

object HDFSToKafka {
  var fileConfig: String = "config.yaml"

  def main(args: Array[String]): Unit = {
    val logger = Logger("Spark Provisioning Logger")

    logger.info("Spark session HDFS to FTPServer")
    val spark = SparkSession.builder()
      .appName("Read HDFS File Example")
      .master("local[*]")
      .getOrCreate()
    //get data config
    val mapConfig = new FunctionUtils().getDataFromFileConfigYaml(fileConfig)
    val mapData = mapConfig(Const.DATA_CONFIG)
    val mapBackup = mapConfig(Const.BACKUP_CONFIG)
    val mapStorage = mapConfig(Const.STORAGE)
    val mapKafkaConfig = mapConfig(Const.KAFKA_CONFIG)
    var mapColumnsMapping: Map[String, List[String]] = Map.empty[String, List[String]]
    if (mapConfig.contains(Const.MAPPING_COLUMN)) {
      mapColumnsMapping = mapConfig(Const.MAPPING_COLUMN)
    }
    logger.info(s"================ $mapData ================")
    var df: DataFrame = null
    var dfEncrypted: DataFrame = null
    //        val pathHDFS = String.format("hdfs://10.254.207.99:9091/user/thaipd/%s", fileName);
    try {
      //get file from hdfs
      df = new FunctionUtils().readFile(spark, mapStorage(Const.NAME_FILE_CONFIG).head)
      // create column key to encrypt
      df = df.withColumn(Const.KEY_CONFIG, lit(mapData(Const.KEY_CONFIG).head))
      df.show()
      logger.info("=============== ENCRYPT DATA AND SHOW DATA ================")
      dfEncrypted = new FunctionUtils().encryptDataFrame(df, mapData)
      dfEncrypted.show()
      if (mapConfig.contains(Const.MAPPING_COLUMN)) {
        logger.info("=============== DATA MAPPED COLUMNS ================")
        dfEncrypted = new FunctionUtils().renameColumnsDataframe(mapColumnsMapping, dfEncrypted)
        dfEncrypted.show()
      }
      dfEncrypted = dfEncrypted.drop(Const.KEY_CONFIG)
      // export file transferred data
      val statusExportFile = new FunctionUtils().exportTransferredDataFile(dfEncrypted, mapStorage, mapBackup)
      if (statusExportFile) {
        logger.info("=============== EXPORT FILE SUCCESS ================")
      }
      logger.info("=============== UPLOAD FILE TO KAFKA SERVER ================")
      // get config Kafka
      val kafkaBroker = mapKafkaConfig(Const.BROKERS).head
      val topic = mapKafkaConfig(Const.TOPIC).head
      val acks = mapKafkaConfig(Const.ACK).head
      val delimiter = mapKafkaConfig(Const.DELIMITER_KAFKA_CONFIG).head





      // Show the DataFrame to verify the transformation
      println("Convert data to Kafka format")

      // convert DataFrame to JSON string
      val kafkaDF = dfEncrypted.withColumn("value", concat_ws(delimiter, dfEncrypted.columns.map(c => col(c)): _*))
      println("convert data")
      kafkaDF.show()
      // write data into Kafka
      kafkaDF.selectExpr("uuid as key", "value as value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBroker)
        .option("topic", topic)
        .option("acks", acks)
        .save()


      println("============ read data kafka ============")
      var dfTest = spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBroker)
        .option("subscribe", topic)
        .load()
      dfTest = dfTest.withColumn("value", col("value").cast("string"))
        .withColumn("key", col("key").cast("string"))
      dfTest.show(1000)
    } catch {
    case e: java.io.IOException =>
      logger.error("IO error occurred: " + e.getMessage)
    case e: org.apache.spark.SparkException =>
      logger.error("Spark error occurred: " + e.getMessage)
    case e: org.apache.kafka.common.KafkaException =>
      logger.error("Kafka error occurred: " + e.getMessage)
    case e: java.lang.Exception =>
      logger.error("An unexpected error occurred: " + e.getMessage)
  } finally {
    spark.stop()
  }

  }
}
