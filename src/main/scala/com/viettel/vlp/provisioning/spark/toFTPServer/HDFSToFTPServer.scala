package com.viettel.vlp.provisioning.spark.toFTPServer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import com.typesafe.scalalogging.Logger
import com.viettel.vlp.provisioning.utils.{Const, FTPServerUtils, FunctionUtils}

object HDFSToFTPServer {
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
    val mapFTPConfig = mapConfig(Const.FTP_CONFIG)
    var mapColumnsMapping: Map[String, List[String]] = Map.empty[String, List[String]]
    if (mapConfig.contains(Const.MAPPING_COLUMN)) {
      mapColumnsMapping = mapConfig(Const.MAPPING_COLUMN)
    }
    logger.info(s"================ $mapData ================")
    var df: DataFrame = null
    var dfEncrypted: DataFrame = null
    var dfDecrypted: DataFrame = null

    try {
      //get file from hdfs
      df = new FunctionUtils().readFile(spark, mapStorage(Const.NAME_FILE_CONFIG).head)
      // create column key to encrypt
      df = df.withColumn(Const.KEY_CONFIG, lit(mapData(Const.KEY_CONFIG).head))

      logger.info("=============== ENCRYPT DATA AND SHOW DATA ================")
      dfEncrypted = new FunctionUtils().encryptDataFrame(df, mapData)
      dfEncrypted.show()
      if (mapConfig.contains(Const.MAPPING_COLUMN)) {
        logger.info("=============== DATA MAPPED COLUMNS ================")
        dfEncrypted = new FunctionUtils().renameColumnsDataframe(mapColumnsMapping, dfEncrypted)
        dfEncrypted.show()
      }
      // Drop key column
      dfEncrypted = dfEncrypted.drop(Const.KEY_CONFIG)
      // export file transferred data
      val statusExportFile = new FunctionUtils().exportTransferredDataFile(dfEncrypted, mapStorage, mapBackup)
      if (statusExportFile) {
        logger.info("=============== EXPORT FILE SUCCESS ================")
      }
      logger.info("=============== UPLOAD FILE TO FTP SERVER ================")
      new FTPServerUtils().uploadFileToFtpServer(spark, dfEncrypted, mapFTPConfig, mapStorage)
    } catch {
    case e: java.io.IOException =>
      logger.error("IO error occurred: " + e.getMessage)
    case e: org.apache.spark.SparkException =>
      logger.error("Spark error occurred: " + e.getMessage)
    case e: java.lang.Exception =>
      logger.error("An unexpected error occurred: " + e.getMessage)
  } finally {
    if (spark != null) {
      spark.stop()
    }
  }


  }
}
