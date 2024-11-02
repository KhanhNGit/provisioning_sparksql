package com.viettel.vlp.provisioning.spark.toRDBMS

import com.typesafe.scalalogging.Logger
import com.viettel.vlp.provisioning.utils.{Const, FunctionUtils, RDBMSUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

import java.io.{FileNotFoundException, IOException}
import java.sql.SQLException

object HDFSToRDBMS {
  var fileConfig: String = "config.yaml"


  def main(args: Array[String]): Unit = {
    val logger = Logger("Spark Provisioning Logger")
    val spark = SparkSession.builder()
      .appName("Read HDFS File Example")
      .master("local[*]")
      .getOrCreate()
    //get data config
    logger.info("get data config")
    val mapConfig = new FunctionUtils().getDataFromFileConfigYaml(fileConfig)
    val mapData = mapConfig(Const.DATA_CONFIG)
    val mapBackup = mapConfig(Const.BACKUP_CONFIG)
    val mapStorage = mapConfig(Const.STORAGE)
    val mapRdbmsConfig = mapConfig(Const.RDBMS_CONFIG)
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

      dfEncrypted = new FunctionUtils().encryptDataFrame(df, mapData)
      logger.info("=============== ENCRYPT DATA AND SHOW DATA ================")
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
      //preSQL
      logger.info("=============== EXECUTE PRESQL ================")
      new RDBMSUtils().executeQueryDB(mapRdbmsConfig, mapRdbmsConfig(Const.PRE_SQL).head)
      //insert data to postgre
      logger.info("=============== INSERT DATA TO POSTGRE ================")
      new RDBMSUtils().insertDataToRDBMS(dfEncrypted, mapRdbmsConfig)
    }  catch {
      case e: FileNotFoundException =>
        logger.error("File not found: " + e.getMessage)
      case e: IOException =>
        logger.error("IO error occurred: " + e.getMessage)
      case e: SQLException =>
        logger.error("Database error: " + e.getMessage)
      case e: Exception =>
        logger.error("An unexpected error occurred: " + e.getMessage)
    }
    spark.stop()
  }
}
