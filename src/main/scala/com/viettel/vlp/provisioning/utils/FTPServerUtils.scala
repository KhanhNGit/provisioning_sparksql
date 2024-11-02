package com.viettel.vlp.provisioning.utils

import org.apache.commons.net.ftp.FTPClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.concat_ws

import java.io.{File, FileInputStream}
import scala.reflect.io.Directory

class FTPServerUtils {
  // get files upload to ftp server and export file to folder
  private def getFileAndExportFolder(spark: SparkSession, mapFTPConfig: Map[String, List[String]], df: DataFrame, pathFileTmp: String, typeFile: String, delimiter: String): Unit = {
    var partitionedDF: DataFrame = null
    var dfSingleColumn = df
    if (typeFile.equals("txt")) {
      val headers: Array[String] = df.columns
      val headerString: String = headers.mkString(delimiter)
      dfSingleColumn = df.select(concat_ws(delimiter, df.columns.map(df(_)): _*).as(headerString))
    }
    if (mapFTPConfig(Const.TYPE_FTP).head.equals(Const.NUMBER_FILE)) {
      partitionedDF = dfSingleColumn.repartition(mapFTPConfig(Const.NUMBER_FILE).head.toInt)
    } else {
      var numberFiles: Long = dfSingleColumn.count() / mapFTPConfig(Const.MAX_LINES).head.toLong
      if (numberFiles * mapFTPConfig(Const.MAX_LINES).head.toLong < dfSingleColumn.count()) {
        numberFiles += 1
      }
      partitionedDF = dfSingleColumn.repartition(numberFiles.toInt)
    }
    //    partitionedDF.show()
    // export file to folder tmp
    new FunctionUtils().exportFileExtensionToFolderTmp(typeFile, partitionedDF, pathFileTmp)

  }

  // update load file to ftp server
  def uploadFileToFtpServer(spark: SparkSession, df: DataFrame, mapFTPConfig: Map[String, List[String]], mapFileConfig: Map[String, List[String]]): Unit = {
    //Path folder tạm
    val currentTimeMillis: String = System.currentTimeMillis().toString
    val folderName: String = String.format(Const.FOLDER_TMP, currentTimeMillis)
    val pathFileTmp = Const.PATH_FILE_TMP + "/" + folderName

    // export file from dataFrame to folder tmp
    val typeFile = mapFTPConfig(Const.EXTENSION_FTP_CONFIG).head
    val delimiter = mapFTPConfig(Const.DELIMITER_FTP_CONFIG).head
    getFileAndExportFolder(spark, mapFTPConfig, df, pathFileTmp, typeFile, delimiter)
    val compression  = mapFTPConfig(Const.COMPRESSION_FTP_CONFIG).head
    if(!compression.equals(Const.NONE_FTP_COMPRESSION)){
      new FunctionUtils().zipFolder(Const.PATH_FILE_TMP, folderName, typeFile, mapFTPConfig(Const.NAME_FILE_UPLOAD).head, currentTimeMillis, compression)
    }

    //connect server ftp
    val ftpClient = new FTPClient()
    try {
      ftpClient.connect(mapFTPConfig(Const.IP_FTP).head, mapFTPConfig(Const.PORT_FTP).head.toInt)

      ftpClient.enterLocalPassiveMode()

      ftpClient.login(mapFTPConfig(Const.USERNAME_FTP).head, mapFTPConfig(Const.PASSWORD_FTP).head)

      var files = new File(pathFileTmp).listFiles().filter(_.getName.endsWith("." + typeFile))
      if(!compression.equals(Const.NONE_FTP_COMPRESSION)){
        files = Array(new File(String.format("%s.%s", folderName, compression)))
      }

      // upload file
      var countFiles: Int = 0
      files.foreach { file =>
        // set name file
        var fileName = String.format("%s_%s_%s.%s", mapFTPConfig(Const.NAME_FILE_UPLOAD).head, countFiles.toString, currentTimeMillis, typeFile)
        if(!compression.equals(Const.NONE_FTP_COMPRESSION)){
          fileName = file.getName
        }
        val remoteFileName = String.format("%s/%s", mapFTPConfig(Const.PATH_FILE_UPLOAD).head, fileName)
        countFiles += 1
        val inputStream = new FileInputStream(file)
        try {
          val result = ftpClient.storeFile(remoteFileName, inputStream)
          if (!result) {
            println(s"Failed to upload file: ${ftpClient.getReplyString}")
          }

        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          inputStream.close()
        }
      }
      // delete folder tmp
      new Directory(new File(pathFileTmp)).deleteRecursively()
      new Directory(new File(String.format("%s.%s", folderName, compression))).deleteRecursively()

    } finally {
      if (ftpClient.isConnected) {
        false
        ftpClient.logout()
        ftpClient.disconnect()
      }
    }
  }
}
