package com.viettel.vlp.provisioning.utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.json.JSONObject

import java.io._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.security.MessageDigest
import java.util.Base64
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.collection.mutable
import scala.reflect.io.Directory

class FunctionUtils {


  // get data config from file yaml
  def getDataFromFileConfigYaml(filePath: String): Map[String, Map[String, List[String]]] = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(filePath)
    val mapConfig: mutable.Map[String, Map[String, List[String]]] = mutable.Map.empty[String, Map[String, List[String]]]
    val mapData: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    val mapColumnMapping: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    val mapFile: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    val mapFTP: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    val mapKafka: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    val mapTargetConfig: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    val mapBackupConfig: mutable.Map[String, List[String]] = mutable.Map.empty[String, List[String]]
    if (inputStream == null) {
      return mapConfig.toMap
    }
    val mapper = new ObjectMapper(new YAMLFactory())
    val rootNode: JsonNode = mapper.readTree(inputStream)
    // get data & encryption
    if (rootNode.has(Const.ENCRYPTION_CONFIG)) {
      val data = rootNode.get(Const.ENCRYPTION_CONFIG)
      // get key secret
      if (data.has(Const.KEY_CONFIG)) {
        mapData.put(Const.KEY_CONFIG, List(data.get(Const.KEY_CONFIG).toPrettyString))
      }
      //get columns encode
      if (data.has(Const.COLUMNS_CONFIG)) {
        val columns = data.get(Const.COLUMNS_CONFIG)
        val JSONObject = new JSONObject(columns.toPrettyString)
        val keys = JSONObject.keys()
        while (keys.hasNext) {
          val key = keys.next()
          mapData.put(key, JSONObject.get(key).toString.split(", ").toList)
        }
      }
      mapConfig.put(Const.DATA_CONFIG, mapData.toMap)
    }
    // get mapping column
    if (rootNode.has(Const.MAPPING_COLUMN)) {
      val mappingColumns = rootNode.get(Const.MAPPING_COLUMN)
      if (!mappingColumns.toPrettyString.equals("null")) {
        val JSONObject = new JSONObject(mappingColumns.toPrettyString)
        val keys = JSONObject.keys()
        while (keys.hasNext) {
          val key = keys.next()
          mapColumnMapping.put(key, JSONObject.get(key).toString.split(", ").toList)
        }
        mapConfig.put(Const.MAPPING_COLUMN, mapColumnMapping.toMap)
      }
    }
    // get info file backup
    if (rootNode.has(Const.BACKUP_CONFIG)) {
      val backupConfigs = rootNode.get(Const.BACKUP_CONFIG)
      if (backupConfigs.has(Const.PATH_FILE_EXPORT)) {
        mapBackupConfig.put(Const.PATH_FILE_EXPORT, List(backupConfigs.get(Const.PATH_FILE_EXPORT).asText()))
      }
      if (backupConfigs.has(Const.EXPORT_FILE_NAME)) {
        mapBackupConfig.put(Const.EXPORT_FILE_NAME, List(backupConfigs.get(Const.EXPORT_FILE_NAME).asText()))
      }
      mapConfig.put(Const.BACKUP_CONFIG, mapBackupConfig.toMap)
    }
    // get info file config
    if (rootNode.has(Const.STORAGE)) {
      val fileConfigs = rootNode.get(Const.STORAGE)
      if (fileConfigs.has(Const.NAME_FILE_CONFIG)) {
        mapFile.put(Const.NAME_FILE_CONFIG, List(fileConfigs.get(Const.NAME_FILE_CONFIG).asText()))
      }
      if (fileConfigs.has(Const.PATH_FILE_CONFIG)) {
        mapFile.put(Const.PATH_FILE_CONFIG, List(fileConfigs.get(Const.PATH_FILE_CONFIG).asText()))
      }
      mapConfig.put(Const.STORAGE, mapFile.toMap)
    }
    // get target config
    if (rootNode.has(Const.TARGET_CONFIG)) {
      val targetConfigs = rootNode.get(Const.TARGET_CONFIG)
      if (targetConfigs.has(Const.TYPE_TARGET)) {
        mapTargetConfig.put(Const.TYPE_TARGET, List(targetConfigs.get(Const.TYPE_TARGET).asText()))
      }
      mapConfig.put(Const.TARGET_CONFIG, mapTargetConfig.toMap)
    }
    //get ìnfo RDBMS
    if (rootNode.has(Const.RDBMS_CONFIG)) {
      val rdbmsConfigs = rootNode.get(Const.RDBMS_CONFIG)
      if (rdbmsConfigs.has(Const.JDBC_URL_CONFIG)) {
        mapFile.put(Const.JDBC_URL_CONFIG, List(rdbmsConfigs.get(Const.JDBC_URL_CONFIG).asText()))
      }
      if (rdbmsConfigs.has(Const.TABLE_NAME)) {
        mapFile.put(Const.TABLE_NAME, List(rdbmsConfigs.get(Const.TABLE_NAME).asText()))
      }
      if (rdbmsConfigs.has(Const.USERNAME)) {
        mapFile.put(Const.USERNAME, List(rdbmsConfigs.get(Const.USERNAME).asText()))
      }
      if (rdbmsConfigs.has(Const.PASSWORD)) {
        mapFile.put(Const.PASSWORD, List(rdbmsConfigs.get(Const.PASSWORD).asText()))
      }
      if (rdbmsConfigs.has(Const.DRIVER)) {
        mapFile.put(Const.DRIVER, List(rdbmsConfigs.get(Const.DRIVER).asText()))
      }
      if (rdbmsConfigs.has(Const.PRE_SQL)) {
        mapFile.put(Const.PRE_SQL, List(rdbmsConfigs.get(Const.PRE_SQL).asText()))
      }
      mapConfig.put(Const.RDBMS_CONFIG, mapFile.toMap)
    }
    //get config FTP
    if (rootNode.has(Const.FTP_CONFIG)) {
      val ftpConfigs = rootNode.get(Const.FTP_CONFIG)
      if (ftpConfigs.has(Const.TYPE_FTP)) {
        mapFTP.put(Const.TYPE_FTP, List(ftpConfigs.get(Const.TYPE_FTP).asText()))
      }
      if (ftpConfigs.has(Const.NUMBER_FILE)) {
        mapFTP.put(Const.NUMBER_FILE, List(ftpConfigs.get(Const.NUMBER_FILE).asText()))
      }
      if (ftpConfigs.has(Const.MAX_LINES)) {
        mapFTP.put(Const.MAX_LINES, List(ftpConfigs.get(Const.MAX_LINES).asText()))
      }
      if (ftpConfigs.has(Const.IP_FTP)) {
        mapFTP.put(Const.IP_FTP, List(ftpConfigs.get(Const.IP_FTP).asText()))
      }
      if (ftpConfigs.has(Const.PORT_FTP)) {
        mapFTP.put(Const.PORT_FTP, List(ftpConfigs.get(Const.PORT_FTP).asText()))
      }
      if (ftpConfigs.has(Const.USERNAME_FTP)) {
        mapFTP.put(Const.USERNAME_FTP, List(ftpConfigs.get(Const.USERNAME_FTP).asText()))
      }
      if (ftpConfigs.has(Const.PASSWORD_FTP)) {
        mapFTP.put(Const.PASSWORD_FTP, List(ftpConfigs.get(Const.PASSWORD_FTP).asText()))
      }
      if (ftpConfigs.has(Const.PATH_FILE_UPLOAD)) {
        mapFTP.put(Const.PATH_FILE_UPLOAD, List(ftpConfigs.get(Const.PATH_FILE_UPLOAD).asText()))
      }
      if (ftpConfigs.has(Const.NAME_FILE_UPLOAD)) {
        mapFTP.put(Const.NAME_FILE_UPLOAD, List(ftpConfigs.get(Const.NAME_FILE_UPLOAD).asText()))
      }
      if (ftpConfigs.has(Const.EXTENSION_FTP_CONFIG)) {
        mapFTP.put(Const.EXTENSION_FTP_CONFIG, List(ftpConfigs.get(Const.EXTENSION_FTP_CONFIG).asText()))
      }
      if (ftpConfigs.has(Const.COMPRESSION_FTP_CONFIG)) {
        mapFTP.put(Const.COMPRESSION_FTP_CONFIG, List(ftpConfigs.get(Const.COMPRESSION_FTP_CONFIG).asText()))
      }
      if (ftpConfigs.has(Const.DELIMITER_FTP_CONFIG)) {
        mapFTP.put(Const.DELIMITER_FTP_CONFIG, List(ftpConfigs.get(Const.DELIMITER_FTP_CONFIG).asText()))
      }

      mapConfig.put(Const.FTP_CONFIG, mapFTP.toMap)
    }
    // get kafka config
    if (rootNode.has(Const.KAFKA_CONFIG)) {
      val kafkaConfigs = rootNode.get(Const.KAFKA_CONFIG)
      if(kafkaConfigs.has(Const.BROKERS)) {
        mapKafka.put(Const.BROKERS, List(kafkaConfigs.get(Const.BROKERS).asText()))
      }
      if(kafkaConfigs.has(Const.TARGET_NAME)) {
        mapKafka.put(Const.TARGET_NAME, List(kafkaConfigs.get(Const.TARGET_NAME).asText()))
      }
      if(kafkaConfigs.has(Const.TOPIC)) {
        mapKafka.put(Const.TOPIC, List(kafkaConfigs.get(Const.TOPIC).asText()))
      }
      if(kafkaConfigs.has(Const.ACK)) {
        mapKafka.put(Const.ACK, List(kafkaConfigs.get(Const.ACK).asText()))
      }
      if (kafkaConfigs.has(Const.DELIMITER_KAFKA_CONFIG)) {
        mapKafka.put(Const.DELIMITER_KAFKA_CONFIG, List(kafkaConfigs.get(Const.DELIMITER_KAFKA_CONFIG).asText()))
      }
      mapConfig.put(Const.KAFKA_CONFIG, mapKafka.toMap)
    }

    mapConfig.toMap
  }


  // ENCODE FUNCTION
  def encryptCustom(rawData: String, key: String, additionalKeys: String): String = {
    // Tạo khóa AES từ khóa không giới hạn độ dài
    val combinedKey = key + additionalKeys.split(",").mkString("")
    val aesKey = generateAESKeyFromFlexibleKey(combinedKey, 16) // Tạo khóa 16 byte từ combinedKey

    val secretKey = new SecretKeySpec(aesKey, "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    val iv = new IvParameterSpec(aesKey) // Sử dụng aesKey làm IV

    cipher.init(Cipher.ENCRYPT_MODE, secretKey, iv)

    val encryptedData = cipher.doFinal(rawData.getBytes("UTF-8"))
    Base64.getEncoder.encodeToString(iv.getIV ++ encryptedData) // Kết hợp IV và dữ liệu mã hóa
  }

  // DECODE FUNCTION
  def decryptCustom(encryptedData: String, key: String, additionalKeys: String): String = {
    // Tạo khóa AES từ khóa không giới hạn độ dài
    val combinedKey = key + additionalKeys.split(",").mkString("")
    val aesKey = generateAESKeyFromFlexibleKey(combinedKey, 16) // Tạo khóa 16 byte từ combinedKey

    val secretKey = new SecretKeySpec(aesKey, "AES")
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")

    // Giải mã dữ liệu
    val decodedData = Base64.getDecoder.decode(encryptedData)
    val iv = new IvParameterSpec(decodedData.take(16)) // Lấy IV từ đầu dữ liệu mã hóa
    val cipherText = decodedData.drop(16) // Dữ liệu mã hóa

    cipher.init(Cipher.DECRYPT_MODE, secretKey, iv)
    new String(cipher.doFinal(cipherText), "UTF-8")
  }

  // Hàm tạo khóa AES từ một khóa có độ dài bất kỳ
  private def generateAESKeyFromFlexibleKey(key: String, keySize: Int = 16): Array[Byte] = {
    val sha256 = MessageDigest.getInstance("SHA-256")
    val hash = sha256.digest(key.getBytes("UTF-8"))
    hash.take(keySize) // Lấy 16 bytes đầu tiên (128 bit) cho AES-128
  }


  // Hàm giải nén file ZIP
  def unzipFile(zipFilePath: String): String = {
    val zipInputStream = new ZipInputStream(Files.newInputStream(Paths.get(zipFilePath)))
    val outputDir = "path/to/unzip/output" // path unzip
    var entry = zipInputStream.getNextEntry
    while (entry != null) {
      val newFile = new File(outputDir, entry.getName)
      FileUtils.copyInputStreamToFile(zipInputStream, newFile)
      entry = zipInputStream.getNextEntry
    }
    zipInputStream.closeEntry()
    zipInputStream.close()

    // Giả sử file bên trong ZIP là một file duy nhất, bạn có thể trả về đường dẫn của file này
    outputDir + "/" + new File(outputDir).listFiles().head.getName
  }

  // unzip tar
  def untarFile(tarFilePath: String): String = {
    throw new UnsupportedOperationException("not support unzip TAR")
  }

  def readFile(spark: SparkSession, path: String): DataFrame = {
    val fileExtension = path.split("\\.").last.toLowerCase()

    fileExtension match {
      case "csv" =>
        spark.read.option("header", "true").csv(path)

      case "json" =>
        spark.read.json(path)

      case "txt" =>
        spark.read.text(path)

      case "zip" =>
        val unzippedPath = unzipFile(path)
        readFile(spark, unzippedPath)

      case "tar" =>
        val untarredPath = untarFile(path)
        readFile(spark, untarredPath)

      case other =>
        throw new UnsupportedOperationException(s"format file $other does not support")
    }
  }

  def concatColumns(cols: List[Column], separator: String): Column = {
    cols.reduceLeft((col1, col2) => concat(col1, lit(separator), col2))
  }

  def convertMapListStringToMapString(mapListString: Map[String, List[String]]): Map[String, String] = {
    mapListString.map {
      case (key, list) => key -> list.mkString(",")
    }
  }

  // rename column dataframe
  def renameColumnsDataframe(mappingColumns: Map[String, List[String]], df: DataFrame): DataFrame = {
    var dataFrameRenamed = df
    var tmpDataFrame = df
    val currentTimeMillis = "_" + System.currentTimeMillis().toString
    val mapColumnsTmp: mutable.Map[String, String] = mutable.Map.empty[String, String]

    mappingColumns.foreach { case (k, v) =>
      tmpDataFrame = dataFrameRenamed.withColumnRenamed(k, k + currentTimeMillis)
      dataFrameRenamed = tmpDataFrame
      mapColumnsTmp.put(k, k + currentTimeMillis)
    }
    mappingColumns.foreach { case (k, v) =>
      tmpDataFrame = dataFrameRenamed.withColumnRenamed(mapColumnsTmp(k), v.head)
      dataFrameRenamed = tmpDataFrame
    }
    dataFrameRenamed
  }



  // export file to folder tmp
  def exportFileExtensionToFolderTmp(fileExtension: String, dfEncrypted: DataFrame, pathFileTmp: String): Unit = {
    fileExtension match {
      case "csv" =>
        dfEncrypted.write
          .option("header", "true")
          .mode("overwrite")
          .csv(pathFileTmp)
      case "json" =>
        dfEncrypted.write.option("header", "true")
          .mode("overwrite").json(pathFileTmp)

      case "txt" =>
        dfEncrypted.write.option("header", "true")
          .mode("overwrite")
          .text(pathFileTmp)
      case other =>
        dfEncrypted.write
          .option("header", "true")
          .mode("overwrite")
          .csv(pathFileTmp)
    }
  }

  // export transferred file
  def exportTransferredDataFile(dfEncrypted: DataFrame, mapFileConfig: Map[String, List[String]], mapBackupConfig: Map[String, List[String]]): Boolean = {
    val currentTimeMillis: String = System.currentTimeMillis().toString
    val pathFileTmp: String = String.format(Const.PATH_FILE_TMP + "/" + Const.FOLDER_TMP, currentTimeMillis)
    dfEncrypted.write
      .option("header", "true")
      .mode("overwrite")
      .csv(pathFileTmp)
    // search file in tmp folder
    val tempFile = new File(pathFileTmp).listFiles().find(_.getName.endsWith(".csv")).get

    // renamed
    val outputFileName = mapBackupConfig(Const.PATH_FILE_EXPORT).head + String.format(mapBackupConfig(Const.EXPORT_FILE_NAME).head, currentTimeMillis)
    val outputPath = Paths.get(outputFileName)

    // move file export to /file
    Files.move(tempFile.toPath, outputPath, StandardCopyOption.REPLACE_EXISTING)

    // delete tmp folder
    new Directory(new File(pathFileTmp)).deleteRecursively()
  }



  //encrypt dataFrame
  def encryptDataFrame(df: DataFrame, mapData: Map[String, List[String]]): DataFrame = {
    //encrypt columns
    var dfEncrypted: DataFrame = null
    var dfTmp = df
    val key = mapData(Const.KEY_CONFIG).head
    mapData.foreach { case (columnName, valueList) =>
      if (!columnName.contains(Const.KEY_CONFIG)) {

        // Chuyển đổi danh sách các tên cột thành danh sách các đối tượng Column
        val columns = valueList.map(col)

        // Tạo cột nối từ danh sách các cột
        val additionalKeysColumn = new FunctionUtils().concatColumns(columns, ",")
        val encryptColSelect = udf((rawData: String, additionalKeys: String) => new FunctionUtils().encryptCustom(rawData, key, additionalKeys))

        dfEncrypted = dfTmp.withColumn(columnName, encryptColSelect(col(columnName), additionalKeysColumn))
        dfTmp = dfEncrypted
      }
    }
    dfEncrypted
  }

  def zipFolder(sourceFolderPath: String, folderName: String, typeFile: String, nameFileUpload: String, currentTimeMillis: String, compression: String): Unit = {
    // Lấy tất cả các file và thư mục trong thư mục nguồn
    val sourceFolder = new File(sourceFolderPath + "/" + folderName)
    if (!sourceFolder.exists() || !sourceFolder.isDirectory) {
      throw new FileNotFoundException(s"Folder not found: $sourceFolderPath")
    }
    var countFiles: Int = 0
    // rename files
    val renamedFiles: Array[File] = Option(sourceFolder.listFiles().filter(_.getName.endsWith("." + typeFile)))
      .getOrElse(Array.empty[File])
      .map { file =>
        val fileName = String.format("%s_%s_%s.%s", nameFileUpload, countFiles.toString, currentTimeMillis, typeFile)
        val newFile = new File(file.getParentFile, fileName)
        countFiles += 1
        file.renameTo(newFile)  // Optional: Rename the file on disk
        newFile
      }
    val filePaths: Iterable[File] = Option(renamedFiles)
      .toIterable
      .flatten
    zip(filePaths, folderName, compression)
  }

  private def zip(files: Iterable[File], folderName: String, compression: String): Unit = {
    val zip = new ZipOutputStream(new FileOutputStream(String.format("%s.%s", folderName, compression)))
    files.foreach { file =>
      // Calculate the relative path to use inside the ZIP
      val relativePath = file
      val entryName = File.separator + relativePath
      zip.putNextEntry(new ZipEntry(entryName))
      val in = new BufferedInputStream(new FileInputStream(file))
      var b = in.read()
      while (b > -1) {
        zip.write(b)
        b = in.read()
      }
      in.close()
      zip.closeEntry()
    }

    zip.close()
  }


}
