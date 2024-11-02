package com.viettel.vlp.provisioning.utils

object Const {
  val SECRET_DECODE_AES18_DATA = "343541AB61334765"
  // config.yaml
  //encrypt
  val DATA_CONFIG = "dataConfig"
  val ENCRYPTION_CONFIG= "encryption"
  val KEY_CONFIG = "key"
  val COLUMNS_CONFIG = "columns"
  val MAPPING_COLUMN = "mappingColumn"
  //Storage
  val STORAGE = "storage"
  val NAME_FILE_CONFIG = "nameFile"
  val PATH_FILE_CONFIG = "pathFile"
  //FileConfig
  val PATH_FILE_TMP = "file"
  val FOLDER_TMP = "tmp%s"
  //backup
  val BACKUP_CONFIG = "backup"
  val PATH_FILE_EXPORT = "pathFileExport"
  val EXPORT_FILE_NAME = "exportFileName"
  //RDBMS
  val RDBMS_CONFIG = "RDBMSConfig"
  val JDBC_URL_CONFIG = "jdbcUrl"
  val TABLE_NAME = "table"
  val USERNAME = "username"
  val PASSWORD = "password"
  val DRIVER = "driver"
  val PRE_SQL = "preSql"
  // FTP server
  val FTP_CONFIG = "FTPConfig"
  val TYPE_FTP = "type"
  val NUMBER_FILE= "numberFile"
  val MAX_LINES = "maxLines"
  val IP_FTP = "ipFtp"
  val PORT_FTP = "portFtp"
  val USERNAME_FTP = "username"
  val PASSWORD_FTP = "password"
  val PATH_FILE_UPLOAD = "pathFileUpload"
  val NAME_FILE_UPLOAD= "nameFileUpload"
  val EXTENSION_FTP_CONFIG = "extension"
  val COMPRESSION_FTP_CONFIG = "compression"
  val DELIMITER_FTP_CONFIG = "delimiter"
  val NONE_FTP_COMPRESSION = "none"
  // Kafka
  val KAFKA_CONFIG = "KafkaConfig"
  val TARGET_NAME = "targetName"
  val DELIMITER_KAFKA_CONFIG = "delimiter"
  val TOPIC = "topic"
  val ACK = "ack"
  val BROKERS = "brokers"
  //Target
  val TARGET_CONFIG = "target"
  val TYPE_TARGET = "type"
  val FTP_TARGET = "FTPServer"
  val RDBMS_TARGET = "RDBMS"
  val KAFKA_TARGET = "Kafka"
}
