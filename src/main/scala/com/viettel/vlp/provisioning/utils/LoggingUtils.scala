package com.viettel.vlp.provisioning.utils

//package provisioning.utils
//
//import java.sql.{Connection, DriverManager, Statement}
//
//class LoggingUtils {
//  private var connection: Connection = _
//  private var statement: Statement = _
//
//  def this(dbUrl: String, dbUser: String, dbPassword: String) = {
//    this()
//    try {
//      connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
//      statement = connection.createStatement()
//    } catch {
//      case e: Exception => println(s"Error when init DB Connection: $e")
//    }
//  }
//
//  def this(argsMap: Map[String, String]) = {
//    this()
//    val loggingDBUrl = argsMap.getOrElse("loggingDBURL", "")
//    val loggingDBUser = argsMap.getOrElse("loggingDBUser", "")
//    val loggingDBPassword = argsMap.getOrElse("loggingDBPassword", "")
//    connection = DriverManager.getConnection(loggingDBUrl, loggingDBUser, loggingDBPassword)
//    statement = connection.createStatement()
//  }
//
//  def insertLog(configId: String, startTime: Long, totalFiles: Long, totalDownload: Long, downloadError: String,
//                isBackup: Boolean, totalBackup: Long, backupError: String, logger: Logger): Unit = {
//    val endTime = System.currentTimeMillis()
//    val query =
//      s"INSERT INTO vlp_ftp_pipeline_logging(uuid, config_id, start_time, end_time, total, total_download, download_error, is_backup, total_backup, backup_error) " +
//      s"values (gen_random_uuid(), '$configId', $startTime, $endTime, $totalFiles, $totalDownload, '$downloadError', $isBackup, $totalBackup, '$backupError')"
//    try {
//      statement.execute(query)
//      println("Execute success")
//    } catch {
//      case e: Exception => logger.error(s"Error when insert log record to DB: $e")
//    }
//  }
//
//  def insertLog(pipelineId: String, startTime: Long, status: Int, metrics: Map[String, Any], errorMsg: Any): Unit = {
//    val endTime = System.currentTimeMillis()
//    var errorMsgString:String = ""
//
//    errorMsg match {
//      case errorMsgMap: Map[String, String] =>
//        errorMsgString = StringUtils.mapToJSONString(errorMsgMap)
//      case errorMsg: String =>
//        errorMsgString = errorMsg
//      case _ =>
//        errorMsgString = errorMsg.toString
//    }
//    errorMsgString = errorMsgString.replace("'", "")
//
//    val metricsMsg = StringUtils.mapToJSONString(metrics).replace("'", "")
//
//    val query = s"INSERT INTO vlp_pipeline_logging(uuid, pipeline_id, job_id, start_time, end_time, status, metrics, error_msg) " +
//      s"values (gen_random_uuid(), '$pipelineId', gen_random_uuid(), " +
//      s"$startTime, $endTime, $status, '$metricsMsg', '$errorMsgString')"
//    statement.execute(query)
//  }
//}
