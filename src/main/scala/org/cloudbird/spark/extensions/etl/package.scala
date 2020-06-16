/*
 * MIT LICENSE
 *
 * Copyright (c) 2020. [cloubird.org]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 *  including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, I
 * NCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

package org.cloudbird.spark.extensions

import java.io.{BufferedReader, FileNotFoundException, FileReader, IOException}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.control.Breaks._

package object etl {

  val log = LoggerFactory.getLogger(getClass)

  val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def readExecutionSteps(args: Array[String], argName: String) = {
    var  instrSets: ETLInstructions=null
    try {
      val filePath = readArgumentValue(args, argName)
      val instructionSets = new BufferedReader(getFileReader(filePath))
      instrSets = objectMapper.readValue(instructionSets, classOf[ETLInstructions])
    }catch {
      case e: FileNotFoundException => log.error("Could not locate provided file.")
      case e: IOException => log.error("IOException trying to read file.")
      case e: Exception => log.error("Unknown exception to process instruction file.")
    }
    instrSets
  }

  def getFileReader(fileName: String): FileReader = {
    new FileReader(fileName)
  }

  def readArgumentValue(args: Array[String], argName: String): String = {
    if (args.contains(argName))
      args(args.indexOf(argName) + 1)
    else
      null
  }

  def getConfName(xformName: String, sData: String): String = {
    return "spark." + xformName + "." + sData
  }

  def valParams(xformName: String, sData: String): String = {
    return "spark." + xformName + "." + sData
  }

  def multiValueFieldCheck(fieldGroup: String, field: String, instrSet: Option[InstructionSet]): Boolean = {
    var fieldValueProvided: Boolean = false;
    if (!field.contains("||"))
      fieldValueProvided = if (instrSet.get.multiValueField.getOrElse(fieldGroup, Map[String, String]()).getOrElse(field, null) != null) true else false
    else {
      val fieldArray = field.split("||")
      breakable {
        fieldArray.foreach(x => {
          fieldValueProvided = if (instrSet.get.multiValueField.getOrElse(fieldGroup, Map[String, String]()).getOrElse(x, null) != null) true else false
          if (fieldValueProvided) break
        })
      }
    }
    if (!fieldValueProvided)
      log.info("Data for field {} is not Provided", field)

    fieldValueProvided
  }

  def groupFieldListValueCheck(groupFieldList: Map[String, Array[String]], instrSet: Option[InstructionSet]): Boolean = {
    var reqFieldDataFound = true
    groupFieldList.keys.foreach(key => {
      groupFieldList.getOrElse(key, Array[String]()).foreach(field => {
        reqFieldDataFound = multiValueFieldCheck(key, field, instrSet)
        if (!reqFieldDataFound) return false
      })
    })
    reqFieldDataFound
  }

  def validateLoadData(instrSet: Option[InstructionSet]): Boolean = {
    val format = instrSet.get.singleValueField.getOrElse("format", "parquet")
    format match {
      case "parquet" | "avro" | "text" | "csv" | "json" | "orc" => validateFileLoad(instrSet)
      case "jdbc" => validateJDBCLoad(instrSet)
      case "kafka" => {
        log.debug("Validation Not Yet Supported for Kafka")
        return true
      }
      case _ => {
        log.debug("Supplied Format cannot be validated.Please ensure correctess")
        return true
      }
    }
  }

  def validateFileLoad(instrSet: Option[InstructionSet]): Boolean = {
    var reqFileLoadDataCheckStatus = false
    var fieldList = Array("path", "outputView")
    reqFileLoadDataCheckStatus = fieldListValueCheck(fieldList, instrSet)
    reqFileLoadDataCheckStatus
  }

  def validateJDBCLoad(instrSet: Option[InstructionSet]): Boolean = {
    var reqJDBCLoadDataCheckStatus = false;
    var groupFieldList = Map("options" -> Array("url", "dbtable||query", "driver"))
    var fieldList = Array("outputView")
    reqJDBCLoadDataCheckStatus = groupFieldListValueCheck(groupFieldList, instrSet)
    if (reqJDBCLoadDataCheckStatus)
      reqJDBCLoadDataCheckStatus = fieldListValueCheck(fieldList, instrSet)
    reqJDBCLoadDataCheckStatus
  }

  def validateExecQryData(instrSet: Option[InstructionSet]): Boolean = {
    var reqExecQryDataChkSt = false;
    var fieldList = Array("sql", "outputView")
    reqExecQryDataChkSt = fieldListValueCheck(fieldList, instrSet)
    reqExecQryDataChkSt
  }

  def fieldListValueCheck(fieldList: Array[String], instrSet: Option[InstructionSet]): Boolean = {
    var reqFieldDataFound = true
    fieldList.foreach(field => {
      reqFieldDataFound = singleValueFieldCheck(field, instrSet)
      if (!reqFieldDataFound) return false
    })
    reqFieldDataFound
  }

  def singleValueFieldCheck(field: String, instrSet: Option[InstructionSet]): Boolean = {
    var fieldValueProvided = false
    if (field.contains("||")) {
      val fieldArray = field.split("||")
      breakable {
        fieldArray.foreach(x => {
          fieldValueProvided = if (instrSet.get.singleValueField.getOrElse(x, null) != null) true else false
          if (fieldValueProvided) break
        })
      }
    } else {
      fieldValueProvided = if (instrSet.get.singleValueField.getOrElse(field, null) != null) true else false
    }
    if (!fieldValueProvided)
      log.error("Data for field {}  not Provided", field)
    fieldValueProvided
  }

  def validateSaveData(instrSet: Option[InstructionSet]): Boolean = {
    val format = instrSet.get.singleValueField.getOrElse("format", "parquet")
    format match {
      case "parquet" | "avro" | "text" | "csv" | "json" | "orc" => validateFileSave(instrSet)
      case "jdbc" => validateJDBCSave(instrSet)
      case "kafka" => {
        log.debug("Validation Not Yet Supported for Kafka")
        return true
      }
      case _ => {
        log.debug("format cannot be validated.Please ensure correctess")
        return true
      }
    }
  }

  def validateFileSave(instrSet: Option[InstructionSet]): Boolean = {
    var reqFileLoadDataCheckStatus = false
    var fieldList = Array("path", "inputView")
    reqFileLoadDataCheckStatus = fieldListValueCheck(fieldList, instrSet)
    reqFileLoadDataCheckStatus
  }

  def validateJDBCSave(instrSet: Option[InstructionSet]): Boolean = {
    var reqJDBCLoadDataCheckStatus = false;
    var groupFieldList = Map("options" -> Array("url", "dbtable||query", "driver"))
    var fieldList = Array("inputView")
    reqJDBCLoadDataCheckStatus = groupFieldListValueCheck(groupFieldList, instrSet)
    if (reqJDBCLoadDataCheckStatus)
      reqJDBCLoadDataCheckStatus = fieldListValueCheck(fieldList, instrSet)
    reqJDBCLoadDataCheckStatus
  }

  def validateExeFuncData(instrSet: Option[InstructionSet]): Boolean = {
    var reqExeFuncDataCheckStatus = false;
    var fieldList = Array("class", "function")
    reqExeFuncDataCheckStatus = fieldListValueCheck(fieldList, instrSet)
    reqExeFuncDataCheckStatus
  }

  case class SourceConf(processingType: String, format: String, schema: String, options: mutable.Map[String, String], path: String, debug: Boolean)

  case class SinkConf(processingType: String, format: String, streamTrigger: Trigger, streamOutputMode: String, batchSaveMode: String, options: mutable.Map[String, String], path: String, debug: Boolean)

  case class XFormConf(compression: String, batchSize: String, maxPartitionBytes: String, openCostInBytes: String, broadcastTimeout: String, autoBroadcastJoinThreshold: String, shufflePartitions: String)

}