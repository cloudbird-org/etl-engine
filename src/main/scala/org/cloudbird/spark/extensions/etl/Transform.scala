/*
 * MIT LICENSE
 *
 * Copyright (c) 2020. [cloubird.org]
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.cloudbird.spark.extensions.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedFunction}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.udf
import scala.reflect.runtime.universe.{Quasiquote,runtimeMirror}
import scala.tools.reflect.ToolBox

class Transform(spark: SparkSession) {

  val sparkConf = spark.sparkContext.getConf
  val log = LoggerFactory.getLogger(classOf[Transform])

  def executeQuery(xformName: String, sql: String, outputView: String, cacheView: Boolean, debug:Boolean):Unit = {
    val compression = Option(sparkConf.get(getConfName(xformName, "sql.inMemoryColumnarStorage.compressed")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "compressed"))).getOrElse("true"))
    val batchSize = Option(sparkConf.get(getConfName(xformName, "sql.inMemoryColumnarStorage.batchSize")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "batchSize"))).getOrElse("10000"))
    val maxPartitionBytes = Option(sparkConf.get(getConfName(xformName, "sql.files.maxPartitionBytes")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "maxPartitionBytes"))).getOrElse("134217728"))
    val openCostInBytes = Option(sparkConf.get(getConfName(xformName, "sql.files.openCostInBytes")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "openCostInBytes"))).getOrElse("4194304"))
    val broadcastTimeout = Option(sparkConf.get(getConfName(xformName, "sql.broadcastTimeout")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "broadcastTimeout"))).getOrElse("300"))
    val autoBroadcastJoinThreshold = Option(sparkConf.get(getConfName(xformName, "sql.autoBroadcastJoinThreshold")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "autoBroadcastJoinThreshold"))).getOrElse("10485760"))
    val shufflePartitions = Option(sparkConf.get(getConfName(xformName, "sql.shuffle.partitions")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "shufflePartitions"))).getOrElse("200"))
    val debug = Option(sparkConf.get(getConfName(xformName, "sql.debug")))
      .getOrElse(Option(sparkConf.get(getConfName(xformName, "debug"))).getOrElse("false")).toBoolean

    val xformConf = XFormConf(compression, batchSize, maxPartitionBytes, openCostInBytes, broadcastTimeout, autoBroadcastJoinThreshold, shufflePartitions)
    executeQuery(xformConf, sql, outputView, cacheView, debug)
  }

  def executeQuery(singleValueField:Map[String,String],multiValueField:Map[String,Map[String,String]]):Unit = {
    val outputView = singleValueField.get("outputView").get
    val sql = singleValueField.get("sql").get
    val debug = singleValueField.getOrElse("debug","false").toBoolean
    val cacheView = singleValueField.getOrElse("cacheView","false").toBoolean
    val sparkSettings = multiValueField.get("sparkSettings").getOrElse(Map[String,String]())
    val compression = sparkSettings.getOrElse("sql.inMemoryColumnarStorage.compressed",
      sparkSettings.getOrElse("compressed","true"))
    val batchSize = sparkSettings.getOrElse("sql.inMemoryColumnarStorage.batchSize",
      sparkSettings.getOrElse("batchSize","10000"))
    val maxPartitionBytes = sparkSettings.getOrElse("sql.files.maxPartitionBytes",
      sparkSettings.getOrElse("maxPartitionBytes","134217728"))
    val openCostInBytes = sparkSettings.getOrElse("sql.files.openCostInBytes",
      sparkSettings.getOrElse("openCostInBytes","4194304"))
    val broadcastTimeout = sparkSettings.getOrElse("sql.broadcastTimeout",
      sparkSettings.getOrElse("broadcastTimeout","300"))
    val autoBroadcastJoinThreshold = sparkSettings.getOrElse("sql.autoBroadcastJoinThreshold",
      sparkSettings.getOrElse("autoBroadcastJoinThreshold","10485760"))
    val shufflePartitions = sparkSettings.getOrElse("sql.shuffle.partitions",
      sparkSettings.getOrElse("shufflePartitions","200"))

    val xformConf = XFormConf(compression, batchSize, maxPartitionBytes, openCostInBytes, broadcastTimeout, autoBroadcastJoinThreshold, shufflePartitions)
    executeQuery(xformConf, sql, outputView, cacheView, debug)
  }

  def executeQuery(xformConf: XFormConf, sql: String, outputView: String, cacheView: Boolean, debug: Boolean):Unit = {
    setSparkProperties(xformConf)
    executeQuery(sql, outputView, cacheView, debug)
  }

  def setSparkProperties(xformConf: XFormConf) {
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", xformConf.compression)
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", xformConf.batchSize)
    spark.conf.set("spark.sql.files.maxPartitionBytes", xformConf.maxPartitionBytes)
    spark.conf.set("spark.sql.files.openCostInBytes", xformConf.openCostInBytes)
    spark.conf.set("spark.sql.broadcastTimeout", xformConf.broadcastTimeout)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", xformConf.autoBroadcastJoinThreshold)
    spark.conf.set("spark.sql.shuffle.partitions", xformConf.shufflePartitions)
    log.info("Spark for Settings SQL Query\n" +
      "spark.sql.inMemoryColumnarStorage.compressed = {},\n" +
      "spark.sql.inMemoryColumnarStorage.batchSize = {}, \n" +
      "spark.sql.files.maxPartitionBytes = {}, \n" +
      "spark.sql.files.openCostInBytes = {}, \n" +
      "spark.sql.broadcastTimeout = {} ",
      "spark.sql.autoBroadcastJoinThreshold = {} ",
      "spark.spark.sql.shuffle.partitions = {} ",
      xformConf.compression, xformConf.batchSize, xformConf.maxPartitionBytes.toString,
      xformConf.openCostInBytes,xformConf.broadcastTimeout,xformConf.autoBroadcastJoinThreshold,
      xformConf.shufflePartitions)
  }

  def executeQuery(sql: String, outputView: String, cacheView: Boolean, debug:Boolean) {
    log.info("Spark SQL Executed:{}", sql)
    val df = spark.sql(sql)
    df.createTempView(outputView)
    if(debug) df.show(20,false)
    if (cacheView) spark.catalog.cacheTable(outputView, StorageLevel.MEMORY_AND_DISK)
  }


  def executeFunction(instrSet:InstructionSet): Unit = {
    val xformFuncClassStr = instrSet.singleValueField.get("class").get
    val xformFuncStr = instrSet.singleValueField.get("function").get
    val funcClass = Class.forName(xformFuncClassStr)
    val funcObject = funcClass.newInstance();
    val method = funcClass.getMethod(xformFuncStr, instrSet.getClass)
    method.invoke(funcObject,instrSet)
  }

  def executeFunction(xformFunc: (InstructionSet) => Unit, instrSet:InstructionSet): Unit = {
    xformFunc(instrSet)
  }

  def registerUDF(udfName: String, udf: UserDefinedFunction): UserDefinedFunction = {
    spark.udf.register(udfName, udf)
  }

}
