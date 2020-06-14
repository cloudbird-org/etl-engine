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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

class Transform(spark: SparkSession) {

  val sparkConf = spark.sparkContext.getConf
  val log = LoggerFactory.getLogger(classOf[Transform])

  def executeQuery(xformName: String, sql: String, outputView: String, cacheView: Boolean):Unit = {
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

    val xformConf = XFormConf(compression, batchSize, maxPartitionBytes, openCostInBytes, broadcastTimeout, autoBroadcastJoinThreshold, shufflePartitions)
    executeQuery(xformConf, sql, outputView, cacheView)
  }

  def executeQuery(singleValueField:Map[String,String],multiValueField:Map[String,Map[String,String]]):Unit = {
    val outputView = singleValueField.get("outputView").get
    val sql = singleValueField.get("sql").get
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
    executeQuery(xformConf, sql, outputView, cacheView)
  }

  def executeQuery(xformConf: XFormConf, sql: String, outputView: String, cacheView: Boolean):Unit = {
    setSparkProperties(xformConf)
    executeQuery(sql, outputView, cacheView)
  }

  def setSparkProperties(xformConf: XFormConf) {
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", xformConf.compression)
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", xformConf.batchSize)
    spark.conf.set("spark.sql.files.maxPartitionBytes", xformConf.maxPartitionBytes)
    spark.conf.set("spark.sql.files.openCostInBytes", xformConf.openCostInBytes)
    spark.conf.set("spark.sql.broadcastTimeout", xformConf.broadcastTimeout)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", xformConf.autoBroadcastJoinThreshold)
    spark.conf.set("spark.sql.shuffle.partitions", xformConf.shufflePartitions)
  }

  def executeQuery(sql: String, outputView: String, cacheView: Boolean) {
    val df = spark.sql(sql)
    df.createTempView(outputView)
    if (cacheView) spark.catalog.cacheTable(outputView, StorageLevel.MEMORY_AND_DISK)
  }

  def register(udfName: String, udf: UserDefinedFunction): UserDefinedFunction = {
    spark.udf.register(udfName, udf)
  }

  def executeFunction(xformFunc: (Map[String, String]) => Unit, funcInput: Map[String, String]): Unit = {
    xformFunc(funcInput)
  }

}
