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

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class Sink(spark: SparkSession) {

  val sparkConf = spark.sparkContext.getConf
  val log = LoggerFactory.getLogger(classOf[Sink])

  def write(inputView:String, sinkName: String):Unit = {
    var options = mutable.Map[String, String]()
    val processingType = Option(sparkConf.get(getConfName(sinkName, "type")))
    val format = Option(sparkConf.get(getConfName(sinkName, "format")))
    val streamTrigger = Option(sparkConf.get(getConfName(sinkName, "trigger")))
    val streamOutputMode = Option(sparkConf.get(getConfName(sinkName, "outputmode")))
    val batchSaveMode = Option(sparkConf.get(getConfName(sinkName, "mode")))
    val optionConfName = getConfName(sinkName, "option")
    val optionsData = sparkConf.getAll.filter(x => (x._1.contains(optionConfName)))
    optionsData.foreach(option => options += (option._1.substring(0, optionConfName.length + 1) -> option._2))
    val path = Option(sparkConf.get(getConfName(sinkName, "path")))
    val sinkConf = SinkConf(processingType.getOrElse("batch"), format.getOrElse("parquet"), Trigger.ProcessingTime(streamTrigger.getOrElse("0l").toLong), streamOutputMode.getOrElse("Append"), batchSaveMode.getOrElse("ErrorIfExists"), options, path.getOrElse(null))
    write(inputView, sinkConf)
  }

  def write(singleValueField:Map[String,String],multiValueField:Map[String,Map[String,String]]):Unit = {
    var options = mutable.Map[String, String]()
    val processingType = singleValueField.get("type")
    val format = singleValueField.get("format")
    val inputView = singleValueField.get("inputView").get
    val streamTrigger = singleValueField.get("trigger")
    val streamOutputMode = singleValueField.get("outputmode")
    val batchSaveMode = singleValueField.get("mode")
    val optionsData = multiValueField.get("options").getOrElse(Map[String,String]())
    optionsData.keys.foreach(option => options += (option -> optionsData.get(option).get))
    val path = singleValueField.get("path")
    val sinkConf = SinkConf(processingType.getOrElse("batch"), format.getOrElse("parquet"), Trigger.ProcessingTime(streamTrigger.getOrElse("0").toLong),  streamOutputMode.getOrElse("Append"), batchSaveMode.getOrElse("ErrorIfExists"), options, path.getOrElse(null))
    write(inputView, sinkConf)
  }

  def write(inputView: String, sinkConf: SinkConf):Unit = {
    if (sinkConf.processingType.equals("stream")) writeStreamData(inputView,sinkConf) else writeBatchData(inputView,sinkConf)
  }

  def writeStreamData(inputView: String, sinkConf: SinkConf) = {
    log.info("Reading Data using config \n" +
      "Processing Type = Stream,\n" +
      "format = {}, \n" +
      "trigger = {}, \n" +
      "outputMode = {}, \n" +
      "options = {}, \n" +
      "path = {} " +
      sinkConf.format, sinkConf.streamTrigger, sinkConf.streamOutputMode, sinkConf.options.toString, sinkConf.path)
    val df = spark.sql("select * from "+ inputView)
    if (sinkConf.path != null)
      df.writeStream.format(sinkConf.format).trigger(sinkConf.streamTrigger).outputMode(sinkConf.streamOutputMode).options(sinkConf.options).start(sinkConf.path)
    else
      df.writeStream.format(sinkConf.format).trigger(sinkConf.streamTrigger).outputMode(sinkConf.streamOutputMode).options(sinkConf.options).start()
  }

  def writeBatchData(inputView: String, sinkConf: SinkConf): Unit = {
    log.info("Reading Data using config \n" +
      "Processing Type = Batch,\n" +
      "format = {}, \n" +
      "mode = {}, \n" +
      "options = {}, \n" +
      "path = {} " +
      sinkConf.format, sinkConf.batchSaveMode, sinkConf.options.toString, sinkConf.path)

    val df = spark.sql("select * from "+ inputView)

    if (sinkConf.path != null)
      df.write.format(sinkConf.format).mode(sinkConf.batchSaveMode).options(sinkConf.options).save(sinkConf.path)
    else
      df.write.format(sinkConf.format).mode(sinkConf.batchSaveMode).options(sinkConf.options).save()
  }

}
