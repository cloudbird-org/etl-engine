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

import org.apache.spark.sql.types.{StructField, StructType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class Source(spark: SparkSession) {

  val sparkConf = spark.sparkContext.getConf
  val log = LoggerFactory.getLogger(classOf[Source])

  def read(srcName: String,outputView:String):Unit = {
    var options = mutable.Map[String, String]()
    val processingType = Option(sparkConf.get(getConfName(srcName, "type")))
    val format = Option(sparkConf.get(getConfName(srcName, "format")))
    val schema =Option(sparkConf.get(getConfName(srcName, "schema")))
    val optionConfName = getConfName(srcName, "option")
    val optionsData = sparkConf.getAll.filter(x => (x._1.contains(optionConfName)))
    optionsData.foreach(option => options += (option._1.substring(0, optionConfName.length + 1) -> option._2))
    val path = Option(sparkConf.get(getConfName(srcName, "path")))
    val srcConf = SourceConf(processingType.getOrElse("batch"), format.getOrElse("parquet"), schema.getOrElse(null), options, path.getOrElse(null))
    read(srcConf,outputView)
  }

  def read(singleValueField:Map[String,String],multiValueField:Map[String,Map[String,String]]):Unit= {
    var options = mutable.Map[String, String]()
    val processingType = singleValueField.get("type")
    val format = singleValueField.get("format")
    val schema = singleValueField.get("schema")
    val outputView = singleValueField.get("outputView").get
    val optionsData = multiValueField.get("options").getOrElse(Map[String,String]())
    optionsData.keys.foreach(option => options += (option -> optionsData.get(option).get))
    val path = singleValueField.get("path")
    val srcConf = SourceConf(processingType.getOrElse("batch"), format.getOrElse("parquet"), schema.getOrElse(null), options, path.getOrElse(null))
    log.info("srcConf:"+srcConf.toString)
    read(srcConf,outputView)
  }

  def read(srcConf: SourceConf,outputView:String):Unit = {
    if (srcConf.processingType.equals("stream")) readStreamData(srcConf,outputView) else readBatchData(srcConf,outputView)
  }

  def readStreamData(srcConf: SourceConf, outputView:String) = {
    log.info("Reading Data using config \n" +
      "Processing Type = Stream,\n" +
      "format = {}, \n" +
      "schema = {}, \n" +
      "options = {}, \n" +
      "path = {} " +
      srcConf.format, srcConf.schema, srcConf.options.toString, srcConf.path)
    var df:DataFrame = null
    if (srcConf.path != null  && srcConf.schema!=null)
      df = spark.readStream.format(srcConf.format).schema(srcConf.schema).options(srcConf.options).load(srcConf.path)
    else if (srcConf.path != null && srcConf.schema==null)
      df = spark.readStream.format(srcConf.format).options(srcConf.options).load(srcConf.path)
    else if (srcConf.path == null && srcConf.schema!=null)
      df = spark.readStream.format(srcConf.format).schema(srcConf.schema).options(srcConf.options).load()
    else
      spark.readStream.format(srcConf.format).options(srcConf.options).load()

    df.createTempView(outputView)
  }

  def readBatchData(srcConf: SourceConf,outputView:String) = {
    log.info("Reading Data using config \n" +
      "Processing Type = Batch,\n" +
      "format = {}, \n" +
      "schema = {}, \n" +
      "options = {}, \n" +
      "path = {} " +
      srcConf.format, srcConf.schema, srcConf.options.toString, srcConf.path)
    log.info("srcConf.format:"+srcConf.format)
    log.info("srcConf.schema:"+srcConf.schema)
    log.info("srcConf.options:"+srcConf.options.toString())
    log.info("srcConf.path:"+srcConf.path)
    var df:DataFrame = null
    if (srcConf.path != null  && srcConf.schema!=null)
      df = spark.read.format(srcConf.format).schema(srcConf.schema).options(srcConf.options).load(srcConf.path)
    else if (srcConf.path != null && srcConf.schema==null)
      df = spark.read.format(srcConf.format).options(srcConf.options).load(srcConf.path)
    else if (srcConf.path == null && srcConf.schema!=null)
      df = spark.read.format(srcConf.format).schema(srcConf.schema).options(srcConf.options).load()
    else
      spark.read.format(srcConf.format).options(srcConf.options).load()

    df.createTempView(outputView)
  }
}
