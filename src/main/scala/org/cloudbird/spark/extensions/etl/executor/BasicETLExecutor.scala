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

package org.cloudbird.spark.extensions.etl.executor

import org.apache.spark.sql.SparkSession
import org.cloudbird.spark.extensions.etl.{Sink, Source, Transform, _}
import org.slf4j.LoggerFactory

object BasicETLExecutor {
  val log = LoggerFactory.getLogger(getClass)

  val spark = SparkSession.builder().appName("BasicETLExecutor").enableHiveSupport().getOrCreate()

  def main(args: Array[String]): Unit = {
    val etlInstructions = readExecutionSteps(args, "--instrSetFile")
    if(etlInstructions!=null) {
      val instrSets = etlInstructions.instructionSets
      val instrSeqList = instrSets.keys.toList.sortWith(_.toInt < _.toInt)
      //Create UDFs Required
      // Register UDF using spark.register.udf(<udfName>,<UDF function Created>
      for (instrSeq <- instrSeqList) {
        val instrSet = instrSets.get(instrSeq)
        if (validInstructionSet(instrSet))
          executeInstructionSet(instrSet)
        else
          log.error("Validation of input failed for run id:" + instrSeq + " with execution Type :" + instrSet.get.execType)
      }
    } else {
      log.error("Valid instruction set not supplied")
    }
  }

  def validInstructionSet(instrSet: Option[InstructionSet]): Boolean = {
    val execType = instrSet.get.execType
    execType match {
      case "load" => validateLoadData(instrSet)
      case "executeQuery" => validateExecQryData(instrSet)
      case "save" => validateSaveData(instrSet)
      case "executeFunction" => validateExeFuncData(instrSet)
      case "registerUDF" => validateExeFuncData(instrSet)
    }
  }

  def executeInstructionSet(instrSet: Option[InstructionSet]) = {
    val execType = instrSet.get.execType
    execType match {
      case "load" => {
        val source = new Source(spark)
        source.read(instrSet.get.singleValueField,
          instrSet.get.multiValueField)
      }
      case "executeQuery" => {
        val xform = new Transform(spark)
        xform.executeQuery(instrSet.get.singleValueField,
          instrSet.get.multiValueField)
      }
      case "save" => {
        val sink = new Sink(spark)
        sink.write(instrSet.get.singleValueField,
          instrSet.get.multiValueField)
      }
      case "executeFunction" => {
        val xform = new Transform(spark)
        xform.executeFunction(instrSet.get)
      }
    }
  }
}