package org.cloudbird.spark.extensions.etl

class FuncTest {
    def square(instrSet:InstructionSet): Unit ={
      println("FuncTest Square Called. ")
      println("Class:"+instrSet.singleValueField.get("class"))
      println("Function:"+instrSet.singleValueField.get("function"))
    }
}
