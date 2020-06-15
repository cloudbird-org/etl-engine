package org.cloudbird.spark.extensions.etl.test

import org.cloudbird.spark.extensions.etl.InstructionSet

class FuncTest {
    def square(instrSet:InstructionSet): Unit ={
      println("FuncTest Square Called. ")
      println("Class:"+instrSet.singleValueField.get("class"))
      println("Function:"+instrSet.singleValueField.get("function"))
    }

    def cleanCountry = (country: String) => {
      val allUSA = Seq("US", "USa", "USA", "United states", "United states of America")
      if (allUSA.contains(country)) {
        "USA"
      }
      else {
        "unknown"
      }
    }
}
