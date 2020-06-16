package org.cloudbird.spark.extensions.etl

case class InstructionSet(execType: String, singleValueField: Map[String, String], multiValueField: Map[String, Map[String, String]])
case class ETLInstructions(instructionSets: Map[String, InstructionSet])

class CommonEntities {
}
