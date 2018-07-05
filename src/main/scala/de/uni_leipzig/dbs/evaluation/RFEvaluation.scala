package de.uni_leipzig.dbs.evaluation

import java.io.{File, FileWriter}

case class RFEvaluation(
                       val size: Int,
                       val positiveFrac: Double,
                       val accuracy: Double,
                       val precision: Double,
                       val recall: Double,
                       val numTrees: Int,
                       val featuresPerSplit: Int,
                       val sampleFraction: Double,
                       val varAcc: Double,
                       val varPrec: Double,
                       val varRec: Double
                       ) {

  override def toString: String = {s"$size,$numTrees,$sampleFraction,$featuresPerSplit,$positiveFrac," +
    s"$accuracy,$precision,$recall,$varAcc,$varPrec,$varRec"}

  def writeToFile(filepath: String): Unit = {
    val outputFile = new File(filepath)
    outputFile.createNewFile()
    val writer = new FileWriter(outputFile, true)
    writer.write(this.toString + "\n")
    writer.flush()
    writer.close()
  }
}
