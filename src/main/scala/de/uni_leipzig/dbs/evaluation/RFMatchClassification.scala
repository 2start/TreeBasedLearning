package de.uni_leipzig.dbs.evaluation

import de.uni_leipzig.dbs.randomforest.{LabeledFeatures, RandomForestModel}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.core.fs.FileSystem.WriteMode

object RFMatchClassification {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile(args(0) ,fieldDelimiter = ";", ignoreFirstLine = true)
    //    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("training12" ,fieldDelimiter = ";", ignoreFirstLine = true)
    val rnd = new scala.util.Random()
    val data = rawData.map(elem => {
      val label =  if (elem._3) 1.0 else -1.0
      val features = Vector(elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10, rnd.nextDouble())
      (label, features)
    })

    val positiveFraction = 0.2
    val negativeFraction = (1-positiveFraction)
    val sampleFraction = 1.0
    val featuresPerSplit = 2
    val size = 400

    val dataLV = data.map(elem => LabeledFeatures(elem._1, elem._2))
    val mismatchData = dataLV.filter(lv => lv.label == -1.0)
    val matchData = dataLV.filter(lv => lv.label == 1.0)

    val matchSize = (size * positiveFraction) toInt
    val mismatchSize = (size * (negativeFraction)) toInt
    val matchSample = matchData.sampleWithSize(false, matchSize)
    val mismatchSample = mismatchData.sampleWithSize(false, mismatchSize)

    val sample = matchSample.union(mismatchSample)
    val rf = new RandomForestModel(1.0, 128, 0.0, 1, Int.MaxValue, 2)
    rf.fit(sample)
    println(rf.evaluateBinaryClassification(data) + rf.getVariableImportances().toString())

  }


}
