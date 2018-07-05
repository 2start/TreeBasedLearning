package de.uni_leipzig.dbs.evaluation

import de.uni_leipzig.dbs.randomforest.{LabeledFeatures, RandomForestModel}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object RFEvalCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile(args(0) ,fieldDelimiter = ";", ignoreFirstLine = true)
    //    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("training12" ,fieldDelimiter = ";", ignoreFirstLine = true)
    val data = rawData.map(elem => {
      val label =  if (elem._3) 1.0 else -1.0
      val features = Vector(elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10)
      (label, features)
    })

    val dataLV = data.map(elem => LabeledFeatures(elem._1, elem._2))

    println("match: " + dataLV.filter(lf => lf.label == 1.0).count() + " mismatch " +
    dataLV.filter(lf => lf.label == -1.0).count())

  }


}
