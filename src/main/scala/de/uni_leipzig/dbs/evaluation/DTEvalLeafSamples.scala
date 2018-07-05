package de.uni_leipzig.dbs.evaluation

import de.uni_leipzig.dbs.decisiontree.DecisionTreeModel
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils._
import org.apache.flink.core.fs.FileSystem.WriteMode

object DTEvalLeafSamples {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile(args(0) ,fieldDelimiter = ";", ignoreFirstLine = true)
    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile(args(0) ,fieldDelimiter = ";", ignoreFirstLine = true)
    val data = rawData.map(elem => {
      val label =  if (elem._3) 1.0 else -1.0
      val features = Vector(elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10)
      (label, features)
    })

    val sample = data.sampleWithSize(false, 400)
    val dt = new DecisionTreeModel(minLeafSamples = args(1).toInt)
    dt.fit(sample)
    println(dt.rootNode.toString)
    println(dt.evaluateBinaryClassification(data))

  }


}