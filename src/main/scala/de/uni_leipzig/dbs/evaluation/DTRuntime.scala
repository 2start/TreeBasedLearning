package de.uni_leipzig.dbs.evaluation

import de.uni_leipzig.dbs.decisiontree.DecisionTreeModel
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object DTRuntime {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val runs = 1 to 5
    for(i <- runs) {
      val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("hdfs:///user/duldhardt/training12.csv" ,fieldDelimiter = ";", ignoreFirstLine = true)
      //    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("training12" ,fieldDelimiter = ";", ignoreFirstLine = true)
      val data = rawData.map(elem => {
        val label =  if (elem._3) 1.0 else -1.0
        val features = Vector(elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10)
        (label, features)
      })

      val dt = new DecisionTreeModel(minLeafSamples = 2)
      dt.fit(data)
      val predictedSet = dt.predict(data.map(lv => lv._2))
      predictedSet.writeAsText("hdfs:///user/duldhardt/runtimetestDT.txt", WriteMode.OVERWRITE)
      env.execute()
    }

  }
}
