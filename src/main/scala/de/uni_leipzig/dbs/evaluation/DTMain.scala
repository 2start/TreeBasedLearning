package de.uni_leipzig.dbs.evaluation

import java.io.{File, FileWriter, InputStream}

import de.uni_leipzig.dbs.decisiontree.DecisionTreeModel
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.scala.utils._
import org.apache.flink.core.fs.FileSystem.WriteMode

object DTMain {
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.createCollectionsEnvironment
        val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("musicbrainzExtCombined.csv" ,fieldDelimiter = ";", ignoreFirstLine = true)
//    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("hdfs:///user/duldhardt/training12.csv" ,fieldDelimiter = ";", ignoreFirstLine = true)
    val data = rawData.map(elem => {
      val label =  if (elem._3) 1.0 else -1.0
      val features = Vector(elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10)
      (label, features)
    })


    val matchData = data.filterWith{case(label, features) => label == 1.0}
    val mismatchData = data.filterWith{case(label, features) => label == -1.0}

    val size = 400
    val positiveFractions = List(0.1, 0.2, 0.3, 0.4, 0.5, 0.6)
    val minLeafSizes = List(1,2,4,6,8,10,15,20)
    val runs = 5
    var result = Vector.empty[DTEvaluation]

    for (i <- 1 to runs) {
      for (minLeafSize <- minLeafSizes) {
        for (positiveFraction <- positiveFractions) {
          val matchSize = (size * positiveFraction) toInt
          val mismatchSize = (size * (1.0-positiveFraction)) toInt
          val matchSample = matchData.sampleWithSize(false, matchSize)
          val mismatchSample = mismatchData.sampleWithSize(false, mismatchSize)
          val sample = matchSample.union(mismatchSample)
          val dt = new DecisionTreeModel(minLeafSamples = minLeafSize)
          dt.fit(sample)
          val (accuracy, precision, recall) = dt.evaluateBinaryClassification(data)
          result = result.+:(DTEvaluation(size, positiveFraction, accuracy, precision, recall, minLeafSize, 0.0,0.0,0.0))
        }
      }
    }

    val avgResult = result
      .groupBy(eval => (eval.size, eval.positiveFrac, eval.minLeafSamples))
      .map{case (config, evals) => {
        val (size, positiveFrac, minLeafSamples) = config
        val accuracys = evals map(eval => eval.accuracy)
        val avgAccuracy = accuracys.sum / evals.size
        val precisions = evals map(eval => eval.precision)
        val avgPrecision = precisions.sum / evals.size
        val recalls = evals map(eval => eval.recall)
        val avgRecall = recalls.sum / evals.size
        val varianceAccuracy = (accuracys map (acc => (acc - avgAccuracy)*(acc - avgAccuracy)) sum) / accuracys.size
        val variancePrecision = (precisions map (prec => (prec - avgPrecision)*(prec - avgPrecision)) sum) / precisions.size
        val varianceRecall = (recalls map (rec => (rec - avgRecall)*(rec - avgRecall)) sum) / recalls.size

        DTEvaluation(size, positiveFrac, avgAccuracy, avgPrecision, avgRecall, minLeafSamples, varianceAccuracy, variancePrecision, varianceRecall)
      }}

        avgResult.foreach(_.writeToFile("DTCombEvaluation.txt"))

//    val avgResultData = env.fromCollection(avgResult)
//    avgResultData.writeAsText("hdfs:///user/duldhardt/dteval.txt", WriteMode.OVERWRITE).setParallelism(1)
//    env.execute()


  }
}
