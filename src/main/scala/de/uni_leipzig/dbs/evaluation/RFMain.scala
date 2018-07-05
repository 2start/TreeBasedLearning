package de.uni_leipzig.dbs.evaluation

import java.io.{File, FileWriter, InputStream}

import de.uni_leipzig.dbs.randomforest.{LabeledFeatures, RandomForestModel}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.scala.utils._
import org.apache.flink.core.fs.FileSystem.WriteMode

object RFMain {
  def joinCSV(filepathes: List[InputStream], output: String): Unit = {
      val outputFile = new File(output)
      outputFile.createNewFile()
      val writer = new FileWriter(outputFile)

      filepathes.map(path => {
        val bufferedSource = io.Source.fromInputStream(path);
        // skips header
        val lines = bufferedSource.getLines().drop(1)
        lines.foreach(line => {
          writer.write(line + "\n")
        })
        bufferedSource.close()
      })

      writer.flush()
      writer.close()
    }


  def main(args: Array[String]) {


//    val filepathes = List(
//      getClass.getResourceAsStream("/musicbrainzExt/training12"),
//      getClass.getResourceAsStream("/musicbrainzExt/training13"),
//      getClass.getResourceAsStream("/musicbrainzExt/training14"),
//      getClass.getResourceAsStream("/musicbrainzExt/training15"),
//      getClass.getResourceAsStream("/musicbrainzExt/training23"),
//      getClass.getResourceAsStream("/musicbrainzExt/training24"),
//      getClass.getResourceAsStream("/musicbrainzExt/training25"),
//      getClass.getResourceAsStream("/musicbrainzExt/training34"),
//      getClass.getResourceAsStream("/musicbrainzExt/training35"),
//      getClass.getResourceAsStream("/musicbrainzExt/training45")
//    )
//    val outputFile = "musicbrainzExtCombined.csv"

//    joinCSV(filepathes, outputFile)

    val env = ExecutionEnvironment.getExecutionEnvironment
//    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile("hdfs:///user/duldhardt/musExt.csv" ,fieldDelimiter = ";")
    val rawData: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile(args(0) ,fieldDelimiter = ";", ignoreFirstLine = true)
    val data = rawData.map(elem => {
      val label =  if (elem._3) 1.0 else -1.0
      val features = Vector(elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10)
      (label, features)
    })


    val matchData = data.filterWith{case(label, features) => label == 1.0}
    val mismatchData = data.filterWith{case(label, features) => label == -1.0}

    val sizes = List(400)
    val positiveFractions = List(0.1, 0.2, 0.3, 0.4, 0.5, 0.6)
    val runs = 10
    val numTrees = 128
    val sampleFraction = 1.0
    val featuresPerSplit = 2
    var result = Vector.empty[RFEvaluation]

    for (i <- 1 to runs) {
      for (size <- sizes) {
        for (positiveFraction <- positiveFractions) {
          val matchSize = (size * positiveFraction) toInt
          val mismatchSize = (size * (1.0-positiveFraction)) toInt
          val matchSample = matchData.sampleWithSize(false, matchSize)
          val mismatchSample = mismatchData.sampleWithSize(false, mismatchSize)
          val sample = matchSample.union(mismatchSample)
          val sampleLF = sample.mapWith { case (label, features) => LabeledFeatures(label, features) }
          val rfm = new RandomForestModel(numTrees = numTrees, sampleFraction = sampleFraction, featuresPerSplit = featuresPerSplit)
          rfm.fit(sampleLF)
          val (accuracy, precision, recall) = rfm.evaluateBinaryClassification(data)
          result = result.+:(RFEvaluation(size, positiveFraction, accuracy, precision, recall, numTrees, featuresPerSplit, sampleFraction,0.0,0.0,0.0))
        }
      }
    }

    val avgResult = result
      .groupBy(eval => (eval.size, eval.positiveFrac, eval.numTrees, eval.featuresPerSplit, eval.sampleFraction))
      .map{case (config, evals) => {
        val (size, positiveFrac, numTrees, featuresPerSplit, sampleFraction) = config
        val accuracys = evals map(eval => eval.accuracy)
        val avgAccuracy = accuracys.sum / evals.size
        val precisions = evals map(eval => eval.precision)
        val avgPrecision = precisions.sum / evals.size
        val recalls = evals map(eval => eval.recall)
        val avgRecall = recalls.sum / evals.size
        val varianceAccuracy = (accuracys map (acc => (acc - avgAccuracy)*(acc - avgAccuracy)) sum) / accuracys.size
        val variancePrecision = (precisions map (prec => (prec - avgPrecision)*(prec - avgPrecision)) sum) / precisions.size
        val varianceRecall = (recalls map (rec => (rec - avgRecall)*(rec - avgRecall)) sum) / recalls.size

        RFEvaluation(size, positiveFrac, avgAccuracy, avgPrecision, avgRecall, numTrees, featuresPerSplit, sampleFraction, varianceAccuracy, variancePrecision, varianceRecall)
      }}

//    avgResult.foreach(_.writeToFile("RFevaluation.txt"))

    val avgResultData = env.fromCollection(avgResult)
    avgResultData.writeAsText("hdfs:///user/duldhardt/rfeval.txt", WriteMode.OVERWRITE).setParallelism(1)
    env.execute()


  }
}
