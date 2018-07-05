package de.uni_leipzig.dbs.randomforest

import java.net.URLDecoder

import org.apache.flink.api.scala._
import org.apache.flink.ml.preprocessing.Splitter
import org.scalatest.{FlatSpec, Matchers}

class RandomForestModelTest extends FlatSpec with Matchers {
  val filepathTraining = URLDecoder.decode(getClass.getResource("/musicbrainzExt/musicbrainzExtCombined.csv").toURI.toString, "UTF-8")
  val env = ExecutionEnvironment.getExecutionEnvironment
  val inputFull: DataSet[(Int, Int, Boolean, Double, Double, Double, Double, Double, Double, Double)] = env.readCsvFile(filepathTraining, fieldDelimiter = ";", ignoreFirstLine = true)
  val input = inputFull
  val data = input.map(t => (if (t._3) 1.0 else -1.0, Vector(t._4, t._5, t._6, t._7,t._8, t._9, t._10)))

  val lfData = data.map(t => LabeledFeatures(t._1, t._2))
  val testFeatures = lfData.map(lf => lf.features)
  "A tree ensemble" should "do something" in {
    val te = new RandomForestModel(sampleFraction = 1.0, numTrees = 128, featuresPerSplit = 2)
    val lfDataTrainTest = Splitter.trainTestSplit(lfData, 0.0016, false)
    val trainData = lfDataTrainTest.training
    val testData = lfDataTrainTest.testing.map(lf => (lf.label, lf.features))
    te.fit(trainData)

    println(te.evaluateBinaryClassification(testData));
    println(te.getVariableImportances());
  }
}
