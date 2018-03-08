package de.uni_leipzig.dbs.randomforest

import java.net.URLDecoder

import de.uni_leipzig.dbs.tree.Node
import org.scalatest.{FlatSpec, Matchers}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.scala.utils._

class RandomForestTrainerTest extends FlatSpec with Matchers {
  val filepathTraining = URLDecoder.decode(getClass.getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_5].csv").toURI.toString, "UTF-8")
  val env = ExecutionEnvironment.getExecutionEnvironment
  val inputFull: DataSet[(Int, Int, Boolean, Double, Double, Double)] = env.readCsvFile(filepathTraining, fieldDelimiter = ";", ignoreFirstLine = true)
  val input = inputFull

  val data = input.map(t => (if (t._3) 1.0 else -1.0, Vector(t._4,   t._5,   t._6)))
  val lfData = data.map(t => LabeledFeatures(t._1, t._2))
  val testFeatures = lfData.map(lf => lf.features)
  "A tree ensemble" should "do something" in {
    val te = new RandomForestTrainer(sampleFraction = 0.02, numTrees = 1, featuresPerSplit = 2)
    te.fit(lfData)
    val prediction = te.predictLabeledFeatures(testFeatures)
//    prediction.first(100).print()
    println(te.evaluateBinaryClassification(data))

  }
}
