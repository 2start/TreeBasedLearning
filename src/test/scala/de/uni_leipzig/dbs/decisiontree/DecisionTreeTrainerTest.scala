package de.uni_leipzig.dbs.decisiontree

import java.net.URLDecoder

import org.apache.flink.api.scala._
import org.scalatest.{FlatSpec, Matchers}


class DecisionTreeTest extends FlatSpec with Matchers {

  behavior of "a decision tree"

  it should "not throw an error" in {
    val filepathTraining = URLDecoder.decode(getClass().getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_4].csv").toURI().toString, "UTF-8")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputFull: DataSet[(Int, Int, Boolean, Double, Double, Double)] = env.readCsvFile(filepathTraining, fieldDelimiter = ";", ignoreFirstLine = true)
    val input = inputFull

    val data = input.map(t => {
      ( {
        if (t._3) 1.0 else -1.0
      }, Vector(t._4, t._5, t._6))
    })

    val model = new DecisionTreeModel fit(data)



    val filepathTest = URLDecoder.decode(getClass().getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_4].csv").toURI().toString, "UTF-8")
    val testFull: DataSet[(Int, Int, Boolean, Double, Double, Double)] = env.readCsvFile(filepathTest, fieldDelimiter = ";", ignoreFirstLine = true)
    val test = testFull

    val testData = test.map(t => {
      ( {
        if (t._3) 1.0 else -1.0
      }, Vector(t._4, t._5, t._6))
    })

    println("(Accuracy, Precision, Recall): " + model.evaluateBinaryClassification(testData))





  }
}
