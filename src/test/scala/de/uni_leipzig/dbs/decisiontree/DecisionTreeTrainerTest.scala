package de.uni_leipzig.dbs.decisiontree

import java.net.URLDecoder

import org.apache.flink.api.scala._
import org.scalatest.{FlatSpec, Matchers}


class DecisionTreeTrainerTest extends FlatSpec with Matchers {

  behavior of "a decision tree"

  it should "not throw an error" in {
    val filepathTraining = URLDecoder.decode(getClass.getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_5].csv").toURI.toString, "UTF-8")
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputFull: DataSet[(Int, Int, Boolean, Double, Double, Double)] = env.readCsvFile(filepathTraining, fieldDelimiter = ";", ignoreFirstLine = true)
    val input = inputFull

    val data = input.map(t => (if (t._3) 1.0 else -1.0, Vector(t._4,   t._5,   t._6)))

    val model = new DecisionTreeModel(minLeafSamples = 100) fit(data)

    val filepathTest = URLDecoder.decode(getClass.getResource("/musicbrainz/training_musicbrainz_softTFIDF[1_5].csv").toURI.toString, "UTF-8")
    val testFull: DataSet[(Int, Int, Boolean, Double, Double, Double)] = env.readCsvFile(filepathTest, fieldDelimiter = ";", ignoreFirstLine = true)
    val test = testFull

    val testData = test.map(t => {
      ( {
        if (t._3) 1.0 else -1.0
      }, Vector(t._4, t._5, t._6))
    })

    println("(Accuracy, Precision, Recall): " + model.evaluateBinaryClassification(testData))
    println(model.rootNode)




  }

  it should "build the right tree" in {

    val lfList1 = (1.0, Vector(1.0,1.0)) ::
      (1.0, Vector(1.0,2.0)) ::
      (1.0, Vector(2.0,1.0)) ::
      (1.0, Vector(2.0,4.0)) ::
      (2.0, Vector(6.0,5.0)) ::
      (2.0, Vector(5.0,6.0)) ::
      (2.0, Vector(8.0,6.0)) ::
      (3.0, Vector(11.0,20.0)) ::
      (3.0, Vector(8.0,8.0)) ::
      (3.0, Vector(8.0, 10.0)) :: Nil

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(lfList1)

    val model = new DecisionTreeModel().fit(data)
    println(model.rootNode)

  }
}
