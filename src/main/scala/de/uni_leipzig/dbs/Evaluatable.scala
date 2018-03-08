package de.uni_leipzig.dbs

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._


trait Evaluatable {
  def predict(dataSet: DataSet[Vector[Double]]): DataSet[(Double, Vector[Double])]

  def evaluate(testData: DataSet[(Double, Vector[Double])]): DataSet[(Double, Double)] = {
    val testVectors = testData.mapWith { case (_, vec) => vec }.distinct(_.hashCode())
    val testLabels = testData.mapWith { case (label, _) => label }
    val predictionData = predict(testVectors)


    predictionData.join(testData).where(_._2.hashCode()).equalTo(_._2.hashCode()).mapWith {
      case ((label, vec), (label1, vec1)) => (label, label1)
    }
  }

  def evaluateBinaryClassification(testData: DataSet[(Double, Vector[Double])]): (Double, Double, Double) = {
    val evalData = evaluate(testData)
    val counts = evalData.map(t => (t._1, t._2, 1)).groupBy(0, 1).reduce((t, t1) => (t._1, t._2, t._3 + t1._3)).collect()
    val truePositives = counts.filter { x => (x._1 == 1 && x._2 == 1) }.headOption.getOrElse((0, 0, 0))._3
    val falsePositives = counts.filter { x => (x._1 == 1 && x._2 == -1) }.headOption.getOrElse((0, 0, 0))._3
    val trueNegatives = counts.filter { x => (x._1 == -1 && x._2 == -1) }.headOption.getOrElse((0, 0, 0))._3
    val falseNegatives = counts.filter { x => (x._1 == -1 && x._2 == 1) }.headOption.getOrElse((0, 0, 0))._3
    val precision = truePositives.toDouble / (truePositives + falsePositives)
    val recall = truePositives.toDouble / (truePositives + falseNegatives)
    val accuracy = (truePositives + trueNegatives).toDouble / (truePositives + trueNegatives + falseNegatives + falsePositives)


    (accuracy, precision, recall)
  }

}
