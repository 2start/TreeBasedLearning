package de.uni_leipzig.dbs.api.java

import de.uni_leipzig.dbs.{DecisionTreeModel, Util}
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.java.tuple.{Tuple3 => JavaTuple3}
import org.apache.flink.api.java.tuple.{Tuple2 => JavaTuple2}

import scala.collection.JavaConverters._

class DecisionTree(
                    val maxDepth: Int = Int.MaxValue,
                    val minLeafSamples: Int = 1,
                    val minSplitGain: Double = 0
                  ) {

  // necessary for java api
  def this() = this(Int.MaxValue, 1, 0.0)

  var model: DecisionTreeModel = _

  def fit(javaData: JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]]): DecisionTree = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(t => {
      val d = t.f0.toDouble
      val vec = t.f1.asScala.toVector.map(x => x.doubleValue())
      (d, vec)
    })
    model = new DecisionTreeModel fit(data)
    return this
  }

  def predict(javaData: JavaDataSet[java.util.Vector[java.lang.Double]]): JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]] = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(javaVec => javaVec.asScala.toVector.map(x => x.doubleValue()))
    val predictionData = model.predict(data)
    val tempData = predictionData.mapWith{case(d, vec) => {
      val javaDouble = Predef.double2Double(d)
      val javaVec = new java.util.Vector[java.lang.Double](vec.map(x => Predef.double2Double(x)).asJava)
      new JavaTuple2(javaDouble, javaVec)
    }}
    Util.scalaDataSetToJavaDataSet(tempData)
  }

  def evaluate(javaData: JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]]): JavaDataSet[JavaTuple2[java.lang.Double, java.lang.Double]] = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(t => {
      val d = t.f0.toDouble
      val vec = t.f1.asScala.toVector.map(x => x.doubleValue())
      (d, vec)
    })
    val evalData = model.evaluate(data)
    Util.scalaDataSetToJavaDataSet(evalData.map(t => {
      new JavaTuple2(Predef.double2Double(t._1), Predef.double2Double(t._2))
    }))
  }

  def evaluateBinaryClassification(javaData: JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]]): JavaTuple3[java.lang.Double, java.lang.Double, java.lang.Double] = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(t => {
      val d = t.f0.toDouble
      val vec = t.f1.asScala.toVector.map(x => x.doubleValue())
      (d, vec)
    })
    val(accuracy, precision, recall) = model.evaluateBinaryClassification(data)
    val javaAccuracy = Predef.double2Double(accuracy)
    val javaPrecision = Predef.double2Double(precision)
    val javaRecall = Predef.double2Double(recall)
    new JavaTuple3[java.lang.Double, java.lang.Double, java.lang.Double](javaAccuracy, javaPrecision, javaRecall)
  }
}




