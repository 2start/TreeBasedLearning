package de.uni_leipzig.dbs.api.java

import de.uni_leipzig.dbs.Util
import de.uni_leipzig.dbs.randomforest.{LabeledFeatures, RandomForestModel}
import org.apache.flink.api.java.tuple.{Tuple2 => JavaTuple2, Tuple3 => JavaTuple3}
import org.apache.flink.api.java.{DataSet => JavaDataSet}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._

import scala.collection.JavaConverters._

class RandomForest(
                    val sampleFraction: Double = 0.05,
                    val numTrees: Int = 100,
                    val minImpurityDecrease: Double = 0.00,
                    val minLeafSamples: Int = 1,
                    val maxDepth: Int = Int.MaxValue,
                    val featuresPerSplit: Int = 1
                  ) {
  var model: RandomForestModel = _

  def fit(javaData: JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]]): RandomForest = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(t => {
      val label = t.f0.toDouble
      val features = t.f1.asScala.toVector.map(x => x.doubleValue())
      LabeledFeatures(label, features)
    })
    this.model = new RandomForestModel(sampleFraction, numTrees, minImpurityDecrease, minLeafSamples, maxDepth, featuresPerSplit)
    model fit data
    return this
  }

  def predict(javaData: JavaDataSet[java.util.Vector[java.lang.Double]]): JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]] = {
    if (model == null) {
      throw new IllegalAccessError("Model has to be trained before trying to predict.")
    }
    val data = Util.javaDataSetToScalaDataSet(javaData).map(javaVec => javaVec.asScala.toVector.map(x => x.doubleValue()))
    val predictionData = model.predict(data)
    val tempData = predictionData.mapWith { case (label, features) => {
      val javaDouble = Predef.double2Double(label)
      val javaVec = new java.util.Vector[java.lang.Double](features.map(x => Predef.double2Double(x)).asJava)
      new JavaTuple2(javaDouble, javaVec)
    }
    }
    Util.scalaDataSetToJavaDataSet(tempData)
  }

  def evaluate(javaData: JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]]): JavaDataSet[JavaTuple2[java.lang.Double, java.lang.Double]] = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(t => {
      val label = t.f0.toDouble
      val features = t.f1.asScala.toVector.map(x => x.doubleValue())
      (label, features)
    })
    val evalData = model.evaluate(data)
    Util.scalaDataSetToJavaDataSet(evalData.map(t => {
      new JavaTuple2(Predef.double2Double(t._1), Predef.double2Double(t._2))
    }))
  }

  def evaluateBinaryClassification(javaData: JavaDataSet[JavaTuple2[java.lang.Double, java.util.Vector[java.lang.Double]]]): JavaTuple3[java.lang.Double, java.lang.Double, java.lang.Double] = {
    val data = Util.javaDataSetToScalaDataSet(javaData).map(t => {
      val label = t.f0.toDouble
      val features = t.f1.asScala.toVector.map(x => x.doubleValue())
      (label, features)
    })
    val (accuracy, precision, recall) = model.evaluateBinaryClassification(data)
    val javaAccuracy = Predef.double2Double(accuracy)
    val javaPrecision = Predef.double2Double(precision)
    val javaRecall = Predef.double2Double(recall)
    new JavaTuple3[java.lang.Double, java.lang.Double, java.lang.Double](javaAccuracy, javaPrecision, javaRecall)
  }
}
