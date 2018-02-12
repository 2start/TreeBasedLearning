package de.uni_leipzig.dbs

import java.lang

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class DecisionTreeTrainer {
  def getMedian(featureValueData: DataSet[(Int, Double)]): DataSet[(Int, Double)] = {
    featureValueData
      .groupingBy {case(feature, value) => feature}
      .sortGroupWith(Order.ASCENDING) {case(_, value) => value}
      .reduceGroup(new FeatureMedianFilter)
  }
}

class FeatureMedianFilter extends GroupReduceFunction[(Int, Double), (Int, Double)] {
  override def reduce(it: lang.Iterable[(Int, Double)], out: Collector[(Int, Double)]): Unit = {
    if(!it.iterator().hasNext) {
      return
    }
    val featureValueVec = it.asScala.toVector
    val featureIndex = featureValueVec(0)._1
    val values = featureValueVec.map({case(feature, value) => value})

    if(values.size % 2 == 0) {
     val median = (values(values.size/2) + values(values.size/2+1))/2
      out.collect(featureIndex, median)
    } else {
      out.collect(featureIndex, values(values.size/2))
    }
  }
}

