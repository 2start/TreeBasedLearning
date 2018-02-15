package de.uni_leipzig.dbs

import java.lang

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class DecisionTreeTrainer {
  def getMedian(featureValueData: DataSet[(Int, Double)]): DataSet[(Int, Double)] = {
    featureValueData
      .groupingBy {case(feature, value) => feature}
      .sortGroupWith(Order.ASCENDING) {case(_, value) => value}
      .reduceGroup(new FeatureMedianFilter)
  }

  def bestSplit(id: Int, data: DataSet[(Double, Vector[Double])], entropy: Entropy): Node = {

    val featureValueLabelData = data
      .flatMapWith {case(label, vec) => 0 until vec.size map(i => (i, vec(i), label))}
    val featureValueData = featureValueLabelData.mapWith {case(feature, value, _) => (feature, value)}

    val featureMedianData = getMedian(featureValueData)


    def featureValueLabelToFeatureEntropy(featureValueLabelData: DataSet[(Int, Double, Double)]): DataSet[(Int, Entropy)] = {
      featureValueLabelData
        .mapWith {case(feature, value, label) => (feature, label)}
        .groupingBy {case(feature, label) => feature}
        .reduceGroup(new FeatureLabelToEntropy)
    }

    val lowerData = featureValueLabelData
      .filter(new FeatureValueLowerUpperSplit(upperSplit = false)).withBroadcastSet(featureMedianData, "featureMedianData")

    val upperData = featureValueLabelData
      .filter(new FeatureValueLowerUpperSplit(upperSplit = true)).withBroadcastSet(featureMedianData, "featureMedianData")

    val lowerFeatureEntropyData = featureValueLabelToFeatureEntropy(lowerData)
    val upperFeatureEntropyData = featureValueLabelToFeatureEntropy(upperData)

    val featureEntropyData = lowerFeatureEntropyData.join(upperFeatureEntropyData, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo(0)
      .mapWith{case((feature, entropy),(feature1, entropy1)) => (feature,entropy,entropy1)}


    val featureMedianMap = featureMedianData.collect().toMap
    val featureEntropySeq = featureEntropyData.collect()

    def shouldTerminate(id: Int, entropy: Entropy, featureEntropySeq: Seq[(Int, Entropy, Entropy)]): Boolean = {
      val sampleCount = entropy.totalCount
      val depth = (math.log(id)/math.log(2)).toInt


      if(sampleCount < DecisionTreeTrainer.minLeafSamples) {
        return true
      }
      if(depth > DecisionTreeTrainer.maxDepth) {
        return true
      }
      if(featureEntropySeq.isEmpty) {
        return true
      }

      return false
    }

    if(shouldTerminate(id, entropy, featureEntropySeq)) {
      val totalCount = entropy.totalCount
      val labelCountMap = entropy.labelCountMap
      val predictionLabelCount = labelCountMap.maxBy{case(label, count) => count}
      val prediction = predictionLabelCount._1
      val predictionCount = predictionLabelCount._2

      val predictionProbability = predictionCount.toDouble/totalCount.toDouble

      return new Node(id, prediction, predictionProbability, None, None, None, None, true)
    }

    val minFeatureEntropy = featureEntropySeq.minBy{case(feature, entropy, entropy1) => (entropy.entropy + entropy1.entropy)}
    val lowerEntropy = minFeatureEntropy._2
    val upperEntropy = minFeatureEntropy._3
    val splitFeatureIndex = minFeatureEntropy._1
    val splitEntropy = 1/2 * lowerEntropy.entropy + 1/2 * upperEntropy.entropy
    val lowerCount = lowerEntropy.totalCount
    val upperCount = lowerEntropy.totalCount
    val median = featureMedianMap(splitFeatureIndex)
    val prediction = entropy.labelCountMap.maxBy{case(label, count) => count}._1
    val predicitonProbability = entropy.labelCountMap(prediction).toDouble/entropy.totalCount.toDouble

    val informationGain = entropy.entropy - lowerEntropy.totalCount/entropy.totalCount*lowerEntropy.entropy - upperEntropy.totalCount/entropy.totalCount*upperEntropy.entropy
    if(informationGain<=DecisionTreeTrainer.gainThreshold) {
      val totalCount = entropy.totalCount
      val labelCountMap = entropy.labelCountMap
      val predictionLabelCount = labelCountMap.maxBy{case(label, count) => count}
      val prediction = predictionLabelCount._1
      val predictionCount = predictionLabelCount._2

      val predictionProbability = predictionCount.toDouble/totalCount.toDouble

      return new Node(id, prediction, predictionProbability, None, None, None, None, true)
    }

    val leftChildData = data.filterWith{case(label, vec) => vec(splitFeatureIndex) <= median}
    val rightChildData = data.filterWith{case(label, vec) => vec(splitFeatureIndex) > median}
    val leftChild = bestSplit(id*2, leftChildData, lowerEntropy)
    val rightChild = bestSplit(id*2+1, rightChildData, upperEntropy)

    return new Node(id, prediction, predicitonProbability, Some(splitFeatureIndex), Some(median), Some(leftChild), Some(rightChild), false)
  }

  def createTree(data: DataSet[(Double, Vector[Double])]): Node = {
    val labelCountData = data
      .mapWith{case(label, vec) => (label,1)}
      .groupingBy{case(label, count) => label}
      .reduceWith{case((label, count), (label1, count1)) => (label, count + count1)}

    val labelCountSeq = labelCountData.collect()

    val totalCount = labelCountSeq.map{case(label, count) => count}.sum

    val entropy = labelCountSeq.map{case(label, count) => count.toDouble/totalCount.toDouble}.map(x => x*math.log(x)/math.log(2)).sum * -1

    val entropyData = Entropy(totalCount, labelCountSeq.toMap, entropy)

    return bestSplit(1, data, entropyData)

  }

}

object DecisionTreeTrainer {
  val maxDepth = 3
  val minLeafSamples = 1
  val gainThreshold = 0
}


class FeatureLabelToEntropy extends GroupReduceFunction[(Int, Double), (Int,Entropy)] {
  override def reduce(it: lang.Iterable[(Int, Double)], out: Collector[(Int, Entropy)]) = {
    val values = it.asScala.toVector
    val totalCount = values.size

    val labelCountMap = values
      .map{case(feature, label ) => (label, 1)}
      .groupBy{case(label, count) => label}
      .map{case(label, labelCountVec) => (label, labelCountVec.size)}



    val labelProbabilityMap = labelCountMap.map{case(label, count) => (count.toDouble/totalCount.toDouble)}

    val entropy = labelProbabilityMap.map(x => x*math.log(x)/math.log(2)).sum * -1

    val featureIndex = values(0)._1

    out.collect(featureIndex, Entropy(totalCount, labelCountMap, entropy))


  }
}

class FeatureMedianFilter extends GroupReduceFunction[(Int, Double), (Int, Double)] {
  override def reduce(it: lang.Iterable[(Int, Double)], out: Collector[(Int, Double)]): Unit = {
    val featureValueVec = it.asScala.toVector

    if(featureValueVec.isEmpty) {
      return
    }

    val featureIndex = featureValueVec(0)._1
    val values = featureValueVec.map({case(feature, value) => value})

    if(values.size % 2 == 0) {
     val median = (values(values.size/2-1) + values(values.size/2))/2
      out.collect(featureIndex, median)
    } else {
      out.collect(featureIndex, values(values.size/2))
    }
  }
}

class FeatureValueLowerUpperSplit(val upperSplit: Boolean = true) extends RichFilterFunction[(Int, Double, Double)] {
  var featureMedianMap: Map[Int, Double] = _

  override def open(parameters: Configuration): Unit = {
    featureMedianMap = getRuntimeContext
      .getBroadcastVariable[(Int, Double)]("featureMedianData")
      .asScala
      .toMap
  }

  override def filter(in: (Int, Double, Double)): Boolean = {
    val(featureIndex, value, label) = in

    if(upperSplit == (value > featureMedianMap(featureIndex))) {
      true
    } else {
      false
    }
  }
}
