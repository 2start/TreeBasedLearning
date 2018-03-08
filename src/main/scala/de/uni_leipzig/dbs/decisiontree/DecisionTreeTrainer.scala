package de.uni_leipzig.dbs.decisiontree

import java.lang

import de.uni_leipzig.dbs.tree.{Node, NodeStatistics, Split}
import org.apache.flink.api.common.functions.{GroupReduceFunction, RichFilterFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class DecisionTreeTrainer(
                           val maxDepth: Int = Int.MaxValue,
                           val minLeafSamples: Int = 1,
                           val minImpurityDecrease: Double = 0
                         ) {

  def getMedian(featureValueData: DataSet[(Int, Double)]): DataSet[(Int, Double)] = {
    featureValueData
      .groupingBy { case (feature, value) => feature }
      .sortGroupWith(Order.ASCENDING) { case (_, value) => value }
      .reduceGroup(new FeatureMedianFilter)
  }

  def bestSplit(id: Int, data: DataSet[(Double, Vector[Double])], parentStats: NodeStatistics): Node = {

    def featureValueLabelToFeatureStats(featureValueLabelData: DataSet[(Int, Double, Double)]): DataSet[(Int, NodeStatistics)] = {
      featureValueLabelData
        .mapWith { case (feature, value, label) => (feature, label) }
        .groupingBy { case (feature, label) => feature }
        .reduceGroup(new FeatureLabelToFeatureStats)
    }

    def calculateSplitEntropy(lowerStats: NodeStatistics, upperStats: NodeStatistics): Double = {
      val lowerCount = lowerStats.count.toDouble
      val upperCount = upperStats.count.toDouble
      val totalCount = lowerCount + upperCount
      val lowerEntropy = lowerStats.entropy
      val upperEntropy = upperStats.entropy

      lowerCount / totalCount * lowerEntropy + upperCount / totalCount * upperEntropy
    }

    def shouldTerminate(id: Int, parentStats: NodeStatistics, lowerStats: NodeStatistics, upperStats: NodeStatistics): Boolean = {
      val depth = (math.log(id) / math.log(2)).toInt
      val impurityDecrease = parentStats.entropy - calculateSplitEntropy(lowerStats, upperStats)

      if (lowerStats.count < minLeafSamples || upperStats.count < minLeafSamples) {
        return true
      }
      if (depth >= maxDepth) {
        return true
      }
      if (impurityDecrease <= minImpurityDecrease) {
        return true
      }

      return false
    }

    val featureValueLabelData = data
      .flatMapWith { case (label, vec) => 0 until vec.size map (i => (i, vec(i), label)) }
    val featureValueData = featureValueLabelData.mapWith { case (feature, value, _) => (feature, value) }

    val featureMedianData = getMedian(featureValueData)

    val lowerData = featureValueLabelData
      .filter(new FeatureValueLowerUpperSplit(upperSplit = false)).withBroadcastSet(featureMedianData, "featureMedianData")

    val upperData = featureValueLabelData
      .filter(new FeatureValueLowerUpperSplit(upperSplit = true)).withBroadcastSet(featureMedianData, "featureMedianData")

    val lowerFeatureStatsData = featureValueLabelToFeatureStats(lowerData)
    val upperFeatureStatsData = featureValueLabelToFeatureStats(upperData)

    val featureStatsData = lowerFeatureStatsData.fullOuterJoin(upperFeatureStatsData).where(0).equalTo(0) {
      (lowerFeatureStats, upperFeatureStats) => {

        val featureIndex = Option(lowerFeatureStats) match {
          case (Some((featureIndex, _))) => featureIndex
          case _ => upperFeatureStats._1
        }

        val dummyFeatureStats = (featureIndex, NodeStatistics(Map(0.0 -> 0)))
        val lowerStats = Option(lowerFeatureStats).getOrElse(dummyFeatureStats)._2
        val upperStats = Option(upperFeatureStats).getOrElse(dummyFeatureStats)._2

        (featureIndex, lowerStats, upperStats)
      }
    }


    val featureMedianMap = featureMedianData.collect().toMap
    val featureStatsSeq = featureStatsData.collect()

    val (splitFeatureIndex, lowerStats, upperStats) =
      featureStatsSeq.minBy { case (feature, lowerStats, upperStats) => calculateSplitEntropy(lowerStats, upperStats) }

    val median = featureMedianMap(splitFeatureIndex)

    if (shouldTerminate(id, parentStats, lowerStats, upperStats)) {
      return new Node(id, parentStats, None)
    }

    val leftChildData = data.filterWith { case (label, vec) => vec(splitFeatureIndex) <= median }
    val rightChildData = data.filterWith { case (label, vec) => vec(splitFeatureIndex) > median }

    val leftChild = bestSplit(id * 2, leftChildData, lowerStats)
    val rightChild = bestSplit(id * 2 + 1, rightChildData, upperStats)

    val split = Split(leftChild, rightChild, splitFeatureIndex, median)
    return new Node(id, parentStats, Some(split))
  }

  def createTree(data: DataSet[(Double, Vector[Double])]): Node = {
    val labelCountData = data
      .mapWith { case (label, vec) => (label, 1) }
      .groupingBy { case (label, count) => label }
      .reduceWith { case ((label, count), (label1, count1)) => (label, count + count1) }

    val labelCountSeq = labelCountData.collect()

    if (labelCountSeq.isEmpty) {
      throw new IllegalArgumentException("Dataset should not be empty!")
    }

    val rootNodeStats = NodeStatistics(labelCountSeq.toMap)

    return bestSplit(1, data, rootNodeStats)

  }
}

class FeatureMedianFilter extends GroupReduceFunction[(Int, Double), (Int, Double)] {
  override def reduce(it: lang.Iterable[(Int, Double)], out: Collector[(Int, Double)]): Unit = {
    val featureValueVec = it.asScala.toVector

    if (featureValueVec.size == 1) {
      out.collect(featureValueVec(0))
      return
    }

    val featureIndex = featureValueVec(0)._1
    val values = featureValueVec.map({ case (feature, value) => value })

    if (values.size % 2 == 0) {
      val median = (values(values.size / 2 - 1) + values(values.size / 2)) / 2
      out.collect(featureIndex, median)
    } else {
      out.collect(featureIndex, values(values.size / 2))
    }
  }
}

class FeatureLabelToFeatureStats extends GroupReduceFunction[(Int, Double), (Int, NodeStatistics)] {
  override def reduce(it: lang.Iterable[(Int, Double)], out: Collector[(Int, NodeStatistics)]) = {
    val values = it.asScala.toVector

    val featureIndex = values(0)._1
    val labelCountMap = values
      .map { case (feature, label) => label }
      .groupBy(identity)
      .mapValues(_.size)

    out.collect(featureIndex, NodeStatistics(labelCountMap))
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
    val (featureIndex, value, label) = in

    if (upperSplit == (value > featureMedianMap(featureIndex))) {
      true
    } else {
      false
    }
  }
}
