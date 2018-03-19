package de.uni_leipzig.dbs.decisiontree

import java.lang

import de.uni_leipzig.dbs.tree.{Node, NodeStatistics, Split, SplitInformation}
import org.apache.flink.api.common.functions.{GroupReduceFunction, RichFilterFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable

class DecisionTreeTrainer(
                           val maxDepth: Int = Int.MaxValue,
                           val minLeafSamples: Int = 1,
                           val minImpurityDecrease: Double = 0
                         ) {

  def bestSplit(featureValueLabelData: DataSet[(Int, Double, Double)]): SplitInformation = {
    featureValueLabelData
      .groupingBy { case (feature, value, label) => feature }
      .sortGroupWith(Order.ASCENDING) { case (_, value, _) => value }
      .reduceGroup(new FeatureSplitInformationCalculator)
      .collect()
      .minBy(splitInfo => splitInfo.calculateWeightedEntropy)
  }

  def createNode(id: Int, data: DataSet[(Double, Vector[Double])], parentStats: NodeStatistics): Node = {

    def shouldTerminate(id: Int, parentStats: NodeStatistics, splitInfo: SplitInformation): Boolean = {
      val depth = (math.log(id) / math.log(2)).toInt
      val impurityDecrease = parentStats.entropy - splitInfo.calculateWeightedEntropy

      if (splitInfo.lowerStats.count < minLeafSamples || splitInfo.upperStats.count < minLeafSamples) {
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

    val splitInfo = bestSplit(featureValueLabelData)

    if (shouldTerminate(id, parentStats, splitInfo)) {
      return new Node(id, parentStats, None)
    }

    val leftChildData = data.filterWith { case (label, vec) => vec(splitInfo.feature) <= splitInfo.threshold }
    val rightChildData = data.filterWith { case (label, vec) => vec(splitInfo.feature) > splitInfo.threshold }

    val leftChild = createNode(id * 2, leftChildData, splitInfo.lowerStats)
    val rightChild = createNode(id * 2 + 1, rightChildData, splitInfo.upperStats)

    val split = Split(leftChild, rightChild, splitInfo.feature, splitInfo.threshold)
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

    return createNode(1, data, rootNodeStats)

  }
}

class FeatureSplitInformationCalculator extends GroupReduceFunction[(Int, Double, Double), SplitInformation] {
  override def reduce(iterable: lang.Iterable[(Int, Double, Double)], collector: Collector[SplitInformation]): Unit = {
    def getSplitInformation(feature: Int, valueLabelData: List[(Double, Double)]): SplitInformation = {
      val sortedValueLabelData = valueLabelData
        .sortBy{ case(value, label) => value}


      val lowerLabelCountMap: mutable.Map[Double, Int] = mutable.Map.empty
      val upperLabelCountMap: mutable.Map[Double, Int] = mutable.Map.empty
      for ((value, label) <- valueLabelData) {
        if (upperLabelCountMap.contains(label)) upperLabelCountMap(label) += 1
        else upperLabelCountMap(label) = 1
      }


      var lowerStats = NodeStatistics(lowerLabelCountMap.toMap)
      var upperStats = NodeStatistics(upperLabelCountMap.toMap)
      var bestSplitInfo = SplitInformation(feature, sortedValueLabelData(0)._1, lowerStats.copy(), upperStats.copy())

      var i = 0
      while (i < sortedValueLabelData.length) {

        val currentLabel = sortedValueLabelData(i)._2
        var upperLabelCount = upperLabelCountMap.get(currentLabel).get
        upperLabelCount -= 1
        upperLabelCountMap += (currentLabel -> upperLabelCount)
        var lowerLabelCount = lowerLabelCountMap.get(currentLabel).getOrElse(0)
        lowerLabelCount += 1
        lowerLabelCountMap += (currentLabel -> lowerLabelCount)

        // gets current and next value as an option
        val currentVal = sortedValueLabelData.lift(i).map{case(value, label) => value}
        val nextVal = sortedValueLabelData.lift(i+1).map{case(value, label) => value}

        // skips same elements until last elemente because they always belong to the same node
        if(currentVal != nextVal) {
          upperStats = NodeStatistics(upperLabelCountMap.toMap)
          lowerStats = NodeStatistics(lowerLabelCountMap.toMap)
          val currentSplitInfo = SplitInformation(feature, currentVal.get, lowerStats, upperStats)
          if (currentSplitInfo.calculateWeightedEntropy < bestSplitInfo.calculateWeightedEntropy) {
            bestSplitInfo = currentSplitInfo
          }
        }


        i += 1
      }

      return bestSplitInfo
    }
    val data = iterable.asScala.toList
    val feature = data.head._1
    val valueLabelData = data.map{case(feature, value, label) => (value, label)}
    val bestSplitInfo = getSplitInformation(feature, valueLabelData)
    collector.collect(bestSplitInfo)
  }
}
