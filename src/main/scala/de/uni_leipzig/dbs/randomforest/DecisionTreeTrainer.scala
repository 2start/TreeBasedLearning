package de.uni_leipzig.dbs.randomforest

import de.uni_leipzig.dbs.tree.{Node, NodeStatistics, Split, SplitInformation}

import scala.collection.mutable
import scala.util.Random

class DecisionTreeTrainer(
                           val minImpurityDecrease: Double = 0.0,
                           val minLeafSamples: Int = 1,
                           val featuresPerSplit: Int = 1,
                           val maxDepth: Int = Int.MaxValue
                         ) extends Serializable {
  /**
    *
    * @param valueLabelData a list of value, label tupels
    * @return threshold to split child data on
    */

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

  def splitNode(labeledFeaturesList: List[LabeledFeatures], leaf: Node): Node = {
    require(labeledFeaturesList.nonEmpty)
    require(featuresPerSplit <= labeledFeaturesList.head.features.length)

    // get random feature subset and create feature index, feature value, label data
    val featureSize = labeledFeaturesList.head.features.size
    val randFeatureOrder = Random.shuffle(Vector.range(0, featureSize))
    val randFeatureSubset = randFeatureOrder.take(featuresPerSplit)
    val featureValueLabelData = labeledFeaturesList.flatMap(lf => {
      randFeatureSubset.map(i => (i, lf.features(i), lf.label))
    })

    val splitInfos = featureValueLabelData
      .groupBy{case(feature, value, label) => feature}
      .mapValues(featureValueLabelList => featureValueLabelList.map { case(feature, value, label) =>
        (value, label)
      })
      .map{case(feature, valueLabelData) => getSplitInformation(feature, valueLabelData)}

    val bestSplitInfo = splitInfos.minBy(splitInfo => splitInfo.calculateWeightedEntropy)

    val lowerLeaf = new Node(leaf.id * 2, bestSplitInfo.lowerStats, None)
    val upperLeaf = new Node(leaf.id * 2 + 1, bestSplitInfo.upperStats, None)

    if (lowerLeaf.stats.count < minLeafSamples || upperLeaf.stats.count < minLeafSamples) {
      return leaf
    }

    val depth = (math.log(leaf.id) / math.log(2)).toInt
    if (depth == maxDepth) {
      return leaf
    }

    val informationGain = leaf.stats.entropy - bestSplitInfo.calculateWeightedEntropy
    if (informationGain <= minImpurityDecrease) {
      return leaf
    }

    val lowerNodeData = labeledFeaturesList.filter(lf => lf.features(bestSplitInfo.feature) <= bestSplitInfo.threshold)
    val upperNodeData = labeledFeaturesList.filter(lf => lf.features(bestSplitInfo.feature) > bestSplitInfo.threshold)

    val lowerNode = splitNode(lowerNodeData, lowerLeaf)
    val upperNode = splitNode(upperNodeData, upperLeaf)

    val split = Split(lowerNode, upperNode, bestSplitInfo.feature, bestSplitInfo.threshold)
    val node = new Node(leaf.id, leaf.stats, Some(split))

    return node
  }

  def createTree(labeledFeaturesList: List[LabeledFeatures]): Node = {
    val rootNodeStats = NodeStatistics(labeledFeaturesList.map(lf => lf.label).groupBy(identity).mapValues(_.size))
    val rootNode = new Node(1, rootNodeStats, None)
    return splitNode(labeledFeaturesList, rootNode)
  }


}
