package de.uni_leipzig.dbs.randomforest

import de.uni_leipzig.dbs.tree.{Node, NodeStatistics, Split}

import scala.util.Random

class DecisionTreeTrainer(
                           val minImpurityDecrease: Double = 0.0,
                           val minLeafSamples: Int = 1,
                           val featuresPerSplit: Int = 2,
                           val maxDepth: Int = Int.MaxValue
                         ) {
  def splitNode(labeledFeaturesList: List[LabeledFeatures], leaf: Node): Node = {
    require(labeledFeaturesList.nonEmpty)

    val featureSize = labeledFeaturesList.head.features.size
    val filteredFeatures = Random.shuffle(Vector.range(0, featureSize)).take(featuresPerSplit)

    val filteredLabeledFeaturesList = labeledFeaturesList.map(_.filterFeatures(filteredFeatures))


    val splitStats = 0 until featureSize map (i => {
      val labelFeatureList = filteredLabeledFeaturesList.map(lf => (lf.label, lf.features(i)))
      val sortedLabelFeatureList = labelFeatureList.sortBy(_._2)
      val size = sortedLabelFeatureList.size
      val median = if (size % 2 == 0) {
        sortedLabelFeatureList.slice(size / 2 - 1, size / 2 + 1).map(_._2).sum / 2
      } else {
        sortedLabelFeatureList(size / 2)._2
      }
      val lowerSplitLabels = sortedLabelFeatureList.filter(_._2 <= median).map(_._1)
      val upperSplitLabels = sortedLabelFeatureList.filter(_._2 > median).map(_._1)

      val lowerStats = NodeStatistics(lowerSplitLabels.groupBy(identity).mapValues(_.size))
      val upperStats = NodeStatistics(upperSplitLabels.groupBy(identity).mapValues(_.size))

      (i, lowerStats, upperStats, median)

    })

    val minFeatureEntropy = splitStats
      .map {case (featureIndex, lowerStats, upperStats, median) => {
        val totalCount = lowerStats.count + upperStats.count
        val lowerWeightedEntropy = (lowerStats.count.toDouble / totalCount) * lowerStats.entropy
        val upperWeightedEntropy = (upperStats.count.toDouble / totalCount) * upperStats.entropy
        val splitEntropy = lowerWeightedEntropy + upperWeightedEntropy
        (featureIndex, lowerStats, upperStats, splitEntropy, median)
      }
      }
      .minBy(_._4)

    val(splitFeatureIndex, lowerStats, upperStats, splitEntropy, median) = minFeatureEntropy

    val lowerLeaf = new Node(leaf.id*2, lowerStats, None)
    val upperLeaf = new Node(leaf.id*2 + 1, upperStats, None)

    if(lowerLeaf.stats.count < minLeafSamples || upperLeaf.stats.count < minLeafSamples) {
      return leaf
    }

    val depth = (math.log(leaf.id)/math.log(2)).toInt
    if(depth == maxDepth) {
      return leaf
    }

    val informationGain = leaf.stats.entropy - splitEntropy
    if(informationGain <= minImpurityDecrease) {
      return leaf
    }


    val lowerNodeData = labeledFeaturesList.filter(lf => lf.features(splitFeatureIndex) <= median)
    val upperNodeData = labeledFeaturesList.filter(lf => lf.features(splitFeatureIndex) > median)
    val lowerNode = splitNode(lowerNodeData, lowerLeaf)
    val upperNode = splitNode(upperNodeData, upperLeaf)

    val split = Split(lowerNode, upperNode, splitFeatureIndex, median)
    val node = new Node(leaf.id, leaf.stats, Some(split))

    return node
  }

  def createTree(labeledFeaturesList: List[LabeledFeatures]): Node = {
    val rootNodeStats = NodeStatistics(labeledFeaturesList.map(lf => lf.label).groupBy(identity).mapValues(_.size))
    val rootNode = new Node(1, rootNodeStats, None)
    return splitNode(labeledFeaturesList, rootNode)
  }


}
