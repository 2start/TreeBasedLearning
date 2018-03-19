package de.uni_leipzig.dbs.tree

case class SplitInformation(val feature: Int, val threshold: Double, val lowerStats: NodeStatistics, val upperStats: NodeStatistics) {
  def calculateWeightedEntropy = {
    val lowerCount = lowerStats.count.toDouble
    val upperCount = upperStats.count.toDouble
    val totalCount = lowerCount + upperCount
    val lowerEntropy = lowerStats.entropy
    val upperEntropy = upperStats.entropy

    (lowerCount / totalCount) * lowerEntropy + (upperCount / totalCount) * upperEntropy
  }
}
