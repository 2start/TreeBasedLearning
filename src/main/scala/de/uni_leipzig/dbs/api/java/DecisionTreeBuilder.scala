package de.uni_leipzig.dbs.api.java

class DecisionTreeBuilder {

  var maxDepth: Int = Int.MaxValue
  var minLeafSamples: Int = 1
  var minSplitGain: Double = 0

  def setMaxDepth(maxDepth: java.lang.Integer): DecisionTreeBuilder = {
    this.maxDepth = maxDepth.intValue()
    return this
  }
  def setMinLeafSamples(minLeafSample: java.lang.Integer): DecisionTreeBuilder = {
    this.minLeafSamples = minLeafSamples.intValue()
    return this
  }
  def setMinSplitGain(minSplitGain: java.lang.Double): DecisionTreeBuilder = {
    this.minSplitGain = minSplitGain.doubleValue()
    return this
  }
  def build(): DecisionTree = {
    new DecisionTree(maxDepth, minLeafSamples, minSplitGain)
  }
}
