package de.uni_leipzig.dbs.api.java

class RandomForestBuilder {
  var sampleFraction: Double = 0.05
  var numTrees: Int = 100
  var minImpurityDecrease: Double = 0.00
  var minLeafSamples: Int = 1
  var maxDepth: Int = Int.MaxValue
  var featuresPerSplit: Int = 1

  def setSampleFraction(sampleFraction: Double): RandomForestBuilder = {
    this.sampleFraction = sampleFraction
    this
  }
  def setNumTrees(numTrees: Int): RandomForestBuilder = {
    this.numTrees = numTrees
    this
  }
  def setMinImpurityDecrease(minImpurityDecrease: Double): RandomForestBuilder = {
    this.minImpurityDecrease = minImpurityDecrease
    this
  }
  def setMinLeafSamples(minLeafSamples: Int): RandomForestBuilder = {
    this.minLeafSamples = minLeafSamples
    this
  }
  def setMaxDepth(maxDepth: Int): RandomForestBuilder = {
    this.maxDepth = maxDepth
    this
  }
  def setFeaturesPerSplit(featuresPerSplit: Int): RandomForestBuilder = {
    this.featuresPerSplit = featuresPerSplit
    this
  }
  def build(): RandomForest = {
    new RandomForest(this.sampleFraction, this.numTrees, this.minImpurityDecrease, this.minLeafSamples, this.maxDepth, this.featuresPerSplit)
  }

}
