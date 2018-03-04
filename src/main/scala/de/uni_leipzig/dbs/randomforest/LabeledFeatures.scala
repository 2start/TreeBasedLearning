package de.uni_leipzig.dbs.randomforest

case class LabeledFeatures(label: Double, features: Vector[Double]) {
  // TODO let this accept every TraversableOnce
  def filterFeatures(featureIndices: Vector[Int]): LabeledFeatures = {
    val newFeatures = featureIndices.map(i => features(i))
    LabeledFeatures(label, newFeatures)

  }
}
