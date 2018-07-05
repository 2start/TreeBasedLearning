package de.uni_leipzig.dbs.randomforest

/***
  * Stores features and their associated label.
  * @param label
  * @param features
  */
case class LabeledFeatures(label: Double, features: Vector[Double]) {
  /***
    * Filters the features using a vector of indices to determine the features.
    *
    * @param featureIndices Determines the kept feature entries.
    * @return A filtered version of this labeled features.
    */
  def filterFeatures(featureIndices: Vector[Int]): LabeledFeatures = {
    val newFeatures = featureIndices.map(i => features(i))
    LabeledFeatures(label, newFeatures)

  }
}
