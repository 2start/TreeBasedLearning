package de.uni_leipzig.dbs

class Node(
            val id: Int,
            var prediction: Double,
            var predictionProbability: Double,
            var featureIndex: Option[Int],
            var threshold: Option[Double],
            var leftChild: Option[Node],
            var rightChild: Option[Node],
            var isLeaf: Boolean
          ) {

  def predict(features: Vector[Double]): Double = {
    if(isLeaf) {
      return prediction
    }

    if (features(featureIndex.get) <= threshold.get) {
      return leftChild.get.predict(features)
    } else {
      return rightChild.get.predict(features)
    }
  }
}
