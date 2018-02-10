package de.uni_leipzig.dbs

class Node(
            val id: Int,
            var prediction: Double,
            var predictionProbability: Double,
            var featureIndex: Option[Double],
            var threshold: Option[Double],
            var leftChild: Option[Node],
            var rightChild: Option[Node],
            var isLeaf: Boolean
          ) {
  
  def predict(features: Vector[Double]): Double = {
    if(isLeaf) {
      prediction
    }

    if (features(featureIndex) <= threshold.get) {
      leftChild.get.predict(features)
    } else {
      rightChild.get.predict(features)
    }
  }
}
