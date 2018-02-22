package de.uni_leipzig.dbs.decisiontree

class Node(
            var id: Int,
            var prediction: Double,
            var predictionProbability: Double,
            var featureIndex: Option[Int],
            var threshold: Option[Double],
            var leftChild: Option[Node],
            var rightChild: Option[Node],
            var isLeaf: Boolean
          ) extends Serializable {

  def this() = this(-1, -1, -1, None, None, None, None, true)

  override def toString: String = {
    val newLine = System.getProperty("line.seperator")
    val leftChildNode = leftChild.getOrElse("NoChild").toString
    val rightChildNode = rightChild.getOrElse("NoChild").toString
    s"$id, label: $prediction, probability: $predictionProbability, threshold: $threshold, featureIndex: $featureIndex " +
      s"\n$leftChildNode" +
      s"\n$rightChildNode"
  }

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
