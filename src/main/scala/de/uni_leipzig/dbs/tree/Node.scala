package de.uni_leipzig.dbs.tree

class Node(
            val id: Int,
            val stats: NodeStatistics,
            val split: Option[Split]
          ) extends Serializable {

  def this() = this(0, null, None)

  def predict(sample: Vector[Double]): Double = {
    if (split.isEmpty) {
      return stats.prediction
    } else {
      if (sample(split.get.featureIndex) <= split.get.threshold) {
        split.get.leftChild.predict(sample)
      } else {
        split.get.rightChild.predict(sample)
      }
    }
  }

  override def toString: String = {
    def generateWhitespaces(id: Int): String = {
      val depth = (math.log(id) / math.log(2)).toInt
      "  " * depth
    }
    if (split.isEmpty) {
      s"${generateWhitespaces(id)}ID: $id, ${stats.prediction}"
    } else {
      s"${generateWhitespaces(id)}ID: $id, feature: ${split.get.featureIndex}, <=: ${split.get.threshold}, entropy: ${stats.entropy}" +
        s"\n${split.get.leftChild.toString}" +
        s"\n${split.get.rightChild.toString}"
    }
  }


}
