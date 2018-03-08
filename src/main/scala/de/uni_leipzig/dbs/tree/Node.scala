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
    if (split.isEmpty) {
      s"ID: $id, $stats, ${stats.entropy}"
    } else {
      s"ID: $id, $stats, ${stats.entropy}, feature: ${split.get.featureIndex}, <= ${split.get.threshold}" +
        s"\n ${split.get.leftChild.toString}" +
        s"\n ${split.get.rightChild.toString}"
    }
  }


}
