package de.uni_leipzig.dbs.tree

case class NodeStatistics(val labelCounts: Map[Double, Int]) extends Serializable {

  def entropy: Double = {
    if (count == 0) {
      return 0.0
    }
    val labelProbabilities = labelCounts.values.map(labelCount => labelCount.toDouble / count.toDouble)
    val entropySumSteps = labelProbabilities.map(x => x * math.log(x) / math.log(2))
    -1.0 * entropySumSteps.sum
  }

  def prediction = {
    labelCounts.maxBy { case (label, count) => count }._1
  }

  def labelProbability(label: Double): Double = {
    require(labelCounts.isDefinedAt(label))
    labelCounts.get(label).get.toDouble / labelCounts.values.sum
  }

  def count = labelCounts.values.sum

  override def toString: String = {
    s"$labelCounts"
  }
}
